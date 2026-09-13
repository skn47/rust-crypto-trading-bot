use crate::types::{Event, Kind, MS};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Instrument {
    pub tick: f64,
    pub step: f64,
    pub min_qty: f64,
    pub min_notional: f64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Book {
    pub instrument: Option<Instrument>,
    pub bids: BTreeMap<i64, f64>,
    pub asks: BTreeMap<i64, f64>,
    pub last_id: Option<u64>,
    pub synced: bool,
    pub updated_ns: u64,
    pub generation: u64,
    pub buffer: Vec<Event>,
}
impl Book {
    pub fn invalidate(&mut self) {
        self.synced = false;
        self.last_id = None;
        self.bids.clear();
        self.asks.clear();
        self.buffer.clear();
        self.generation += 1;
    }
    pub fn top(&self) -> Option<(f64, f64, f64, f64)> {
        let tick = self.instrument.as_ref()?.tick;
        let (b, bq) = self.bids.last_key_value()?;
        let (a, aq) = self.asks.first_key_value()?;
        (*b < *a).then_some((*b as f64 * tick, *bq, *a as f64 * tick, *aq))
    }
    pub fn mid(&self) -> Option<f64> {
        self.top().map(|(b, _, a, _)| (b + a) / 2.0)
    }
    pub fn fresh(&self, now: u64, stale_ms: u64) -> bool {
        self.synced
            && self.top().is_some()
            && now >= self.updated_ns
            && now - self.updated_ns <= stale_ms * MS
    }
    fn update_levels(&mut self, bids: &[[f64; 2]], asks: &[[f64; 2]]) -> Result<()> {
        let tick = self
            .instrument
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing metadata"))?
            .tick;
        for (levels, map) in [(bids, &mut self.bids), (asks, &mut self.asks)] {
            for &[p, q] in levels {
                ensure!(
                    p.is_finite() && p > 0.0 && q.is_finite() && q >= 0.0,
                    "invalid depth level"
                );
                let units = p / tick;
                ensure!(
                    units < i64::MAX as f64 && (units - units.round()).abs() < 1e-5,
                    "off-tick price"
                );
                let key = units.round() as i64;
                if q == 0.0 {
                    map.remove(&key);
                } else {
                    map.insert(key, q);
                }
            }
        }
        Ok(())
    }
    /// Returns false when a fresh REST snapshot is needed.
    pub fn apply(&mut self, e: &Event) -> Result<bool> {
        match &e.kind {
            Kind::Metadata {
                tick,
                step,
                min_qty,
                min_notional,
                ..
            } => {
                ensure!(
                    [tick, step, min_qty, min_notional]
                        .iter()
                        .all(|v| v.is_finite() && **v > 0.0),
                    "invalid instrument"
                );
                if let Some(old) = &self.instrument
                    && (old.tick != *tick || old.step != *step)
                {
                    self.invalidate();
                }
                self.instrument = Some(Instrument {
                    tick: *tick,
                    step: *step,
                    min_qty: *min_qty,
                    min_notional: *min_notional,
                });
            }
            Kind::Snapshot {
                last_id,
                bids,
                asks,
                ..
            } => {
                self.bids.clear();
                self.asks.clear();
                self.synced = false;
                self.update_levels(bids, asks)?;
                self.last_id = Some(*last_id);
                self.generation += 1;
                let buffered = std::mem::take(&mut self.buffer);
                for mut b in buffered {
                    // Snapshot information becomes available now, never at a buffered event's old time.
                    b.recv_ns = e.recv_ns;
                    if !self.apply(&b)? {
                        return Ok(false);
                    }
                }
            }
            Kind::Depth {
                first_id,
                last_id,
                prev_id,
                bids,
                asks,
                ..
            } => {
                ensure!(first_id <= last_id, "invalid depth update range");
                let Some(old) = self.last_id else {
                    ensure!(
                        self.buffer.len() < 8192,
                        "depth synchronization buffer overflow"
                    );
                    self.buffer.push(e.clone());
                    return Ok(false);
                };
                if (self.synced && *last_id <= old) || (!self.synced && *last_id < old) {
                    return Ok(true);
                }
                let contiguous = if self.synced {
                    *prev_id == old
                } else {
                    *first_id <= old && *last_id >= old
                };
                if !contiguous {
                    self.invalidate();
                    self.buffer.push(e.clone());
                    return Ok(false);
                }
                self.update_levels(bids, asks)?;
                self.last_id = Some(*last_id);
                self.synced = self.top().is_some();
                self.updated_ns = e.recv_ns;
                if !self.synced {
                    self.invalidate();
                    return Ok(false);
                }
            }
            _ => (),
        }
        Ok(true)
    }
    pub fn imbalance(&self, n: usize) -> f64 {
        let b: f64 = self.bids.values().rev().take(n).sum();
        let a: f64 = self.asks.values().take(n).sum();
        if a + b > 0.0 { (b - a) / (b + a) } else { 0.0 }
    }
}
