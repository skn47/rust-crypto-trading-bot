use crate::{
    book::Book,
    config::Config,
    types::{Event, Kind, MS},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Position {
    pub qty: f64,
    pub entry: f64,
    pub exit_due_ns: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Order {
    pub side: f64,
    pub qty: f64,
    pub arrival_ns: u64,
    pub reduce: bool,
    pub decision_mid: f64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Journal {
    pub seq: u64,
    pub recv_ns: u64,
    pub symbol: String,
    pub action: String,
    pub qty: f64,
    pub price: f64,
    pub fee: f64,
    pub value: f64,
    pub reason: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SymbolStats {
    pub realized: f64,
    pub unrealized: f64,
    pub fees: f64,
    pub funding: f64,
    pub turnover: f64,
    pub slippage: f64,
    pub fills: u64,
    pub rejected: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Broker {
    pub cash: f64,
    pub positions: [Position; 2],
    pub pending: [Option<Order>; 2],
    pub fees: f64,
    pub funding: f64,
    pub realized: f64,
    pub turnover: f64,
    pub slippage: f64,
    pub rejected: u64,
    pub equity: f64,
    pub peak: f64,
    pub max_drawdown: f64,
    pub day: u64,
    pub day_start: f64,
    pub stopped: bool,
    pub exposure_unpriced: bool,
    pub last_mids: [f64; 2],
    pub consumed: [BTreeMap<i64, f64>; 4],
    pub funding_seen: BTreeSet<String>,
    pub position_history: [Vec<(u64, f64)>; 2],
    /// BTCUSDT, ETHUSDT in configuration order.
    pub per_symbol: [SymbolStats; 2],
}
impl Broker {
    pub fn new(equity: f64) -> Self {
        Self {
            cash: equity,
            positions: Default::default(),
            pending: Default::default(),
            fees: 0.0,
            funding: 0.0,
            realized: 0.0,
            turnover: 0.0,
            slippage: 0.0,
            rejected: 0,
            equity,
            peak: equity,
            max_drawdown: 0.0,
            day: 0,
            day_start: equity,
            stopped: false,
            exposure_unpriced: false,
            last_mids: [0.0; 2],
            consumed: Default::default(),
            funding_seen: Default::default(),
            position_history: Default::default(),
            per_symbol: Default::default(),
        }
    }
    fn row(e: &Event, c: &Config, i: usize, action: &str, reason: &str) -> Journal {
        Journal {
            seq: e.seq,
            recv_ns: e.recv_ns,
            symbol: c.symbols[i].clone(),
            action: action.into(),
            qty: 0.0,
            price: 0.0,
            fee: 0.0,
            value: 0.0,
            reason: reason.into(),
        }
    }
    fn levels(&self, i: usize, side: f64, book: &Book, qty: f64) -> Vec<(i64, f64, f64)> {
        let Some(meta) = &book.instrument else {
            return Vec::new();
        };
        let used = &self.consumed[2 * i + usize::from(side > 0.0)];
        let iter: Box<dyn Iterator<Item = (&i64, &f64)> + '_> = if side > 0.0 {
            Box::new(book.asks.iter())
        } else {
            Box::new(book.bids.iter().rev())
        };
        let mut left = qty;
        let mut fills = Vec::new();
        for (&p, &q) in iter {
            let available = (q - used.get(&p).copied().unwrap_or(0.0)).max(0.0);
            let take = (left.min(available) / meta.step + 1e-9).floor() * meta.step;
            if take > 0.0 {
                fills.push((p, take, p as f64 * meta.tick));
                left -= take;
            }
            if left < meta.step * 0.5 {
                break;
            }
        }
        fills
    }
    fn gross(&self) -> f64 {
        (0..2)
            .map(|i| {
                self.positions[i].qty.abs() * self.last_mids[i]
                    + self.pending[i]
                        .as_ref()
                        .filter(|o| !o.reduce)
                        .map_or(0.0, |o| o.qty * o.decision_mid)
            })
            .sum()
    }
    fn mark_equity(&mut self, books: &[Book; 2], now: u64, c: &Config) {
        self.exposure_unpriced = false;
        for (i, b) in books.iter().enumerate() {
            if b.fresh(now, c.stale_ms) {
                self.last_mids[i] = b.mid().unwrap();
            } else if self.positions[i].qty != 0.0 {
                self.exposure_unpriced = true;
            }
            self.per_symbol[i].unrealized =
                self.positions[i].qty * (self.last_mids[i] - self.positions[i].entry);
        }
        self.equity = self.cash
            + (0..2)
                .map(|i| self.positions[i].qty * (self.last_mids[i] - self.positions[i].entry))
                .sum::<f64>();
        self.peak = self.peak.max(self.equity);
        self.max_drawdown = self.max_drawdown.max(self.peak - self.equity);
    }
    fn enforce_stop(&mut self, e: &Event, c: &Config, valid: bool, out: &mut Vec<Journal>) {
        if self.day_start - self.equity >= c.daily_loss {
            self.stopped = true;
        }
        for i in 0..2 {
            if (!valid || self.stopped) && self.pending[i].as_ref().is_some_and(|o| !o.reduce) {
                self.pending[i] = None;
                out.push(Self::row(
                    e,
                    c,
                    i,
                    "cancel",
                    "invalid data or daily loss stop",
                ));
            }
            if self.positions[i].qty != 0.0
                && self.pending[i].is_none()
                && (self.stopped || e.recv_ns >= self.positions[i].exit_due_ns)
            {
                let p = &self.positions[i];
                self.pending[i] = Some(Order {
                    side: -p.qty.signum(),
                    qty: p.qty.abs(),
                    arrival_ns: e.recv_ns + c.latency_ms * MS,
                    reduce: true,
                    decision_mid: self.last_mids[i],
                });
                out.push(Self::row(
                    e,
                    c,
                    i,
                    "exit_submitted",
                    if self.stopped {
                        "daily loss stop"
                    } else {
                        "holding horizon elapsed"
                    },
                ));
            }
        }
    }
    pub fn on_event(
        &mut self,
        e: &Event,
        books: &[Book; 2],
        c: &Config,
        updated: Option<usize>,
        valid: bool,
    ) -> Result<Vec<Journal>> {
        let mut out = Vec::new();
        if matches!(e.kind, Kind::Disconnect { .. }) {
            self.consumed = Default::default();
        }
        if let Some(s) = e.kind.symbol() {
            let i = c.symbols.iter().position(|x| x == s).unwrap();
            match &e.kind {
                Kind::Snapshot { .. } => {
                    self.consumed[2 * i].clear();
                    self.consumed[2 * i + 1].clear();
                }
                Kind::Depth { bids, asks, .. } if updated == Some(i) => {
                    if let Some(meta) = &books[i].instrument {
                        for (side, levels) in [(0, bids), (1, asks)] {
                            for [p, _] in levels {
                                self.consumed[2 * i + side]
                                    .remove(&((*p / meta.tick).round() as i64));
                            }
                        }
                    }
                }
                Kind::Funding {
                    funding_ns,
                    rate,
                    mark,
                    ..
                } => {
                    ensure!(
                        rate.is_finite() && mark.is_finite() && *mark > 0.0,
                        "invalid funding"
                    );
                    let key = format!("{s}:{funding_ns}");
                    if self.funding_seen.insert(key) {
                        let qty = self.position_history[i]
                            .iter()
                            .rev()
                            .find(|x| x.0 <= *funding_ns)
                            .map_or(0.0, |x| x.1);
                        let payment = -qty * mark * rate;
                        self.cash += payment;
                        self.funding += payment;
                        self.per_symbol[i].funding += payment;
                        let mut r = Self::row(e, c, i, "funding", "settled historical funding");
                        r.value = payment;
                        out.push(r);
                    }
                }
                _ => (),
            }
        }
        self.mark_equity(books, e.recv_ns, c);
        let day = e.utc_ns / (86400 * 1_000_000_000);
        if day > self.day {
            self.day = day;
            self.day_start = self.equity;
            self.stopped = false;
        }
        self.enforce_stop(e, c, valid, &mut out);
        if let Some(i) = updated
            && let Some(mut o) = self.pending[i].clone()
            && e.recv_ns >= o.arrival_ns
            && books[i].fresh(e.recv_ns, c.stale_ms)
            && (o.reduce || valid)
        {
            let fills = self.levels(i, o.side, &books[i], o.qty);
            let qty: f64 = fills.iter().map(|f| f.1).sum();
            if qty > 0.0 {
                let price = fills.iter().map(|f| f.1 * f.2).sum::<f64>() / qty;
                let meta = books[i].instrument.as_ref().unwrap();
                // Recheck gross risk at execution; an entry cannot consume the exit reserve.
                let projected = self.gross() - o.qty * o.decision_mid + qty * price;
                if !o.reduce
                    && (qty < meta.min_qty
                        || qty * price < meta.min_notional
                        || projected > c.gross_cap)
                {
                    self.pending[i] = None;
                    self.rejected += 1;
                    self.per_symbol[i].rejected += 1;
                    out.push(Self::row(
                        e,
                        c,
                        i,
                        "reject",
                        "fill violates instrument or gross limit",
                    ));
                    return Ok(out);
                }
                for (p, q, _) in fills {
                    *self.consumed[2 * i + usize::from(o.side > 0.0)]
                        .entry(p)
                        .or_default() += q;
                }
                let fee = qty * price * c.fee_bps / 10000.0;
                self.cash -= fee;
                self.fees += fee;
                self.turnover += qty * price;
                self.slippage += o.side * (price - o.decision_mid) * qty;
                self.per_symbol[i].fees += fee;
                self.per_symbol[i].turnover += qty * price;
                self.per_symbol[i].slippage += o.side * (price - o.decision_mid) * qty;
                self.per_symbol[i].fills += 1;
                if o.reduce {
                    let pnl =
                        qty * self.positions[i].qty.signum() * (price - self.positions[i].entry);
                    self.cash += pnl;
                    self.realized += pnl;
                    self.per_symbol[i].realized += pnl;
                    self.positions[i].qty += o.side * qty;
                    if self.positions[i].qty.abs() < meta.step * 0.5 {
                        self.positions[i] = Position::default();
                    }
                } else {
                    self.positions[i] = Position {
                        qty: o.side * qty,
                        entry: price,
                        exit_due_ns: e.recv_ns + c.horizon_ms * MS,
                    };
                }
                self.position_history[i].push((e.utc_ns, self.positions[i].qty));
                let mut r = Self::row(e, c, i, "fill", if o.reduce { "exit" } else { "entry" });
                r.qty = o.side * qty;
                r.price = price;
                r.fee = fee;
                out.push(r);
            }
            o.qty -= qty;
            if o.reduce && o.qty > books[i].instrument.as_ref().unwrap().step * 0.5 {
                o.arrival_ns = e.recv_ns + c.latency_ms * MS;
                self.pending[i] = Some(o);
            } else {
                if !o.reduce && o.qty > 1e-12 {
                    out.push(Self::row(e, c, i, "cancel", "unfilled IOC entry remainder"));
                }
                self.pending[i] = None;
            }
        }
        self.mark_equity(books, e.recv_ns, c);
        self.enforce_stop(e, c, valid, &mut out);
        Ok(out)
    }
    pub fn decide(
        &mut self,
        i: usize,
        prediction: f64,
        buffer: f64,
        e: &Event,
        books: &[Book; 2],
        c: &Config,
    ) -> Vec<Journal> {
        let mut signal = Self::row(e, c, i, "prediction", "");
        signal.value = prediction;
        let mut out = vec![signal];
        if self.stopped || self.positions[i].qty != 0.0 || self.pending[i].is_some() {
            return out;
        }
        let mid = books[i].mid().unwrap();
        let meta = books[i].instrument.as_ref().unwrap();
        let qty = (c.entry_notional / mid / meta.step).floor() * meta.step;
        let side = prediction.signum();
        if side == 0.0 {
            return out;
        }
        let entry = self.levels(i, side, &books[i], qty);
        let exit = self.levels(i, -side, &books[i], qty);
        let enough = |v: &Vec<(i64, f64, f64)>| v.iter().map(|x| x.1).sum::<f64>() + 1e-12 >= qty;
        if qty < meta.min_qty || qty * mid < meta.min_notional || !enough(&entry) || !enough(&exit)
        {
            return out;
        }
        let avg = |v: &Vec<(i64, f64, f64)>| v.iter().map(|x| x.1 * x.2).sum::<f64>() / qty;
        let cost = side * (avg(&entry) - avg(&exit)) / mid * 10000.0 + 2.0 * c.fee_bps + buffer;
        if prediction.abs() <= cost {
            return out;
        }
        if self.gross() + qty * mid > c.gross_cap {
            self.rejected += 1;
            self.per_symbol[i].rejected += 1;
            out.push(Self::row(
                e,
                c,
                i,
                "reject",
                "gross exposure including pending entries",
            ));
            return out;
        }
        self.pending[i] = Some(Order {
            side,
            qty,
            arrival_ns: e.recv_ns + c.latency_ms * MS,
            reduce: false,
            decision_mid: mid,
        });
        let mut r = Self::row(
            e,
            c,
            i,
            "entry_submitted",
            "prediction exceeds round-trip costs",
        );
        r.qty = side * qty;
        r.value = cost;
        out.push(r);
        out
    }
}
