use crate::{
    book::Book,
    types::{Event, Kind, MS, SECOND},
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub fn names() -> Vec<String> {
    let mut out = Vec::new();
    for s in ["BTCUSDT", "ETHUSDT"] {
        for n in [
            "spread_bps",
            "microprice_bps",
            "imbalance_1",
            "imbalance_5",
            "imbalance_10",
        ] {
            out.push(format!("{s}.{n}"));
        }
        for w in [100, 1000, 5000] {
            for n in ["ofi", "trade_flow", "return_bps", "volatility_bps"] {
                out.push(format!("{s}.{n}_{w}ms"));
            }
        }
    }
    for w in [100, 1000, 5000] {
        out.push(format!("BTC_minus_ETH.return_{w}ms"));
    }
    out
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct History {
    mids: VecDeque<(u64, f64)>,
    flow: VecDeque<(u64, f64)>,
    trades: VecDeque<(u64, f64, f64)>,
    last_top: Option<(f64, f64, f64, f64)>,
    last_trade: Option<u64>,
}
impl History {
    pub fn observe(&mut self, e: &Event, book: &Book) {
        match &e.kind {
            Kind::Depth { .. } => {
                if let Some((b, bq, a, aq)) = book.top() {
                    self.mids.push_back((e.recv_ns, (b + a) / 2.0));
                    if let Some((pb, pbq, pa, paq)) = self.last_top {
                        let flow = (if b >= pb { bq } else { 0.0 })
                            - (if b <= pb { pbq } else { 0.0 })
                            - (if a <= pa { aq } else { 0.0 })
                            + (if a >= pa { paq } else { 0.0 });
                        self.flow
                            .push_back((e.recv_ns, flow / (bq + aq).max(1e-12)));
                    }
                    self.last_top = Some((b, bq, a, aq));
                }
            }
            Kind::Trade {
                id,
                qty,
                buyer_maker,
                ..
            } if self.last_trade.is_none_or(|old| *id > old) => {
                self.trades
                    .push_back((e.recv_ns, if *buyer_maker { -qty } else { *qty }, *qty));
                self.last_trade = Some(*id);
            }
            _ => (),
        }
        let cutoff = e.recv_ns.saturating_sub(5 * SECOND);
        while self.mids.len() > 1 && self.mids[1].0 <= cutoff {
            self.mids.pop_front();
        }
        while self.flow.front().is_some_and(|x| x.0 <= cutoff) {
            self.flow.pop_front();
        }
        while self.trades.front().is_some_and(|x| x.0 <= cutoff) {
            self.trades.pop_front();
        }
    }
    pub fn sample(&mut self, now: u64, book: &Book) {
        if let Some(mid) = book.mid() {
            self.mids.push_back((now, mid));
        }
        let cutoff = now.saturating_sub(5 * SECOND);
        while self.mids.len() > 1 && self.mids[1].0 <= cutoff {
            self.mids.pop_front();
        }
    }
    pub fn vector(&self, now: u64, book: &Book) -> Option<Vec<f64>> {
        let (b, bq, a, aq) = book.top()?;
        let mid = (b + a) / 2.0;
        let micro = (a * bq + b * aq) / (bq + aq);
        let mut v = vec![
            (a - b) / mid * 10000.0,
            (micro / mid - 1.0) * 10000.0,
            book.imbalance(1),
            book.imbalance(5),
            book.imbalance(10),
        ];
        for w in [100, 1000, 5000] {
            let start = now.saturating_sub(w * MS);
            let base = self.mids.iter().rev().find(|x| x.0 <= start)?.1;
            let ofi: f64 = self.flow.iter().filter(|x| x.0 > start).map(|x| x.1).sum();
            let signed: f64 = self
                .trades
                .iter()
                .filter(|x| x.0 > start)
                .map(|x| x.1)
                .sum();
            let total: f64 = self
                .trades
                .iter()
                .filter(|x| x.0 > start)
                .map(|x| x.2)
                .sum();
            let mut prev = base;
            let mut variance = 0.0;
            for &(_, m) in self.mids.iter().filter(|x| x.0 > start) {
                variance += (10000.0 * (m / prev).ln()).powi(2);
                prev = m;
            }
            v.extend([
                ofi,
                signed / total.max(1e-12),
                10000.0 * (mid / base).ln(),
                variance.sqrt(),
            ]);
        }
        Some(v)
    }
}
pub fn cross_vector(h: &[History; 2], books: &[Book; 2], now: u64) -> Option<Vec<f64>> {
    let a = h[0].vector(now, &books[0])?;
    let b = h[1].vector(now, &books[1])?;
    let diff = [a[7] - b[7], a[11] - b[11], a[15] - b[15]];
    let mut out = a;
    out.extend(b);
    out.extend(diff);
    out.iter().all(|v| v.is_finite()).then_some(out)
}
