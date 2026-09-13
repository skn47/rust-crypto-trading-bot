use crate::{
    book::Book,
    config::Config,
    features::{History, cross_vector},
    model::Model,
    paper::{Broker, Journal},
    types::{Event, Kind, MS, SCHEMA},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sample {
    pub recv_ns: u64,
    pub seq: u64,
    pub epoch: u64,
    pub features: Vec<f64>,
    pub mids: [f64; 2],
    pub spreads_bps: [f64; 2],
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Engine {
    pub config: Config,
    pub books: [Book; 2],
    pub histories: [History; 2],
    pub models: Vec<Model>,
    pub broker: Broker,
    pub last_seq: u64,
    pub now: u64,
    pub session: String,
    pub epoch: u64,
    pub healthy_since: Option<u64>,
    pub last_sample_ns: u64,
    pub gaps: u64,
    pub feeds: [bool; 2],
}
impl Engine {
    pub fn new(config: Config, models: Vec<Model>) -> Result<Self> {
        config.validate()?;
        for m in &models {
            m.validate(&config.symbols)?;
        }
        ensure!(
            models.len() <= 2 && (models.len() != 2 || models[0].symbol != models[1].symbol),
            "duplicate models"
        );
        Ok(Self {
            broker: Broker::new(config.equity),
            config,
            books: Default::default(),
            histories: Default::default(),
            models,
            last_seq: 0,
            now: 0,
            session: String::new(),
            epoch: 0,
            healthy_since: None,
            last_sample_ns: 0,
            gaps: 0,
            feeds: [false; 2],
        })
    }
    pub fn valid_at(&self, t: u64) -> bool {
        self.feeds.iter().all(|x| *x) && self.books.iter().all(|b| b.fresh(t, self.config.stale_ms))
    }
    fn reset_history(&mut self) {
        self.histories = Default::default();
        self.healthy_since = None;
        self.epoch += 1;
    }
    pub fn on_event(&mut self, e: &Event) -> Result<(Option<Sample>, Vec<Journal>)> {
        ensure!(
            e.version == SCHEMA && e.seq > self.last_seq,
            "invalid event schema/sequence"
        );
        ensure!(e.recv_ns >= self.now, "availability clock moved backwards");
        if self.session != e.session {
            for b in &mut self.books {
                b.invalidate();
            }
            self.reset_history();
            self.feeds = [false; 2];
            self.session = e.session.clone();
        }
        if self.healthy_since.is_some() && !self.valid_at(e.recv_ns) {
            self.reset_history();
        }
        self.now = e.recv_ns;
        self.last_seq = e.seq;
        let mut updated = None;
        if let Some(s) = e.kind.symbol() {
            let i = self
                .config
                .symbols
                .iter()
                .position(|x| x == s)
                .ok_or_else(|| anyhow::anyhow!("unexpected symbol {s}"))?;
            let generation = self.books[i].generation;
            let old_id = self.books[i].last_id;
            let was_synced = self.books[i].synced;
            if !self.books[i].apply(e)? {
                self.gaps += 1;
            }
            if self.books[i].generation != generation {
                self.reset_history();
            }
            if matches!(e.kind, Kind::Depth { .. })
                && self.books[i].fresh(e.recv_ns, self.config.stale_ms)
                && (!was_synced || self.books[i].last_id != old_id)
            {
                updated = Some(i);
            }
            if !matches!(e.kind, Kind::Depth { .. }) || updated.is_some() {
                self.histories[i].observe(e, &self.books[i]);
            }
        }
        if let Kind::Connection { source, connected } = &e.kind {
            let i = match source.as_str() {
                "public" => 0,
                "market" => 1,
                _ => anyhow::bail!("unknown feed"),
            };
            self.feeds[i] = *connected;
            if !connected {
                for b in &mut self.books {
                    b.invalidate();
                }
                self.reset_history();
            }
        }
        if matches!(e.kind, Kind::Disconnect { .. }) {
            for b in &mut self.books {
                b.invalidate();
            }
            self.reset_history();
        }
        let valid = self.valid_at(e.recv_ns);
        if !valid && self.healthy_since.is_some() {
            self.reset_history();
        }
        let mut journal = self
            .broker
            .on_event(e, &self.books, &self.config, updated, valid)?;
        let mut sample = None;
        if matches!(e.kind, Kind::Timer) && valid && e.recv_ns > self.last_sample_ns {
            self.last_sample_ns = e.recv_ns;
            let since = *self.healthy_since.get_or_insert(e.recv_ns);
            for i in 0..2 {
                self.histories[i].sample(e.recv_ns, &self.books[i]);
            }
            if e.recv_ns >= since + self.config.warmup_ms * MS
                && let Some(x) = cross_vector(&self.histories, &self.books, e.recv_ns)
            {
                let mids = std::array::from_fn(|i| self.books[i].mid().unwrap());
                let spreads_bps = std::array::from_fn(|i| {
                    let (b, _, a, _) = self.books[i].top().unwrap();
                    (a - b) / mids[i] * 10000.0
                });
                for m in &self.models {
                    ensure!(
                        e.recv_ns > m.selected_through_ns,
                        "model selection/training overlaps replay decision"
                    );
                    let i = self
                        .config
                        .symbols
                        .iter()
                        .position(|s| s == &m.symbol)
                        .unwrap();
                    let prediction = m.predict(&x);
                    ensure!(prediction.is_finite(), "nonfinite prediction");
                    journal.extend(self.broker.decide(
                        i,
                        prediction,
                        m.entry_buffer_bps,
                        e,
                        &self.books,
                        &self.config,
                    ));
                }
                sample = Some(Sample {
                    recv_ns: e.recv_ns,
                    seq: e.seq,
                    epoch: self.epoch,
                    features: x,
                    mids,
                    spreads_bps,
                });
            }
        }
        Ok((sample, journal))
    }
}
