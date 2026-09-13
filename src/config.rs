use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    #[default]
    BinanceUsdm,
    CoinbaseSpot,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub venue: Venue,
    pub symbols: Vec<String>,
    pub public_ws: String,
    pub market_ws: String,
    pub rest_url: String,
    pub stale_ms: u64,
    pub warmup_ms: u64,
    pub decision_ms: u64,
    pub horizon_ms: u64,
    pub latency_ms: u64,
    pub fee_bps: f64,
    pub equity: f64,
    pub entry_notional: f64,
    pub gross_cap: f64,
    pub daily_loss: f64,
    pub queue_capacity: usize,
    #[serde(default = "default_checkpoint_ms")]
    pub checkpoint_ms: u64,
    pub model_paths: Vec<String>,
}
fn default_checkpoint_ms() -> u64 {
    1000
}
impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let c: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<()> {
        let expected: &[&str] = match self.venue {
            Venue::BinanceUsdm => &["BTCUSDT", "ETHUSDT"],
            Venue::CoinbaseSpot => &["BTC-USD", "ETH-USD"],
        };
        ensure!(
            self.symbols == expected,
            "v1 requires this venue's BTC, ETH symbols in that order"
        );
        ensure!(
            self.decision_ms == 100 && self.horizon_ms == 1000 && self.warmup_ms >= 5000,
            "v1 feature/model contract requires 100ms decisions, 1s horizon, >=5s warmup"
        );
        ensure!(
            self.stale_ms > 0 && self.latency_ms > 0 && self.queue_capacity > 0,
            "invalid timing/capacity"
        );
        for v in [
            self.equity,
            self.entry_notional,
            self.gross_cap,
            self.daily_loss,
        ] {
            ensure!(
                v.is_finite() && v > 0.0,
                "risk values must be finite and positive"
            );
        }
        ensure!(
            self.fee_bps.is_finite() && self.fee_bps >= 0.0,
            "invalid fee"
        );
        ensure!(
            self.gross_cap <= self.equity && self.entry_notional <= self.gross_cap,
            "v1 uses unlevered gross limits"
        );
        Ok(())
    }
}
