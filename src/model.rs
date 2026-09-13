use crate::features;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Model {
    pub version: u32,
    pub symbol: String,
    pub features: Vec<String>,
    pub mean: Vec<f64>,
    pub scale: Vec<f64>,
    pub coefficients: Vec<f64>,
    pub intercept: f64,
    pub horizon_ms: u64,
    pub trained_through_ns: u64,
    pub selected_through_ns: u64,
    pub dataset_sha256: String,
    pub entry_buffer_bps: f64,
    pub alpha: f64,
}
impl Model {
    pub fn load(path: &str) -> Result<Self> {
        Ok(serde_json::from_reader(std::fs::File::open(path)?)?)
    }
    /// The distinct per-asset prefixes this model's own feature vector was built from,
    /// in first-seen order (e.g. `["BTCUSDT", "ETHUSDT"]`). Used to self-validate a
    /// model file when no venue configuration is available (see `score`).
    pub fn asset_symbols(&self) -> Vec<String> {
        let mut out = Vec::new();
        for f in &self.features {
            if let Some((prefix, _)) = f.split_once('.')
                && prefix != "BTC_minus_ETH"
                && !out.iter().any(|s| s == prefix)
            {
                out.push(prefix.to_owned());
            }
        }
        out
    }
    pub fn validate(&self, symbols: &[String]) -> Result<()> {
        let n = features::names(symbols);
        ensure!(
            self.version == 1 && self.horizon_ms == 1000,
            "unsupported model contract"
        );
        ensure!(symbols.contains(&self.symbol), "unsupported model symbol");
        ensure!(
            self.features == n
                && self.mean.len() == n.len()
                && self.scale.len() == n.len()
                && self.coefficients.len() == n.len(),
            "feature schema mismatch"
        );
        ensure!(
            self.mean
                .iter()
                .chain(&self.coefficients)
                .all(|v| v.is_finite())
                && self.scale.iter().all(|v| v.is_finite() && *v > 0.0),
            "nonfinite/invalid model"
        );
        ensure!(
            self.intercept.is_finite()
                && self.alpha.is_finite()
                && self.alpha >= 0.0
                && self.entry_buffer_bps.is_finite()
                && self.entry_buffer_bps >= 0.0,
            "invalid model parameter"
        );
        ensure!(
            self.selected_through_ns >= self.trained_through_ns
                && self.dataset_sha256.len() == 64
                && self.dataset_sha256.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid provenance"
        );
        Ok(())
    }
    pub fn predict(&self, x: &[f64]) -> f64 {
        self.intercept
            + x.iter()
                .zip(&self.mean)
                .zip(&self.scale)
                .zip(&self.coefficients)
                .map(|(((x, m), s), w)| (x - m) / s * w)
                .sum::<f64>()
    }
}
