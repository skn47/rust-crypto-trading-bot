use serde::{Deserialize, Serialize};
pub const MS: u64 = 1_000_000;
pub const SECOND: u64 = 1_000_000_000;
pub const SCHEMA: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub version: u32,
    pub session: String,
    pub seq: u64,
    /// Monotonic clock anchored to UTC at session start; availability clock.
    pub recv_ns: u64,
    pub utc_ns: u64,
    pub exchange_ns: Option<u64>,
    pub raw: Option<serde_json::Value>,
    #[serde(flatten)]
    pub kind: Kind,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Kind {
    Metadata {
        symbol: String,
        tick: f64,
        step: f64,
        min_qty: f64,
        min_notional: f64,
    },
    Snapshot {
        symbol: String,
        last_id: u64,
        bids: Vec<[f64; 2]>,
        asks: Vec<[f64; 2]>,
    },
    Depth {
        symbol: String,
        first_id: u64,
        last_id: u64,
        prev_id: u64,
        bids: Vec<[f64; 2]>,
        asks: Vec<[f64; 2]>,
    },
    Trade {
        symbol: String,
        id: u64,
        price: f64,
        qty: f64,
        buyer_maker: bool,
    },
    Mark {
        symbol: String,
        price: f64,
        rate: f64,
        next_funding_ns: u64,
    },
    Funding {
        symbol: String,
        funding_ns: u64,
        rate: f64,
        mark: f64,
    },
    Connection {
        source: String,
        connected: bool,
    },
    Disconnect {
        reason: String,
    },
    Timer,
}
impl Kind {
    pub fn symbol(&self) -> Option<&str> {
        match self {
            Self::Metadata { symbol, .. }
            | Self::Snapshot { symbol, .. }
            | Self::Depth { symbol, .. }
            | Self::Trade { symbol, .. }
            | Self::Mark { symbol, .. }
            | Self::Funding { symbol, .. } => Some(symbol),
            _ => None,
        }
    }
}
