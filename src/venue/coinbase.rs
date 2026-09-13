use crate::{config::Config, types::Kind};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::collections::HashMap;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Fetches per-symbol instrument metadata from Coinbase's public product
/// endpoint (no authentication required).
pub async fn metadata(client: &reqwest::Client, c: &Config) -> Result<Vec<(Kind, Value)>> {
    let mut out = Vec::new();
    for s in &c.symbols {
        let v: Value = client
            .get(format!(
                "{}/api/v3/brokerage/market/products/{s}",
                c.rest_url
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(v["status"] == "online", "instrument not online");
        out.push((
            Kind::Metadata {
                symbol: s.clone(),
                tick: num(&v["quote_increment"])?,
                step: num(&v["base_increment"])?,
                min_qty: num(&v["base_min_size"])?,
                min_notional: num(&v["quote_min_size"])?,
            },
            v,
        ));
    }
    Ok(out)
}

fn num(v: &Value) -> Result<f64> {
    v.as_str()
        .context("missing number")?
        .parse()
        .context("invalid number")
}
fn id(v: &Value) -> Result<u64> {
    v.as_str()
        .context("missing id")?
        .parse()
        .context("invalid id")
}
fn levels(updates: &[Value], side: &str) -> Result<Vec<[f64; 2]>> {
    updates
        .iter()
        .filter(|u| u["side"] == side)
        .map(|u| Ok([num(&u["price_level"])?, num(&u["new_quantity"])?]))
        .collect()
}
fn exchange_ns(raw: &Value) -> Result<Option<u64>> {
    match raw["timestamp"].as_str() {
        Some(s) => {
            let t = OffsetDateTime::parse(s, &Rfc3339).context("invalid timestamp")?;
            Ok(Some(t.unix_timestamp_nanos() as u64))
        }
        None => Ok(None),
    }
}
/// Decodes Coinbase Advanced Trade WebSocket messages, tracking each product's
/// last-seen `sequence_num` so `l2_data` updates can be chained into the ids
/// `Book::apply` expects (see `types::Kind::Depth`). Sequence numbers are
/// assigned per connection, not per product, so this state cannot be derived
/// from a single message in isolation.
#[derive(Default)]
pub struct Decoder {
    last_seq: HashMap<String, u64>,
}
impl Decoder {
    pub fn decode(&mut self, raw: &Value) -> Result<Vec<(Kind, Option<u64>)>> {
        let channel = raw["channel"].as_str().context("missing channel")?;
        let ts = exchange_ns(raw)?;
        let mut out = Vec::new();
        if channel == "l2_data" {
            let seq = raw["sequence_num"]
                .as_u64()
                .context("missing sequence_num")?;
            for ev in raw["events"].as_array().context("missing events")? {
                let symbol = ev["product_id"]
                    .as_str()
                    .context("missing product_id")?
                    .to_owned();
                let updates = ev["updates"].as_array().context("missing updates")?;
                let bids = levels(updates, "bid")?;
                let asks = levels(updates, "offer")?;
                let kind = if ev["type"] == "snapshot" {
                    Kind::Snapshot {
                        symbol: symbol.clone(),
                        last_id: seq,
                        bids,
                        asks,
                    }
                } else {
                    let prev = self.last_seq.get(&symbol).copied().unwrap_or(0);
                    Kind::Depth {
                        symbol: symbol.clone(),
                        first_id: prev,
                        last_id: seq,
                        prev_id: prev,
                        bids,
                        asks,
                    }
                };
                self.last_seq.insert(symbol, seq);
                out.push((kind, ts));
            }
        } else if channel == "market_trades" {
            for ev in raw["events"].as_array().context("missing events")? {
                if ev["type"] == "update" {
                    for t in ev["trades"].as_array().context("missing trades")? {
                        out.push((
                            Kind::Trade {
                                symbol: t["product_id"]
                                    .as_str()
                                    .context("missing product_id")?
                                    .to_owned(),
                                id: id(&t["trade_id"])?,
                                price: num(&t["price"])?,
                                qty: num(&t["size"])?,
                                buyer_maker: t["side"] == "BUY",
                            },
                            ts,
                        ));
                    }
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn l2_snapshot_maps_bid_to_bids_and_offer_to_asks() {
        let raw = json!({
            "channel": "l2_data",
            "sequence_num": 0,
            "events": [{
                "type": "snapshot",
                "product_id": "BTC-USD",
                "updates": [
                    {"side": "bid", "price_level": "77259.9", "new_quantity": "0.71361683"},
                    {"side": "offer", "price_level": "77260.5", "new_quantity": "0.42380834"}
                ]
            }]
        });
        let out = Decoder::default().decode(&raw).unwrap();
        assert_eq!(out.len(), 1);
        match &out[0].0 {
            Kind::Snapshot {
                symbol, bids, asks, ..
            } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(bids, &vec![[77259.9, 0.71361683]]);
                assert_eq!(asks, &vec![[77260.5, 0.42380834]]);
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }

    #[test]
    fn l2_update_maps_bid_to_bids_and_offer_to_asks() {
        let raw = json!({
            "channel": "l2_data",
            "sequence_num": 1,
            "events": [{
                "type": "update",
                "product_id": "ETH-USD",
                "updates": [
                    {"side": "bid", "price_level": "2518.79", "new_quantity": "0.8437"},
                    {"side": "offer", "price_level": "2522.36", "new_quantity": "0.42380834"}
                ]
            }]
        });
        let out = Decoder::default().decode(&raw).unwrap();
        assert_eq!(out.len(), 1);
        match &out[0].0 {
            Kind::Depth {
                symbol, bids, asks, ..
            } => {
                assert_eq!(symbol, "ETH-USD");
                assert_eq!(bids, &vec![[2518.79, 0.8437]]);
                assert_eq!(asks, &vec![[2522.36, 0.42380834]]);
            }
            other => panic!("expected Depth, got {other:?}"),
        }
    }

    #[test]
    fn market_trades_update_maps_side_to_buyer_maker() {
        let raw = json!({
            "channel": "market_trades",
            "sequence_num": 27,
            "events": [{
                "type": "update",
                "trades": [
                    {"product_id": "BTC-USD", "trade_id": "1092094324", "price": "77257.26", "size": "0.00001281", "side": "SELL"},
                    {"product_id": "BTC-USD", "trade_id": "1092094325", "price": "77257.30", "size": "0.005", "side": "BUY"}
                ]
            }]
        });
        let out = Decoder::default().decode(&raw).unwrap();
        assert_eq!(out.len(), 2);
        match &out[0].0 {
            Kind::Trade {
                symbol,
                id,
                price,
                qty,
                buyer_maker,
            } => {
                assert_eq!(symbol, "BTC-USD");
                assert_eq!(*id, 1092094324);
                assert_eq!(*price, 77257.26);
                assert_eq!(*qty, 0.00001281);
                assert!(!*buyer_maker);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
        match &out[1].0 {
            Kind::Trade { buyer_maker, .. } => assert!(*buyer_maker),
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn trade_snapshots_heartbeats_and_subscriptions_produce_no_events() {
        let trade_snapshot = json!({
            "channel": "market_trades",
            "sequence_num": 12,
            "events": [{
                "type": "snapshot",
                "trades": [
                    {"product_id": "BTC-USD", "trade_id": "1092094323", "price": "77257.25", "size": "0.00000212", "side": "BUY"}
                ]
            }]
        });
        let heartbeat = json!({
            "channel": "heartbeats",
            "sequence_num": 36,
            "events": [{"current_time": "2026-09-13 02:12:36 UTC", "heartbeat_counter": 213038}]
        });
        let subscriptions = json!({
            "channel": "subscriptions",
            "sequence_num": 10,
            "events": [{"subscriptions": {"level2": ["BTC-USD", "ETH-USD"]}}]
        });
        for raw in [&trade_snapshot, &heartbeat, &subscriptions] {
            assert!(
                Decoder::default().decode(raw).unwrap().is_empty(),
                "unexpected events for {raw}"
            );
        }
    }

    #[test]
    fn message_timestamp_becomes_exchange_ns() {
        let raw = json!({
            "channel": "l2_data",
            "timestamp": "2026-09-13T02:12:35.904376Z",
            "sequence_num": 1,
            "events": [{
                "type": "update",
                "product_id": "ETH-USD",
                "updates": [{"side": "bid", "price_level": "2518.79", "new_quantity": "0.8437"}]
            }]
        });
        let out = Decoder::default().decode(&raw).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, Some(1789265555904376000));
    }
}
