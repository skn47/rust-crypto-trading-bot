use futures_util::{SinkExt, StreamExt};
use microengine::{
    config::Config,
    engine::Engine,
    io::{LiveStore, events},
    recorder,
    types::Kind,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn coinbase_config(rest: &str, public: &str, market: &str) -> Config {
    let mut c: Config = toml::from_str(include_str!("../config/coinbase.toml")).unwrap();
    c.rest_url = format!("http://{rest}");
    c.public_ws = format!("ws://{public}/");
    c.market_ws = format!("ws://{market}/");
    c
}

async fn serve_products(rest: TcpListener) {
    loop {
        let (mut socket, _) = rest.accept().await.unwrap();
        tokio::spawn(async move {
            let mut bytes = vec![0; 8192];
            let n = socket.read(&mut bytes).await.unwrap();
            let request = String::from_utf8_lossy(&bytes[..n]).to_string();
            let symbol = if request.contains("BTC-USD") {
                "BTC-USD"
            } else {
                "ETH-USD"
            };
            let body = json!({
                "product_id": symbol, "status": "online",
                "quote_increment": "0.01", "base_increment": "0.00000001",
                "base_min_size": "0.00000001", "quote_min_size": "1"
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
    }
}

/// Waits for the client's subscribe messages (one per channel), then returns
/// the accepted WebSocket ready to send data. Takes the listener by
/// reference so a test can accept a second connection after a reconnect.
async fn accept_after_subscribe(
    listener: &TcpListener,
    channels: usize,
) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    let (socket, _) = listener.accept().await.unwrap();
    let mut ws = accept_async(socket).await.unwrap();
    for _ in 0..channels {
        ws.next().await.unwrap().unwrap();
    }
    ws
}

#[tokio::test]
async fn coinbase_recording_reconnect_and_replay() {
    let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let market = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c = coinbase_config(
        &rest.local_addr().unwrap().to_string(),
        &public.local_addr().unwrap().to_string(),
        &market.local_addr().unwrap().to_string(),
    );

    let http = tokio::spawn(serve_products(rest));

    let depth = tokio::spawn(async move {
        let mut ws = accept_after_subscribe(&public, 2).await;
        let mut seq = 0u64;
        for s in ["BTC-USD", "ETH-USD"] {
            let msg = json!({
                "channel": "l2_data", "sequence_num": seq,
                "events": [{"type": "snapshot", "product_id": s, "updates": [
                    {"side": "bid", "price_level": "99.9", "new_quantity": "10"},
                    {"side": "offer", "price_level": "100.1", "new_quantity": "10"}
                ]}]
            });
            ws.send(Message::Text(msg.to_string().into()))
                .await
                .unwrap();
            seq += 1;
        }
        for _ in 0..200_u64 {
            for s in ["BTC-USD", "ETH-USD"] {
                let msg = json!({
                    "channel": "l2_data", "sequence_num": seq,
                    "events": [{"type": "update", "product_id": s, "updates": [
                        {"side": "bid", "price_level": "99.9", "new_quantity": "10"},
                        {"side": "offer", "price_level": "100.1", "new_quantity": "10"}
                    ]}]
                });
                if ws
                    .send(Message::Text(msg.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
                seq += 1;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });
    let trades = tokio::spawn(async move {
        let mut ws = accept_after_subscribe(&market, 2).await;
        let mut seq = 0u64;
        for id in 1..200_u64 {
            for s in ["BTC-USD", "ETH-USD"] {
                let msg = json!({
                    "channel": "market_trades", "sequence_num": seq,
                    "events": [{"type": "update", "trades": [
                        {"product_id": s, "trade_id": id.to_string(), "price": "100", "size": "0.01", "side": "BUY"}
                    ]}]
                });
                if ws
                    .send(Message::Text(msg.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
                seq += 1;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });

    let dir =
        std::env::temp_dir().join(format!("microengine-coinbase-test-{}", recorder::utc_ns()));
    let log = dir.join("events.jsonl");
    recorder::run(
        LiveStore::open(&log, c.clone(), vec![], false).unwrap(),
        Some(8),
    )
    .await
    .unwrap();
    http.abort();
    depth.abort();
    trades.abort();

    let mut replay = Engine::new(c, vec![]).unwrap();
    let mut samples = 0;
    for e in events(&log).unwrap() {
        samples += usize::from(replay.on_event(&e.unwrap()).unwrap().0.is_some());
    }
    assert!(
        samples > 0,
        "must warm up and emit features from Coinbase data"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

fn l2_update_msg(symbol: &str, seq: u64) -> Value {
    json!({
        "channel": "l2_data", "sequence_num": seq,
        "events": [{"type": "update", "product_id": symbol, "updates": [
            {"side": "bid", "price_level": "99.9", "new_quantity": "10"},
            {"side": "offer", "price_level": "100.1", "new_quantity": "10"}
        ]}]
    })
}
fn l2_snapshot_msg(symbol: &str, seq: u64) -> Value {
    json!({
        "channel": "l2_data", "sequence_num": seq,
        "events": [{"type": "snapshot", "product_id": symbol, "updates": [
            {"side": "bid", "price_level": "99.9", "new_quantity": "10"},
            {"side": "offer", "price_level": "100.1", "new_quantity": "10"}
        ]}]
    })
}

#[tokio::test]
async fn coinbase_sequence_gap_forces_reconnect_and_resnapshot() {
    let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let market = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c = coinbase_config(
        &rest.local_addr().unwrap().to_string(),
        &public.local_addr().unwrap().to_string(),
        &market.local_addr().unwrap().to_string(),
    );

    let http = tokio::spawn(serve_products(rest));

    // First connection: skips one sequence_num a few frames in, simulating a
    // missed message. A second connection follows once the client reconnects.
    let depth = tokio::spawn(async move {
        for connection in 0..2 {
            let mut ws = accept_after_subscribe(&public, 2).await;
            let mut seq = 0u64;
            for s in ["BTC-USD", "ETH-USD"] {
                ws.send(Message::Text(l2_snapshot_msg(s, seq).to_string().into()))
                    .await
                    .unwrap();
                seq += 1;
            }
            'frames: for i in 0..200_u64 {
                if connection == 0 && i == 2 {
                    seq += 1; // simulate a dropped message
                }
                for s in ["BTC-USD", "ETH-USD"] {
                    if ws
                        .send(Message::Text(l2_update_msg(s, seq).to_string().into()))
                        .await
                        .is_err()
                    {
                        break 'frames;
                    }
                    seq += 1;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    });
    let trades = tokio::spawn(async move {
        let mut ws = accept_after_subscribe(&market, 2).await;
        let mut seq = 0u64;
        for id in 1..300_u64 {
            for s in ["BTC-USD", "ETH-USD"] {
                let msg = json!({
                    "channel": "market_trades", "sequence_num": seq,
                    "events": [{"type": "update", "trades": [
                        {"product_id": s, "trade_id": id.to_string(), "price": "100", "size": "0.01", "side": "BUY"}
                    ]}]
                });
                if ws
                    .send(Message::Text(msg.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
                seq += 1;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });

    let dir = std::env::temp_dir().join(format!(
        "microengine-coinbase-gap-test-{}",
        recorder::utc_ns()
    ));
    let log = dir.join("events.jsonl");
    recorder::run(
        LiveStore::open(&log, c.clone(), vec![], false).unwrap(),
        Some(12),
    )
    .await
    .unwrap();
    http.abort();
    depth.abort();
    trades.abort();

    let logged: Vec<_> = events(&log).unwrap().map(|e| e.unwrap()).collect();
    assert!(
        logged.iter().any(|e| matches!(&e.kind,
            Kind::Disconnect { reason } if reason.contains("sequence gap"))),
        "log must record a Disconnect mentioning the sequence gap"
    );
    for s in ["BTC-USD", "ETH-USD"] {
        let snapshots = logged
            .iter()
            .filter(|e| matches!(&e.kind, Kind::Snapshot { symbol, .. } if symbol == s))
            .count();
        assert!(
            snapshots >= 2,
            "{s} must be re-snapshotted after the forced reconnect, got {snapshots}"
        );
    }

    let mut replay = Engine::new(c, vec![]).unwrap();
    let mut samples = 0;
    for e in &logged {
        samples += usize::from(replay.on_event(e).unwrap().0.is_some());
    }
    assert!(samples > 0, "must recover and emit features after the gap");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn coinbase_trade_snapshot_is_not_recorded_as_a_trade() {
    let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let market = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c = coinbase_config(
        &rest.local_addr().unwrap().to_string(),
        &public.local_addr().unwrap().to_string(),
        &market.local_addr().unwrap().to_string(),
    );

    let http = tokio::spawn(serve_products(rest));
    let depth = tokio::spawn(async move {
        let mut ws = accept_after_subscribe(&public, 2).await;
        let mut seq = 0u64;
        for s in ["BTC-USD", "ETH-USD"] {
            ws.send(Message::Text(l2_snapshot_msg(s, seq).to_string().into()))
                .await
                .unwrap();
            seq += 1;
        }
        for _ in 0..80_u64 {
            for s in ["BTC-USD", "ETH-USD"] {
                if ws
                    .send(Message::Text(l2_update_msg(s, seq).to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
                seq += 1;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });
    // The very first market_trades message is a snapshot of historical
    // trades, tagged with a distinctive id that must never appear as a
    // recorded Trade event.
    let trades = tokio::spawn(async move {
        let mut ws = accept_after_subscribe(&market, 2).await;
        let mut seq = 0u64;
        let snapshot = json!({
            "channel": "market_trades", "sequence_num": seq,
            "events": [{"type": "snapshot", "trades": [
                {"product_id": "BTC-USD", "trade_id": "999999", "price": "100", "size": "1", "side": "BUY"}
            ]}]
        });
        ws.send(Message::Text(snapshot.to_string().into()))
            .await
            .unwrap();
        seq += 1;
        for id in 1..80_u64 {
            for s in ["BTC-USD", "ETH-USD"] {
                let msg = json!({
                    "channel": "market_trades", "sequence_num": seq,
                    "events": [{"type": "update", "trades": [
                        {"product_id": s, "trade_id": id.to_string(), "price": "100", "size": "0.01", "side": "BUY"}
                    ]}]
                });
                if ws
                    .send(Message::Text(msg.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
                seq += 1;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });

    let dir = std::env::temp_dir().join(format!(
        "microengine-coinbase-trade-snapshot-test-{}",
        recorder::utc_ns()
    ));
    let log = dir.join("events.jsonl");
    recorder::run(
        LiveStore::open(&log, c.clone(), vec![], false).unwrap(),
        Some(8),
    )
    .await
    .unwrap();
    http.abort();
    depth.abort();
    trades.abort();

    let has_snapshot_trade = events(&log)
        .unwrap()
        .map(|e| e.unwrap())
        .any(|e| matches!(&e.kind, Kind::Trade { id, .. } if *id == 999999));
    assert!(
        !has_snapshot_trade,
        "a market_trades snapshot must not be recorded as a Trade event"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
