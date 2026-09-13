use futures_util::SinkExt;
use microengine::{
    config::Config,
    engine::Engine,
    io::{LiveStore, events},
    recorder,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test]
async fn public_stream_recording_snapshot_gap_recovery_and_replay() {
    let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let market = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c: Config = toml::from_str(include_str!("../config/default.toml")).unwrap();
    c.rest_url = format!("http://{}", rest.local_addr().unwrap());
    c.public_ws = format!("ws://{}/public/stream", public.local_addr().unwrap());
    c.market_ws = format!("ws://{}/market/stream", market.local_addr().unwrap());
    let last = Arc::new(AtomicU64::new(11));
    let snapshot_id = last.clone();
    let http = tokio::spawn(async move {
        loop {
            let (mut socket, _) = rest.accept().await.unwrap();
            let id = snapshot_id.clone();
            tokio::spawn(async move {
                let mut bytes = vec![0; 8192];
                let n = socket.read(&mut bytes).await.unwrap();
                let request = String::from_utf8_lossy(&bytes[..n]);
                let body=if request.contains("exchangeInfo") {
                    json!({"symbols":(["BTCUSDT","ETHUSDT"].iter().map(|s|json!({"symbol":s,"status":"TRADING","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.1"},{"filterType":"MARKET_LOT_SIZE","stepSize":"0.001","minQty":"0.001"},{"filterType":"MIN_NOTIONAL","notional":"5"}]})).collect::<Vec<_>>())})
                } else if request.contains("/depth?") {
                    json!({"lastUpdateId":id.load(Ordering::SeqCst),"bids":[["99.9","10"]],"asks":[["100.1","10"]]})
                } else {json!([])}.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    let depth = tokio::spawn(async move {
        let (socket, _) = public.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        for frame in 0..200_u64 {
            last.store(11 + frame, Ordering::SeqCst);
            for s in ["BTCUSDT", "ETHUSDT"] {
                let msg=json!({"e":"depthUpdate","s":s,"E":1,"U":if frame==0 {10}else{11+frame},"u":11+frame,"pu":if frame==20 {999}else{10+frame},"b":[["99.9","10"]],"a":[["100.1","10"]]}).to_string();
                if ws.send(Message::Text(msg.into())).await.is_err() {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });
    let trades = tokio::spawn(async move {
        let (socket, _) = market.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        for id in 1..200_u64 {
            for s in ["BTCUSDT", "ETHUSDT"] {
                let msg = json!({"e":"aggTrade","s":s,"E":1,"a":id,"p":"100","q":"0.01","m":false})
                    .to_string();
                if ws.send(Message::Text(msg.into())).await.is_err() {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });
    let dir = std::env::temp_dir().join(format!("microengine-network-test-{}", recorder::utc_ns()));
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
        "must recover from injected gap, warm up, and emit features"
    );
    assert!(replay.gaps > 0);
    assert!(
        replay.books.iter().all(|b| !b.synced),
        "shutdown invalidates books"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
