use microengine::{
    book::Book,
    config::Config,
    engine::Engine,
    features,
    io::LiveStore,
    model::Model,
    paper::Broker,
    types::{Event, Kind, MS},
    venue::coinbase,
};

fn config() -> Config {
    toml::from_str(include_str!("../config/default.toml")).unwrap()
}
fn coinbase_config() -> Config {
    toml::from_str(include_str!("../config/coinbase.toml")).unwrap()
}
fn event(seq: u64, ms: u64, kind: Kind) -> Event {
    Event {
        version: 1,
        session: "test".into(),
        seq,
        recv_ns: 1_700_000_000_000_000_000 + ms * MS,
        utc_ns: 1_700_000_000_000_000_000 + ms * MS,
        exchange_ns: None,
        raw: None,
        kind,
    }
}
fn metadata(s: &str) -> Kind {
    Kind::Metadata {
        symbol: s.into(),
        tick: 0.1,
        step: 0.001,
        min_qty: 0.001,
        min_notional: 5.0,
    }
}
fn snapshot(s: &str) -> Kind {
    Kind::Snapshot {
        symbol: s.into(),
        last_id: 10,
        bids: vec![[99.9, 10.0]],
        asks: vec![[100.1, 10.0]],
    }
}
fn depth(s: &str, last: u64, prev: u64) -> Kind {
    Kind::Depth {
        symbol: s.into(),
        first_id: if last == 11 { 10 } else { last },
        last_id: last,
        prev_id: prev,
        bids: vec![[99.9, 10.0]],
        asks: vec![[100.1, 10.0]],
    }
}
fn book() -> Book {
    let mut b = Book::default();
    for (i, k) in [
        metadata("BTCUSDT"),
        snapshot("BTCUSDT"),
        depth("BTCUSDT", 11, 10),
    ]
    .into_iter()
    .enumerate()
    {
        b.apply(&event(i as u64 + 1, 0, k)).unwrap();
    }
    b
}
fn stream(frames: u64) -> Vec<Event> {
    let mut out = Vec::new();
    let mut add = |ms, k| out.push(event(out.len() as u64 + 1, ms, k));
    for source in ["public", "market"] {
        add(
            0,
            Kind::Connection {
                source: source.into(),
                connected: true,
            },
        );
    }
    for s in ["BTCUSDT", "ETHUSDT"] {
        add(0, metadata(s));
        add(0, snapshot(s));
    }
    for i in 0..frames {
        for s in ["BTCUSDT", "ETHUSDT"] {
            add(i * 100, depth(s, 11 + i, 10 + i));
        }
        add(i * 100, Kind::Timer);
    }
    out
}
fn coinbase_l2(symbol: &str, msg_type: &str, seq_num: u64) -> serde_json::Value {
    serde_json::json!({
        "channel": "l2_data",
        "sequence_num": seq_num,
        "events": [{
            "type": msg_type,
            "product_id": symbol,
            "updates": [
                {"side": "bid", "price_level": "99.9", "new_quantity": "10"},
                {"side": "offer", "price_level": "100.1", "new_quantity": "10"}
            ]
        }]
    })
}
fn coinbase_stream(frames: u64) -> Vec<Event> {
    let mut decoder = coinbase::Decoder::default();
    let mut out = Vec::new();
    let mut add = |ms, k| out.push(event(out.len() as u64 + 1, ms, k));
    for source in ["public", "market"] {
        add(
            0,
            Kind::Connection {
                source: source.into(),
                connected: true,
            },
        );
    }
    for s in ["BTC-USD", "ETH-USD"] {
        add(0, metadata(s));
    }
    let mut seq_num = 0u64;
    for s in ["BTC-USD", "ETH-USD"] {
        seq_num += 1;
        let (kind, _) = decoder
            .decode(&coinbase_l2(s, "snapshot", seq_num))
            .unwrap()
            .remove(0);
        add(0, kind);
    }
    for i in 0..frames {
        for s in ["BTC-USD", "ETH-USD"] {
            seq_num += 1;
            let (kind, _) = decoder
                .decode(&coinbase_l2(s, "update", seq_num))
                .unwrap()
                .remove(0);
            add(i * 100, kind);
        }
        add(i * 100, Kind::Timer);
    }
    out
}
fn model() -> Model {
    Model {
        version: 1,
        symbol: "BTCUSDT".into(),
        features: features::names(&config().symbols),
        mean: vec![0.0; 37],
        scale: vec![1.0; 37],
        coefficients: vec![0.0; 37],
        intercept: 100.0,
        horizon_ms: 1000,
        trained_through_ns: 0,
        selected_through_ns: 0,
        dataset_sha256: "0".repeat(64),
        entry_buffer_bps: 0.0,
        alpha: 1.0,
    }
}

#[test]
fn engine_accepts_a_coinbase_configuration() {
    assert!(Engine::new(coinbase_config(), vec![]).is_ok());
}

#[test]
fn engine_validates_model_feature_names_against_venue_symbols() {
    let cb = coinbase_config();
    let mut coinbase_model = model();
    coinbase_model.symbol = "BTC-USD".into();
    coinbase_model.features = features::names(&cb.symbols);
    assert!(Engine::new(cb.clone(), vec![coinbase_model]).is_ok());

    let binance_model = model();
    assert!(Engine::new(cb, vec![binance_model]).is_err());
}

#[test]
fn coinbase_sequence_stays_valid_and_emits_samples_after_warmup() {
    let mut eng = Engine::new(coinbase_config(), vec![]).unwrap();
    let mut samples = 0;
    for e in coinbase_stream(60) {
        if eng.on_event(&e).unwrap().0.is_some() {
            samples += 1;
        }
    }
    assert!(eng.valid_at(eng.now));
    assert!(samples > 0);
}

#[test]
fn coinbase_buy_trade_pushes_100ms_trade_flow_negative() {
    let mut eng = Engine::new(coinbase_config(), vec![]).unwrap();
    let mut seq = 0u64;
    for e in coinbase_stream(60) {
        seq = e.seq;
        eng.on_event(&e).unwrap();
    }
    let trade_raw = serde_json::json!({
        "channel": "market_trades",
        "sequence_num": 1,
        "events": [{
            "type": "update",
            "trades": [{"product_id": "BTC-USD", "trade_id": "1", "price": "100", "size": "0.5", "side": "BUY"}]
        }]
    });
    let (trade_kind, _) = coinbase::Decoder::default()
        .decode(&trade_raw)
        .unwrap()
        .remove(0);
    seq += 1;
    eng.on_event(&event(seq, 6010, trade_kind)).unwrap();
    seq += 1;
    let (sample, _) = eng.on_event(&event(seq, 6090, Kind::Timer)).unwrap();
    let names = features::names(&coinbase_config().symbols);
    let idx = names
        .iter()
        .position(|n| n == "BTC-USD.trade_flow_100ms")
        .unwrap();
    assert_eq!(sample.unwrap().features[idx], -1.0);
}

#[test]
fn snapshot_bridge_duplicates_gap_and_deletion() {
    let mut b = book();
    assert!(b.synced);
    b.apply(&event(4, 100, depth("BTCUSDT", 11, 10))).unwrap();
    assert_eq!(b.updated_ns, event(1, 0, Kind::Timer).recv_ns);
    assert!(!b.apply(&event(5, 200, depth("BTCUSDT", 15, 13))).unwrap());
    assert!(!b.synced);
    assert_eq!(b.buffer.len(), 1);
    let snap = Kind::Snapshot {
        symbol: "BTCUSDT".into(),
        last_id: 15,
        bids: vec![[99.9, 10.0]],
        asks: vec![[100.1, 10.0]],
    };
    b.apply(&event(6, 300, snap)).unwrap();
    assert!(b.synced);
    let delete = Kind::Depth {
        symbol: "BTCUSDT".into(),
        first_id: 16,
        last_id: 16,
        prev_id: 15,
        bids: vec![[99.8, 0.0], [99.9, 0.0], [99.7, 2.0]],
        asks: vec![],
    };
    b.apply(&event(7, 400, delete)).unwrap();
    assert_eq!(b.top().unwrap().0, 99.7);
}

#[test]
fn buffered_snapshot_is_available_only_when_received() {
    let mut b = Book::default();
    b.apply(&event(1, 0, metadata("BTCUSDT"))).unwrap();
    b.apply(&event(2, 10, depth("BTCUSDT", 11, 10))).unwrap();
    assert!(!b.synced);
    b.apply(&event(3, 100, snapshot("BTCUSDT"))).unwrap();
    assert_eq!(b.updated_ns, event(3, 100, Kind::Timer).recv_ns);
    assert!(!b.fresh(event(2, 10, Kind::Timer).recv_ns, 500));
}

#[test]
fn prefix_invariance_and_exchange_timestamps_do_not_reorder_inputs() {
    let events = stream(100);
    let mut a = Engine::new(config(), vec![model()]).unwrap();
    let mut b = a.clone();
    let mut prefix = Vec::new();
    let mut other = Vec::new();
    for e in &events[..200] {
        prefix.push(serde_json::to_value(a.on_event(e).unwrap()).unwrap());
    }
    for e in &events[..200] {
        let mut changed = e.clone();
        changed.exchange_ns = Some(u64::MAX - e.seq);
        other.push(serde_json::to_value(b.on_event(&changed).unwrap()).unwrap());
    }
    assert_eq!(prefix, other);
    let before = prefix.clone();
    for e in &events[200..] {
        a.on_event(e).unwrap();
    }
    assert_eq!(prefix, before);
    assert!(a.broker.turnover > 0.0);
}

#[test]
fn stale_or_disconnected_cross_asset_feed_stops_signals() {
    let mut eng = Engine::new(config(), vec![model()]).unwrap();
    for e in stream(70) {
        eng.on_event(&e).unwrap();
    }
    let seq = eng.last_seq + 1;
    let (s, _) = eng.on_event(&event(seq, 8000, Kind::Timer)).unwrap();
    assert!(s.is_none());
    assert!(eng.healthy_since.is_none());
    eng.on_event(&event(
        seq + 1,
        8100,
        Kind::Connection {
            source: "market".into(),
            connected: false,
        },
    ))
    .unwrap();
    assert!(!eng.valid_at(eng.now));
}

#[test]
fn artifact_validation_and_training_cutoff_are_enforced() {
    let mut m = model();
    m.scale[0] = 0.0;
    assert!(m.validate(&config().symbols).is_err());
    m = model();
    m.features.swap(0, 1);
    assert!(m.validate(&config().symbols).is_err());
    m = model();
    m.selected_through_ns = u64::MAX;
    let mut eng = Engine::new(config(), vec![m]).unwrap();
    assert!(stream(70).iter().any(|e| eng.on_event(e).is_err()));
}

#[test]
fn delayed_fill_partial_exit_and_fees() {
    let c = config();
    let mut broker = Broker::new(c.equity);
    let mut books = [book(), book()];
    let e = event(1, 0, Kind::Timer);
    broker.on_event(&e, &books, &c, None, true).unwrap();
    broker.decide(0, 100.0, 0.0, &e, &books, &c);
    assert!(broker.pending[0].is_some());
    broker
        .on_event(
            &event(2, 49, depth("BTCUSDT", 12, 11)),
            &books,
            &c,
            Some(0),
            true,
        )
        .unwrap();
    assert_eq!(broker.positions[0].qty, 0.0);
    books[0].updated_ns = event(3, 100, Kind::Timer).recv_ns;
    broker
        .on_event(
            &event(3, 100, depth("BTCUSDT", 12, 11)),
            &books,
            &c,
            Some(0),
            true,
        )
        .unwrap();
    assert_eq!(broker.positions[0].qty, 1.0);
    assert!(broker.fees > 0.0);
    books[0].updated_ns = event(4, 1100, Kind::Timer).recv_ns;
    broker
        .on_event(&event(4, 1100, Kind::Timer), &books, &c, None, true)
        .unwrap();
    assert!(broker.pending[0].as_ref().unwrap().reduce);
    books[0].bids.insert(999, 0.4);
    books[0].updated_ns = event(5, 1200, Kind::Timer).recv_ns;
    broker
        .on_event(
            &event(5, 1200, depth("BTCUSDT", 13, 12)),
            &books,
            &c,
            Some(0),
            true,
        )
        .unwrap();
    assert!((broker.positions[0].qty - 0.6).abs() < 1e-9);
    assert!(broker.pending[0].is_some());
    books[0].bids.insert(999, 1.0);
    books[0].updated_ns = event(6, 1300, Kind::Timer).recv_ns;
    broker
        .on_event(
            &event(6, 1300, depth("BTCUSDT", 14, 13)),
            &books,
            &c,
            Some(0),
            true,
        )
        .unwrap();
    assert_eq!(broker.positions[0].qty, 0.0);
    assert!(broker.realized < 0.0);
}

#[test]
fn pending_exposure_and_loss_stop() {
    let mut c = config();
    c.gross_cap = 100.0;
    let mut b = Broker::new(c.equity);
    let books = [book(), book()];
    let e = event(1, 0, Kind::Timer);
    b.on_event(&e, &books, &c, None, true).unwrap();
    b.decide(0, 100.0, 0.0, &e, &books, &c);
    b.decide(1, 100.0, 0.0, &e, &books, &c);
    assert!(b.pending[0].is_some());
    assert!(b.pending[1].is_none());
    assert_eq!(b.rejected, 1);
    b.cash -= 101.0;
    b.on_event(&event(2, 1, Kind::Timer), &books, &c, None, true)
        .unwrap();
    assert!(b.stopped);
    assert!(b.pending[0].is_none());
}

#[test]
fn late_funding_uses_historical_position_once() {
    let c = config();
    let mut b = Broker::new(c.equity);
    let books = [book(), book()];
    let t = event(1, 0, Kind::Timer).utc_ns;
    b.position_history[0] = vec![(t, 2.0), (t + 200 * MS, 0.0)];
    let k = Kind::Funding {
        symbol: "BTCUSDT".into(),
        funding_ns: t + 100 * MS,
        rate: 0.001,
        mark: 100.0,
    };
    b.on_event(&event(1, 300, k.clone()), &books, &c, None, true)
        .unwrap();
    b.on_event(&event(2, 400, k), &books, &c, None, true)
        .unwrap();
    assert!((b.funding + 0.2).abs() < 1e-9);
}

#[test]
fn journal_and_state_recover_uncheckpointed_tail_identically() {
    let dir = std::env::temp_dir().join(format!(
        "microengine-test-{}-{}",
        std::process::id(),
        microengine::recorder::utc_ns()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("events.jsonl");
    let mut live = LiveStore::open(&path, config(), vec![model()], false).unwrap();
    let events = stream(90);
    let mut replay = Engine::new(config(), vec![model()]).unwrap();
    for (i, e) in events.iter().enumerate() {
        live.push(e).unwrap();
        replay.on_event(e).unwrap();
        if i == 180 {
            live.checkpoint_background().unwrap();
        }
    }
    drop(live);
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{partial")
        .unwrap();
    let live = LiveStore::open(&path, config(), vec![model()], true).unwrap();
    assert_eq!(
        serde_json::to_value(&live.engine).unwrap(),
        serde_json::to_value(&replay).unwrap()
    );
    let actual = std::fs::read_to_string(path.with_extension("journal.jsonl")).unwrap();
    let mut expected = Vec::new();
    let mut engine = Engine::new(config(), vec![model()]).unwrap();
    for e in events {
        for row in engine.on_event(&e).unwrap().1 {
            microengine::io::json_line(&mut expected, &row).unwrap();
        }
    }
    assert_eq!(actual.as_bytes(), expected);
    drop(live);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn malformed_sequence_and_crossed_books_fail_closed() {
    let mut eng = Engine::new(config(), vec![]).unwrap();
    let e = stream(1).remove(0);
    eng.on_event(&e).unwrap();
    assert!(eng.on_event(&e).is_err());
    let mut b = book();
    let crossed = Kind::Depth {
        symbol: "BTCUSDT".into(),
        first_id: 12,
        last_id: 12,
        prev_id: 11,
        bids: vec![[100.2, 1.0]],
        asks: vec![],
    };
    assert!(!b.apply(&event(4, 100, crossed)).unwrap());
    assert!(!b.synced);
}

#[test]
fn duplicate_depth_does_not_replenish_consumed_liquidity() {
    let mut engine = Engine::new(config(), vec![]).unwrap();
    for e in stream(70) {
        engine.on_event(&e).unwrap();
    }
    engine.broker.consumed[1].insert(1001, 3.0);
    let id = engine.books[0].last_id.unwrap();
    engine
        .on_event(&event(
            engine.last_seq + 1,
            6910,
            depth("BTCUSDT", id, id - 1),
        ))
        .unwrap();
    assert_eq!(engine.broker.consumed[1].get(&1001), Some(&3.0));
}

#[test]
fn second_writer_cannot_resume_an_active_run() {
    let dir = std::env::temp_dir().join(format!(
        "microengine-lock-test-{}",
        microengine::recorder::utc_ns()
    ));
    let path = dir.join("events.jsonl");
    let mut live = LiveStore::open(&path, config(), vec![], false).unwrap();
    live.checkpoint().unwrap();
    assert!(LiveStore::open(&path, config(), vec![], true).is_err());
    drop(live);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn return_windows_use_book_events_between_timer_samples() {
    let mut b = book();
    let mut h = microengine::features::History::default();
    for i in 0..=50 {
        h.sample(event(1, i * 100, Kind::Timer).recv_ns, &b);
    }
    b.bids.clear();
    b.asks.clear();
    b.bids.insert(1009, 10.0);
    b.asks.insert(1011, 10.0);
    h.observe(&event(2, 5050, depth("BTCUSDT", 12, 11)), &b);
    b.bids.clear();
    b.asks.clear();
    b.bids.insert(1019, 10.0);
    b.asks.insert(1021, 10.0);
    h.observe(&event(3, 5150, depth("BTCUSDT", 13, 12)), &b);
    let now = event(4, 5150, Kind::Timer).recv_ns;
    h.sample(now, &b);
    let v = h.vector(now, &b).unwrap();
    assert!((v[7] - 10000.0 * (102.0_f64 / 101.0).ln()).abs() < 1e-8);
}
