use crate::{
    config::Config,
    io::LiveStore,
    types::{Event, Kind, MS, SCHEMA, SECOND},
};
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub fn utc_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn num(v: &Value) -> Result<f64> {
    let n = v
        .as_str()
        .map(|s| s.parse())
        .transpose()?
        .or_else(|| v.as_f64())
        .context("missing number")?;
    ensure!(n.is_finite(), "nonfinite number");
    Ok(n)
}
fn id(v: &Value) -> Result<u64> {
    v.as_u64().context("missing integer")
}
fn levels(v: &Value) -> Result<Vec<[f64; 2]>> {
    v.as_array()
        .context("missing levels")?
        .iter()
        .map(|x| Ok([num(&x[0])?, num(&x[1])?]))
        .collect()
}
pub fn decode(raw: &Value) -> Result<Option<(Kind, Option<u64>)>> {
    let v = raw.get("data").unwrap_or(raw);
    if v.get("result").is_some() {
        return Ok(None);
    }
    let s = v["s"].as_str().context("missing symbol")?.to_owned();
    ensure!(
        v.get("st").and_then(Value::as_u64).is_none_or(|x| x == 1),
        "unexpected non-USD-M instrument"
    );
    let kind = match v["e"].as_str().context("missing event type")? {
        "depthUpdate" => Kind::Depth {
            symbol: s,
            first_id: id(&v["U"])?,
            last_id: id(&v["u"])?,
            prev_id: id(&v["pu"])?,
            bids: levels(&v["b"])?,
            asks: levels(&v["a"])?,
        },
        "aggTrade" => {
            let price = num(&v["p"])?;
            let qty = num(&v["q"])?;
            ensure!(price > 0.0 && qty > 0.0, "invalid trade");
            Kind::Trade {
                symbol: s,
                id: id(&v["a"])?,
                price,
                qty,
                buyer_maker: v["m"].as_bool().context("missing maker flag")?,
            }
        }
        "markPriceUpdate" => Kind::Mark {
            symbol: s,
            price: num(&v["p"])?,
            rate: num(&v["r"])?,
            next_funding_ns: id(&v["T"])? * MS,
        },
        other => anyhow::bail!("unknown stream event {other}"),
    };
    Ok(Some((kind, v["E"].as_u64().map(|x| x * MS))))
}
enum Input {
    Raw(String),
    Ready(Kind, Value),
    Snapshot(usize, u64, Value),
    SnapshotError(usize, String),
    Disconnect(String),
    Timer,
}
struct Queued {
    recv_ns: u64,
    utc_ns: u64,
    received: Instant,
    input: Input,
}
struct Ingress {
    sender: mpsc::Sender<Queued>,
    fatal: watch::Sender<Option<String>>,
    start: Instant,
    anchor: u64,
}
impl Ingress {
    fn send(&self, input: Input) {
        let received = Instant::now();
        let q = Queued {
            recv_ns: self.anchor + received.duration_since(self.start).as_nanos() as u64,
            utc_ns: utc_ns(),
            received,
            input,
        };
        if self.sender.try_send(q).is_err() {
            let _ = self
                .fatal
                .send(Some("ingress queue overflow/closed".into()));
        }
    }
}
type Bus = Arc<Mutex<Ingress>>;
fn send(bus: &Bus, input: Input) {
    bus.lock().unwrap().send(input);
}

async fn socket(url: String, source: &'static str, bus: Bus) {
    let mut backoff = 1;
    loop {
        let result: Result<()> = async {
            let (mut ws, _) = connect_async(&url).await?;
            send(
                &bus,
                Input::Ready(
                    Kind::Connection {
                        source: source.into(),
                        connected: true,
                    },
                    Value::Null,
                ),
            );
            backoff = 1;
            loop {
                let msg = tokio::time::timeout(Duration::from_secs(15), ws.next())
                    .await?
                    .context("socket closed")??;
                match msg {
                    Message::Text(s) => send(&bus, Input::Raw(s.to_string())),
                    Message::Ping(p) => ws.send(Message::Pong(p)).await?,
                    Message::Close(_) => anyhow::bail!("remote close"),
                    _ => (),
                }
            }
        }
        .await;
        send(
            &bus,
            Input::Disconnect(format!("{url}: {}", result.unwrap_err())),
        );
        send(
            &bus,
            Input::Ready(
                Kind::Connection {
                    source: source.into(),
                    connected: false,
                },
                Value::Null,
            ),
        );
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}
async fn metadata(client: &reqwest::Client, c: &Config) -> Result<Vec<(Kind, Value)>> {
    let v: Value = client
        .get(format!("{}/fapi/v1/exchangeInfo", c.rest_url))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    c.symbols
        .iter()
        .map(|s| {
            let row = v["symbols"]
                .as_array()
                .context("missing symbols")?
                .iter()
                .find(|v| v["symbol"] == *s)
                .context("instrument unavailable")?;
            ensure!(
                row["status"] == "TRADING" && row["contractType"] == "PERPETUAL",
                "instrument not active perpetual"
            );
            let filters = row["filters"].as_array().context("missing filters")?;
            let get = |kind: &str| -> Result<&Value> {
                filters
                    .iter()
                    .find(|v| v["filterType"] == kind)
                    .context("missing instrument filter")
            };
            let lot = get("MARKET_LOT_SIZE")?;
            Ok((
                Kind::Metadata {
                    symbol: s.clone(),
                    tick: num(&get("PRICE_FILTER")?["tickSize"])?,
                    step: num(&lot["stepSize"])?,
                    min_qty: num(&lot["minQty"])?,
                    min_notional: num(&get("MIN_NOTIONAL")?["notional"])?,
                },
                row.clone(),
            ))
        })
        .collect()
}
pub async fn run(mut store: LiveStore, seconds: Option<u64>) -> Result<()> {
    let c = store.engine.config.clone();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let initial = metadata(&client, &c).await?;
    let (tx, mut rx) = mpsc::channel(c.queue_capacity);
    let (fatal_tx, mut fatal_rx) = watch::channel(None);
    let start = Instant::now();
    let anchor = utc_ns().max(store.engine.now + 1);
    let session = format!("{anchor}-{}", std::process::id());
    let bus = Arc::new(Mutex::new(Ingress {
        sender: tx,
        fatal: fatal_tx,
        start,
        anchor,
    }));
    send(
        &bus,
        Input::Disconnect("session start; rebuild both books".into()),
    );
    for (k, v) in initial {
        send(&bus, Input::Ready(k, v));
    }
    let depth = c
        .symbols
        .iter()
        .map(|s| format!("{}@depth@100ms", s.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let market = c
        .symbols
        .iter()
        .flat_map(|s| {
            [
                format!("{}@aggTrade", s.to_lowercase()),
                format!("{}@markPrice@1s", s.to_lowercase()),
            ]
        })
        .collect::<Vec<_>>()
        .join("/");
    let tasks = vec![
        tokio::spawn(socket(
            format!("{}?streams={depth}", c.public_ws),
            "public",
            bus.clone(),
        )),
        tokio::spawn(socket(
            format!("{}?streams={market}", c.market_ws),
            "market",
            bus.clone(),
        )),
    ];
    let timer_bus = bus.clone();
    let timer = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            send(&timer_bus, Input::Timer);
        }
    });
    // Funding settlements are fetched after publication and use historical exposure at funding time.
    let fund_bus = bus.clone();
    let fund_client = client.clone();
    let fund_c = c.clone();
    let funding_start = store
        .engine
        .broker
        .position_history
        .iter()
        .flat_map(|x| x.first().map(|p| p.0))
        .min()
        .unwrap_or(anchor)
        / MS;
    let funding = tokio::spawn(async move {
        let mut starts = [funding_start; 2];
        let mut polls = 0_u64;
        loop {
            for (i, s) in fund_c.symbols.iter().enumerate() {
                let result: Result<()> = async {
                    loop {
                        let rows: Value = fund_client
                            .get(format!("{}/fapi/v1/fundingRate", fund_c.rest_url))
                            .query(&[
                                ("symbol", s.clone()),
                                ("startTime", starts[i].to_string()),
                                ("limit", "1000".into()),
                            ])
                            .send()
                            .await?
                            .error_for_status()?
                            .json()
                            .await?;
                        let a = rows.as_array().context("funding response not array")?;
                        for r in a {
                            let t = id(&r["fundingTime"])?;
                            send(
                                &fund_bus,
                                Input::Ready(
                                    Kind::Funding {
                                        symbol: s.clone(),
                                        funding_ns: t * MS,
                                        rate: num(&r["fundingRate"])?,
                                        mark: num(&r["markPrice"])?,
                                    },
                                    r.clone(),
                                ),
                            );
                            starts[i] = starts[i].max(t + 1);
                        }
                        if a.len() < 1000 {
                            break;
                        }
                    }
                    Ok(())
                }
                .await;
                if let Err(e) = result {
                    send(
                        &fund_bus,
                        Input::Disconnect(format!("funding refresh failed: {e}")),
                    );
                }
            }
            polls += 1;
            if polls.is_multiple_of(30) {
                match metadata(&fund_client, &fund_c).await {
                    Ok(rows) => {
                        for (k, v) in rows {
                            send(&fund_bus, Input::Ready(k, v));
                        }
                    }
                    Err(e) => send(
                        &fund_bus,
                        Input::Disconnect(format!("metadata refresh failed: {e}")),
                    ),
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
    let mut pending = [false; 2];
    let mut request_epoch = [0_u64; 2];
    let mut count = 0_u64;
    let mut latency_max = 0_u128;
    let outcome:Result<()>=async {
        loop {
            let q=tokio::select! {
                biased;
                _=fatal_rx.changed()=>anyhow::bail!("{}",fatal_rx.borrow().clone().unwrap_or("ingress stopped".into())),
                _=tokio::signal::ctrl_c()=>break,
                q=rx.recv()=>q.context("ingress closed")?,
            };
            ensure!(q.received.elapsed()<=Duration::from_millis(c.stale_ms),"processing backlog exceeds freshness limit");
            let (kind,raw,exchange_ns)=match q.input {
                Input::Raw(text)=>{
                    let v:Value=serde_json::from_str(&text)?;
                    match decode(&v)? {Some((k,t))=>(k,Some(v),t),None=>continue}
                }
                Input::Ready(k,v)=>(k,Some(v),None),
                Input::Snapshot(i,g,v)=>{
                    if g!=request_epoch[i] {continue;}
                    pending[i]=false;
                    (Kind::Snapshot{symbol:c.symbols[i].clone(),last_id:id(&v["lastUpdateId"])?,bids:levels(&v["bids"])?,asks:levels(&v["asks"])?},Some(v),None)
                }
                Input::SnapshotError(i,reason)=>{pending[i]=false;(Kind::Disconnect{reason},None,None)},
                Input::Disconnect(reason)=>(Kind::Disconnect{reason},None,None),
                Input::Timer=>(Kind::Timer,None,None),
            };
            let disconnect=matches!(kind,Kind::Disconnect{..}|Kind::Connection{connected:false,..});
            let e=Event{version:SCHEMA,session:session.clone(),seq:store.engine.last_seq+1,recv_ns:q.recv_ns,utc_ns:q.utc_ns,exchange_ns,raw,kind};
            store.push(&e)?;
            latency_max=latency_max.max(q.received.elapsed().as_nanos());count+=1;
            if disconnect {for i in 0..2 {pending[i]=false;request_epoch[i]+=1;}}
            for i in 0..2 {
                if !pending[i] && store.engine.books[i].last_id.is_none() && !store.engine.books[i].buffer.is_empty() {
                    pending[i]=true;request_epoch[i]+=1;
                    let generation=request_epoch[i];let snapshot_bus=bus.clone();let snapshot_client=client.clone();
                    let url=format!("{}/fapi/v1/depth?symbol={}&limit=1000",c.rest_url,c.symbols[i]);
                    tokio::spawn(async move {
                        let result:Result<Value>=async {Ok(snapshot_client.get(url).send().await?.error_for_status()?.json().await?)}.await;
                        match result {Ok(v)=>send(&snapshot_bus,Input::Snapshot(i,generation,v)),Err(e)=>send(&snapshot_bus,Input::SnapshotError(i,e.to_string()))}
                    });
                }
            }
            if e.recv_ns>=store.last_checkpoint_ns+SECOND {
                store.checkpoint_background()?;
                eprintln!("{}",json!({"event":"health","events":count,"queue_depth":rx.len(),"latency_max_us":latency_max/1000,"books_valid":store.engine.valid_at(e.recv_ns),"gaps":store.engine.gaps,"equity":store.engine.broker.equity,"exposure_unpriced":store.engine.broker.exposure_unpriced}));
                latency_max=0;
            }
            if seconds.is_some_and(|s|start.elapsed()>=Duration::from_secs(s)) {break;}
        }
        Ok(())
    }.await;
    for task in tasks {
        task.abort();
    }
    timer.abort();
    funding.abort();
    // Preserve exposure; shutting down never invents a fill.
    let now = (anchor + start.elapsed().as_nanos() as u64).max(store.engine.now + 1);
    let e = Event {
        version: SCHEMA,
        session,
        seq: store.engine.last_seq + 1,
        recv_ns: now,
        utc_ns: utc_ns(),
        exchange_ns: None,
        raw: None,
        kind: Kind::Disconnect {
            reason: outcome
                .as_ref()
                .err()
                .map_or("normal shutdown".into(), |e| e.to_string()),
        },
    };
    if outcome.is_ok() {
        store.push(&e)?;
        store.checkpoint()?;
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_ingress_overflow_is_fatal() {
        let (tx, mut rx) = mpsc::channel(1);
        let (fatal, watch) = watch::channel(None);
        let ingress = Ingress {
            sender: tx,
            fatal,
            start: Instant::now(),
            anchor: 1,
        };
        ingress.send(Input::Timer);
        ingress.send(Input::Timer);
        assert!(watch.borrow().as_ref().unwrap().contains("overflow"));
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }
}
