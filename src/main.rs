use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use microengine::{
    config::Config,
    engine::Engine,
    features, io,
    model::Model,
    recorder,
    types::{Event, Kind, MS, SCHEMA},
};
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Parser)]
#[command(
    version,
    about = "Causal BTC/ETH microstructure research and paper engine"
)]
struct Cli {
    #[arg(long, default_value = "config/default.toml", global = true)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Record {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        seconds: Option<u64>,
        #[arg(long)]
        resume: bool,
    },
    Paper {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        seconds: Option<u64>,
        #[arg(long)]
        resume: bool,
    },
    Dataset {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = ".venv/bin/python")]
        python: String,
    },
    Replay {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        journal: PathBuf,
        #[arg(long)]
        report: PathBuf,
        #[arg(long)]
        start_ns: Option<u64>,
        #[arg(long)]
        end_ns: Option<u64>,
    },
    Benchmark {
        #[arg(long, default_value_t = 10000)]
        events: usize,
        #[arg(long, default_value_t = 1000)]
        rate: u64,
        #[arg(long)]
        output: PathBuf,
    },
    Fixture {
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 180)]
        seconds: u64,
    },
    Score {
        #[arg(long)]
        model: String,
        #[arg(long)]
        input: PathBuf,
    },
}
fn models(c: &Config) -> Result<Vec<Model>> {
    c.model_paths.iter().map(|p| Model::load(p)).collect()
}

fn fixture(symbols: &[String], seconds: u64) -> Vec<Event> {
    let mut out = Vec::new();
    let base = 1_700_000_000_000_000_000;
    let mut add = |t: u64, kind: Kind| {
        out.push(Event {
            version: SCHEMA,
            session: "synthetic-v1".into(),
            seq: out.len() as u64 + 1,
            recv_ns: base + t,
            utc_ns: base + t,
            exchange_ns: Some(base + t),
            raw: None,
            kind,
        });
    };
    for source in ["public", "market"] {
        add(
            0,
            Kind::Connection {
                source: source.into(),
                connected: true,
            },
        );
    }
    for (i, s) in symbols.iter().enumerate() {
        let p = if i == 0 { 60000.0 } else { 3000.0 };
        add(
            0,
            Kind::Metadata {
                symbol: s.to_string(),
                tick: 0.1,
                step: 0.001,
                min_qty: 0.001,
                min_notional: 5.0,
            },
        );
        add(
            0,
            Kind::Snapshot {
                symbol: s.to_string(),
                last_id: 1,
                bids: vec![[p - 0.1, 2.0]],
                asks: vec![[p + 0.1, 2.0]],
            },
        );
    }
    let mut old = [60000.0, 3000.0];
    for frame in 0..=seconds * 10 {
        let t = frame * 100 * MS;
        for (i, s) in symbols.iter().enumerate() {
            let center = if i == 0 { 60000.0 } else { 3000.0 };
            let wave = ((frame as f64 / 17.0 - i as f64 * 0.2).sin()
                * if i == 0 { 200.0 } else { 12.0 })
                + (frame as f64 / 71.0).sin() * 3.0;
            let p = ((center + wave) * 10.0).round() / 10.0;
            let mut bids = Vec::new();
            let mut asks = Vec::new();
            for k in 1..=10 {
                bids.push([old[i] - k as f64 * 0.1, 0.0]);
                asks.push([old[i] + k as f64 * 0.1, 0.0]);
            }
            for k in 1..=10 {
                let tilt = (frame as f64 / 17.0 - i as f64 * 0.2).cos();
                bids.push([p - k as f64 * 0.1, 2.0 + tilt]);
                asks.push([p + k as f64 * 0.1, 2.0 - tilt]);
            }
            add(
                t,
                Kind::Depth {
                    symbol: s.to_string(),
                    first_id: if frame == 0 { 1 } else { frame + 2 },
                    last_id: frame + 2,
                    prev_id: frame + 1,
                    bids,
                    asks,
                },
            );
            add(
                t,
                Kind::Trade {
                    symbol: s.to_string(),
                    id: frame + 1,
                    price: p,
                    qty: 0.01,
                    buyer_maker: p < old[i],
                },
            );
            old[i] = p;
        }
        add(t, Kind::Timer);
    }
    out
}
fn bench(c: Config, n: usize, rate: u64, output: &Path) -> Result<()> {
    ensure!(
        n >= 1000 && rate > 0,
        "benchmark requires >=1000 events and positive rate"
    );
    let mut c = c;
    c.fee_bps = 0.0;
    let m: Vec<_> = c
        .symbols
        .iter()
        .map(|s| Model {
            version: 1,
            symbol: s.clone(),
            features: features::names(&c.symbols),
            mean: vec![0.0; 37],
            scale: vec![1.0; 37],
            coefficients: vec![0.0; 37],
            intercept: 20.0,
            horizon_ms: 1000,
            trained_through_ns: 0,
            selected_through_ns: 0,
            dataset_sha256: "0".repeat(64),
            entry_buffer_bps: 0.0,
            alpha: 1.0,
        })
        .collect();
    let symbols = c.symbols.clone();
    let bench_dir = std::env::temp_dir().join(format!("microengine-bench-{}", recorder::utc_ns()));
    let mut store = io::LiveStore::open(&bench_dir.join("events.jsonl"), c, m, false)?;
    let data=fixture(&symbols, n as u64/50+2).into_iter().take(n).map(|mut e| {
        let symbol=e.kind.symbol().map(str::to_owned);
        e.raw=match &e.kind {
            Kind::Depth{first_id,last_id,prev_id,bids,asks,..}=>Some(serde_json::json!({"e":"depthUpdate","s":symbol,"U":first_id,"u":last_id,"pu":prev_id,"b":bids.iter().map(|x|[x[0].to_string(),x[1].to_string()]).collect::<Vec<_>>(),"a":asks.iter().map(|x|[x[0].to_string(),x[1].to_string()]).collect::<Vec<_>>()})),
            Kind::Trade{id,price,qty,buyer_maker,..}=>Some(serde_json::json!({"e":"aggTrade","s":symbol,"a":id,"p":price.to_string(),"q":qty.to_string(),"m":buyer_maker})),
            _=>None,
        };
        serde_json::to_vec(&e)
    }).collect::<std::result::Result<Vec<_>,_>>()?;
    let (tx, rx) = std::sync::mpsc::sync_channel(8192);
    let producer = std::thread::spawn(move || {
        let start = Instant::now();
        for (i, b) in data.into_iter().enumerate() {
            let deadline =
                start + Duration::from_nanos((i as u128 * 1_000_000_000 / rate as u128) as u64);
            if let Some(wait) = deadline.checked_duration_since(Instant::now()) {
                std::thread::sleep(wait);
            }
            if tx.send((Instant::now(), b)).is_err() {
                break;
            }
        }
    });
    let start = Instant::now();
    let mut times = Vec::new();
    let mut decision_times = Vec::new();
    let mut decisions = 0;
    for (received, b) in rx {
        let mut e: Event = serde_json::from_slice(&b)?;
        if let Some(raw) = &e.raw {
            e.kind = recorder::decode(raw)?.unwrap().0;
        }
        let previous = store.engine.last_sample_ns;
        store.push(&e)?;
        let is_decision = store.engine.last_sample_ns != previous
            && store
                .engine
                .healthy_since
                .is_some_and(|t| e.recv_ns >= t + 5000 * MS);
        decisions += usize::from(is_decision);
        let elapsed = received.elapsed().as_nanos() as u64;
        times.push(elapsed);
        if is_decision {
            decision_times.push(elapsed);
        }
        if e.recv_ns >= store.last_checkpoint_ns + 1_000_000_000 {
            store.checkpoint_background()?;
        }
    }
    producer
        .join()
        .map_err(|_| anyhow::anyhow!("benchmark producer failed"))?;
    let elapsed = start.elapsed().as_secs_f64();
    store.checkpoint()?;
    times.sort_unstable();
    decision_times.sort_unstable();
    let decision_p99 =
        decision_times[((decision_times.len() - 1) as f64 * 0.99).ceil() as usize] as f64 / 1000.0;
    let pct = |p: f64| times[((times.len() - 1) as f64 * p).ceil() as usize] as f64 / 1000.0;
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find(|s| s.starts_with("model name"))
        .unwrap_or("unknown")
        .to_string();
    let report = serde_json::json!({"synthetic":true,"release_build":!cfg!(debug_assertions),"cpu":cpu,"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"logical_cpus":std::thread::available_parallelism()?.get(),"events":times.len(),"decisions":decisions,"offered_events_per_second":rate,"throughput":times.len() as f64/elapsed,"p50_us":pct(0.5),"p99_us":pct(0.99),"p999_us":pct(0.999),"decision_p99_us":decision_p99,"dropped_events":0,"target_pass":pct(0.99)<1000.0 && decision_p99<1000.0,"measurement":"bounded ingestion queue + JSON parsing + exchange normalization + WAL write + shared core + paper journal; asynchronous checkpoints can affect following queue delay; excludes network and websocket framing"});
    drop(store);
    std::fs::remove_dir_all(bench_dir)?;
    io::atomic_json(output, &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let config = Config::load(&args.config)?;
    match args.command {
        Command::Record {
            log,
            seconds,
            resume,
        } => recorder::run(io::LiveStore::open(&log, config, vec![], resume)?, seconds).await?,
        Command::Paper {
            log,
            seconds,
            resume,
        } => {
            let m = models(&config)?;
            ensure!(
                m.len() == 2,
                "paper requires one validated model per symbol in model_paths"
            );
            recorder::run(io::LiveStore::open(&log, config, m, resume)?, seconds).await?;
        }
        Command::Dataset {
            input,
            output,
            python,
        } => {
            println!("{} rows", io::dataset(&input, &output, config, &python)?);
        }
        Command::Replay {
            input,
            journal,
            report,
            start_ns,
            end_ns,
        } => {
            let m = models(&config)?;
            if let Some(start) = start_ns {
                ensure!(
                    m.iter().all(|m| start > m.selected_through_ns),
                    "evaluation starts before model selection finishes"
                );
            }
            let mut engine = Engine::new(config, vec![])?;
            let mut writer = io::create(&journal)?;
            let mut samples = 0;
            for e in io::events(&input)? {
                let e = e?;
                if end_ns.is_some_and(|end| e.recv_ns > end) {
                    break;
                }
                let evaluating = start_ns.is_none_or(|start| e.recv_ns >= start);
                if evaluating && engine.models.is_empty() {
                    engine.models = m.clone();
                }
                let (s, rows) = engine.on_event(&e)?;
                if evaluating {
                    samples += usize::from(s.is_some());
                    for r in rows {
                        io::json_line(&mut writer, &r)?;
                    }
                }
            }
            writer.flush()?;
            io::atomic_json(
                &report,
                &serde_json::json!({"samples":samples,"events":engine.last_seq,"gaps":engine.gaps,"broker":engine.broker,"limitations":"displayed-book approximation; outstanding positions remain marked, no forced EOF fills"}),
            )?;
        }
        Command::Fixture { output, seconds } => {
            ensure!(seconds <= 86400, "fixture capped at one day");
            let mut f = io::create(&output)?;
            for e in fixture(&config.symbols, seconds) {
                io::json_line(&mut f, &e)?;
            }
            f.sync_all()?;
        }
        Command::Benchmark {
            events,
            rate,
            output,
        } => bench(config, events, rate, &output)?,
        Command::Score { model, input } => {
            let m = Model::load(&model)?;
            m.validate(&m.asset_symbols())?;
            for line in BufReader::new(File::open(input)?).lines() {
                let x: Vec<f64> = serde_json::from_str(&line?)?;
                ensure!(
                    x.len() == m.features.len() && x.iter().all(|v| v.is_finite()),
                    "invalid feature vector"
                );
                println!("{}", m.predict(&x));
            }
        }
    }
    Ok(())
}
