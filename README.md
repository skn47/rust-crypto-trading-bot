# Microengine

A BTCUSDT/ETHUSDT cross-asset microstructure engine for Binance USD-M perpetuals. Rust owns collection, order books, features, inference, replay, and paper execution. Python fits and evaluates one standardized ridge model per asset.

This release records public data and simulates directional taker orders. It has no authenticated endpoints, API-key handling, or real-order submission. The included synthetic experiment verifies the pipeline; its results are not market performance evidence.

## Build and verify

Run commands from the repository root. Use a current stable Rust toolchain, Python 3.11+, and `uv`. Dependency versions are recorded in `Cargo.lock` and `uv.lock`.

```bash
uv sync --locked --group dev
cargo build --locked --release
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --locked
.venv/bin/python -m pytest -q
```

The Rust recorder integration test opens localhost HTTP/WebSocket servers for eight seconds. All automated tests work without an exchange account or internet access once dependencies are installed. Restricted sandboxes may require permission to bind localhost sockets.

## Reproducible synthetic walkthrough

Use fresh output paths; datasets, training directories, and journals are protected against accidental reuse.

```bash
target/release/microengine fixture --output data/example/events.jsonl --seconds 180
target/release/microengine dataset --input data/example/events.jsonl --output data/example/dataset.parquet
.venv/bin/micro-train --dataset data/example/dataset.parquet --output artifacts/example/models
.venv/bin/micro-evaluate --dataset data/example/dataset.parquet --models artifacts/example/models --output artifacts/example/evaluation.json --events data/example/events.jsonl --engine target/release/microengine --config config/default.toml
```

Evaluation produces per-asset test errors for cross-asset, own-asset, and zero-return models, plus the Rust execution report and order journal. The replay helper warms books and features using the earlier log, then enables models only at the held-out test start. Outstanding positions at the end remain marked; the engine does not fabricate closing fills.

To reproduce the host latency measurement:

```bash
target/release/microengine benchmark --events 10000 --rate 1000 --output artifacts/benchmark.json
```

The report states CPU/OS, build type, offered rate, throughput, all-event p50/p99/p99.9, decision-event p99, and whether both p99 values are below 1 ms. It measures an ingestion queue, JSON parsing, exchange normalization, write-ahead recording, features, inference, risk, and paper journaling. Background checkpoint work can affect queue latency. Network transport and WebSocket framing are excluded. Run on an otherwise idle target host; the result is specific to that host and workload. The synthetic clock advances faster than wall time and exercises frequent checkpoints.

## Collect and paper trade

```bash
target/release/microengine record --log data/live/events.jsonl --seconds 600
```

Omit `--seconds` to collect until Ctrl-C. The collector uses public feeds and REST metadata/snapshots. After collecting sufficient continuous data, run `dataset`, `micro-train`, and `micro-evaluate` as above. Short recordings may fail the chronological split requirements; multi-day, varied-market recordings are needed for meaningful strategy evaluation.

Copy `config/default.toml` to a new configuration and set:

```toml
model_paths = ["artifacts/example/models/BTCUSDT.json", "artifacts/example/models/ETHUSDT.json"]
```

Then start paper trading with a fresh log:

```bash
target/release/microengine --config config/paper.toml paper --log data/paper/events.jsonl
target/release/microengine --config config/paper.toml paper --log data/paper/events.jsonl --resume
```

The first command runs until interrupted; the second is the restart command. Resume requires identical configuration and model artifacts. Use models trained on collected market data for any market experiment; the walkthrough's synthetic models are for demonstration only.

Raw recording also supports `--resume`. Each log has sibling `.journal.jsonl` and `.checkpoint.json` files. An exclusive file lock prevents concurrent writers. Recovery restores the checkpoint, truncates the journal to its committed offset, and replays complete event records after that checkpoint. An incomplete final log line is discarded. Every new connection session rebuilds both books and warms features again while preserving paper exposure.

## Causality and model contract

- Every event has a global sequence, session ID, monotonic receive time anchored to UTC, separate wall-clock UTC time, exchange timestamp where available, and the raw payload. A serialized ingress queue establishes availability order across sockets, snapshots, and timers. Exchange timestamps never reorder features.
- A snapshot cannot affect state before its receive time. Buffered depth updates are reconciled at snapshot availability; `U/u/pu` enforce continuity. Duplicates cannot refresh a stale book or replenish consumed paper liquidity.
- Both feeds and both books must be healthy. Stale books (500 ms by default), gaps, disconnects, or invalid books suspend entries and reset feature history. Five seconds of fresh history are required after recovery. An ingress overflow or processing backlog beyond the freshness limit terminates collection; write failures stop decisions.
- Features are sampled on recorded 100 ms timer events, using only events already processed. Each asset contributes spread, microprice displacement, 1/5/10-level imbalance, and trailing 100 ms/1 s/5 s order flow, signed trade flow, log return, and realized volatility. Three BTC-minus-ETH return differences complete the 37-element vector. Order flow is normalized by contemporaneous best-level quantity; trade flow is signed quantity divided by total quantity.
- Labels use `10,000 * log(mid(t+1s) / mid(t))` on the receive-time book. No interpolation, future backfill, or exchange-time joins are used. Labels spanning a reset, stale endpoint, or incomplete recording are dropped. Parquet generation streams rows through a temporary JSONL file; Python does not duplicate feature logic.
- Chronological boundaries use unique sample times at 60% and 80%. Exclude six seconds on each side, remove labels reaching a boundary, and require minimum usable rows per partition and asset. Means, scales, and weights use training rows only. Ridge alpha is selected from `0.01, 0.1, 1, 10, 100` by validation MSE.
- Entry buffer selection uses a validation-only mid-return proxy with spread and assumed fees, selecting from `0, 1, 2, 5` bps. This overlapping-opportunity proxy does **not** model execution or represent attainable P&L; use the separate Rust replay results for execution assessment.
- Test results are calculated only by `micro-evaluate`, after artifacts are frozen. Model files include feature names/order, horizon, normalization, coefficients, training and selection cutoffs, and dataset hash. The training manifest also hashes the frozen model files. Runtime rejects incompatible/nonfinite models and decisions at or before selection completion.

These invariants are tested for this pipeline. They do not establish that arbitrary imported datasets, manually edited artifacts, or repeated human tuning against the test set are leakage-free.

## Paper execution and risk

| Setting | Default |
| --- | ---: |
| Initial equity | 10,000 USDT |
| Entry notional | 100 USDT |
| Gross exposure including pending entries | 1,000 USDT |
| UTC-day loss stop | 100 USDT |
| Assumed taker fee, each side | 5 bps |
| Order arrival latency | 50 ms |
| Holding period from entry fill | 1 second |

All monetary and execution assumptions are configurable. The engine limits gross exposure to configured equity, permits one position and one pending order per asset, and reserves capacity for pending entries. Entry predictions must exceed estimated round-trip fees and depth-walking cost plus the selected buffer. Quantities are rounded down to the market step and checked against minimum quantity/notional; risk is checked again at fill time.

An order fills only on a valid depth update at or after its simulated arrival, at displayed prices and available quantities. Unfilled entry quantity is canceled. Exit remainders stay pending and retry after another latency interval. Consumed liquidity stays depleted until that price level is updated. The model cannot anticipate the book used for a future fill.

Fees, funding, gross realized P&L, marked unrealized P&L, turnover, slippage versus decision mid, and rejections are reported. `per_symbol` arrays are ordered BTCUSDT, ETHUSDT. Cash equals starting equity plus realized P&L and funding minus fees. The daily loss stop cancels entries and requests flattening, resetting on the next UTC day. Invalid data never triggers an invented fill; unpriced exposure is flagged and retained.

Funding settlements come from historical funding records polled every minute, using position history at the settlement time and deduplicating payments. Clock synchronization is an operational assumption for funding attribution. Mark-price messages are recorded; book mids value current paper positions. Checkpoints run one at a time in the background and are synced atomically; shutdown waits for a final checkpoint. The event log is written before each decision but fsynced with checkpoints, so power loss may discard the uncommitted tail.

## Operations and limits

Health JSON on stderr reports event count, ingress depth, interval maximum processing latency, book validity, gaps/resynchronization requests, equity, and unpriced exposure. The journal records predictions, submissions, fills, rejections, cancellations, and settlements. Watch for stale books, repeated snapshots, checkpoint/write failures, growing queues, and exposure left open on shutdown.

The collector uses the documented [public depth streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public), [market streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market), and [snapshot reconciliation](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly). Depth publication cadence and network transit are separate from local processing latency.

This is displayed-book simulation: public data cannot establish hidden liquidity, actual queue competition, market impact, exchange matching latency, or counterfactual liquidity after our simulated orders. No liquidation engine, live execution adapter, UI, automatic model replacement, or automatic data retention is included. Training loads the exported matrix into memory. Logs and position history need operator-managed storage for long-running experiments.

See [verification notes](docs/verification.md) for measured results and environment limitations.
