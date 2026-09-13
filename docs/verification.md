# Verification

Verified in the implementation environment on 2026-09-05.

## Automated checks

- 16 Rust tests passed: two unit tests, 13 core/integration tests, and one eight-second localhost exchange test.
- Six Python tests passed, including the synthetic dataset/train/test/replay workflow and Rust/Python prediction agreement at `1e-10` tolerance.
- Formatting, Clippy with warnings denied, and debug/release builds passed.
- Coverage includes receive-order causality, prefix invariance, inter-timer book observations, model cutoffs, training-only normalization, boundary purging, snapshot alignment, gaps, stale/disconnected feeds, duplicates, partial fills, funding, risk stops, disk failure, queue overflow, exclusive writers, and recovery from a partially written log tail.
- Restart recovery uses an asynchronous checkpoint and requires exact final-state and journal-byte equality against uninterrupted replay.

The localhost exchange test serves REST metadata/snapshots and both WebSocket routes, injects a depth-sequence gap, and requires recovery, feature warmup, sample emission, and successful replay.

## Latency

The final release benchmark ran on an Intel Core i5-1240P, Linux x86_64, with 16 visible logical CPUs. It processed 10,000 synthetic events at 1,000 offered events/second, including 1,948 decision ticks, with no dropped events.

| Measurement | Result |
| --- | ---: |
| All-event p50 | 132.216 µs |
| All-event p99 | 446.797 µs |
| All-event p99.9 | 1,528.120 µs |
| Decision-event p99 | 314.652 µs |
| Observed throughput | 999.891 events/s |

Both p99 measurements pass the 1 ms target for this workload. The p99.9 tail exceeds 1 ms; this is not a hard real-time guarantee.

The workload uses two ten-level synthetic books, 37 features, two constant-output linear models, zero benchmark fees to exercise trading, delayed execution, exchange JSON normalization, event/journal writes, and asynchronous durable checkpoints. The synthetic clock advances faster than wall time. Measurements start before enqueueing and end after processing/journaling. Network transit and WebSocket framing are excluded. Real depth sizes, feed bursts, storage, and host scheduling require deployment-specific measurements.

The machine-readable result is in [benchmark.json](benchmark.json). Reproduce it with the [README](../README.md) command.

## Synthetic experiment

The 180-second deterministic fixture produced 3,480 valid labeled rows. Training and separate frozen test evaluation completed with 288 test rows per asset. Its smooth, predictable prices intentionally exercise the model; scores and simulated P&L are software checks, not market-edge evidence.

Generated local artifacts are in `data/verification/` and `artifacts/verification/`: Parquet data, frozen models, validation and test reports, execution report, and journal. Generated outputs are ignored by version control and reproducible from the README.

## Live connectivity

A public Binance recording attempt reached `/fapi/v1/exchangeInfo` but received **HTTP 451 Unavailable For Legal Reasons**. No live dataset was collected. The adapter passed the localhost simulator test; actual Binance end-to-end stream operation remains unverified from this environment.

No authenticated trading or real-money orders were attempted. Profitability and live execution quality have not been established.
