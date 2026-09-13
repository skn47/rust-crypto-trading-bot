"""Chronological ridge fitting. Feature generation belongs exclusively to Rust."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

GAP_NS = 6_000_000_000
ALPHAS = (0.01, 0.1, 1.0, 10.0, 100.0)
BUFFERS = (0.0, 1.0, 2.0, 5.0)


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temp.replace(path)


def convert(rows: Path, output: Path) -> None:
    if output.exists():
        raise ValueError("output exists")
    schema = pa.schema([
        ("symbol", pa.string()), ("recv_ns", pa.uint64()),
        ("label_end_ns", pa.uint64()), ("seq", pa.uint64()),
        ("epoch", pa.uint64()), ("features", pa.list_(pa.float64(), 37)),
        ("mid", pa.float64()), ("spread_bps", pa.float64()),
        ("target_bps", pa.float64()),
    ])
    temp = output.with_suffix(".parquet.tmp")
    with pq.ParquetWriter(temp, schema, compression="zstd") as writer:
        with rows.open() as source:
            batch = []
            for line in source:
                batch.append(json.loads(line))
                if len(batch) == 8192:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
    temp.replace(output)


def load(dataset: Path) -> tuple[dict, dict]:
    manifest = json.loads(dataset.with_suffix(".manifest.json").read_text())
    if manifest["version"] != 1 or len(manifest["features"]) != 37:
        raise ValueError("unsupported dataset contract")
    table = pq.read_table(dataset).to_pydict()
    arrays = {key: np.asarray(value) for key, value in table.items()}
    if not len(arrays["recv_ns"]):
        raise ValueError("empty dataset")
    if not np.isfinite(arrays["features"]).all() or not np.isfinite(arrays["target_bps"]).all():
        raise ValueError("nonfinite training data")
    if np.any(arrays["label_end_ns"] != arrays["recv_ns"] + 1_000_000_000):
        raise ValueError("unexpected label horizon")
    if np.any(arrays["recv_ns"][1:] < arrays["recv_ns"][:-1]):
        raise ValueError("dataset must preserve chronological order")
    return arrays, manifest


def split_masks(t: np.ndarray, end: np.ndarray) -> tuple[dict, dict]:
    unique = np.unique(t)
    b1, b2 = int(unique[int(len(unique) * .6)]), int(unique[int(len(unique) * .8)])
    masks = {
        "train": (t < b1 - GAP_NS) & (end < b1 - GAP_NS),
        "validation": (t >= b1 + GAP_NS) & (t < b2 - GAP_NS) & (end < b2 - GAP_NS),
        "test": t >= b2 + GAP_NS,
    }
    if any(np.count_nonzero(mask) < 20 for mask in masks.values()):
        raise ValueError("insufficient data after chronological purging; collect a longer continuous session")
    return masks, {"boundary_60_ns": b1, "boundary_80_ns": b2, "gap_ns": GAP_NS}


def fit(x: np.ndarray, y: np.ndarray, alpha: float, columns: np.ndarray | None = None) -> dict:
    mean, scale = x.mean(axis=0), x.std(axis=0)
    scale[scale < 1e-12] = 1.0
    columns = np.arange(x.shape[1]) if columns is None else columns
    z = ((x - mean) / scale)[:, columns]
    intercept = float(y.mean())
    weights = np.zeros(x.shape[1])
    weights[columns] = np.linalg.solve(z.T @ z + alpha * np.eye(len(columns)), z.T @ (y - intercept))
    return {"mean": mean.tolist(), "scale": scale.tolist(), "coefficients": weights.tolist(), "intercept": intercept, "alpha": alpha}


def predict(model: dict, x: np.ndarray) -> np.ndarray:
    return (x - np.asarray(model["mean"])) / np.asarray(model["scale"]) @ np.asarray(model["coefficients"]) + model["intercept"]


def metrics(y: np.ndarray, pred: np.ndarray) -> dict:
    corr = float(np.corrcoef(y, pred)[0, 1]) if np.std(y) > 1e-12 and np.std(pred) > 1e-12 else None
    return {"rows": len(y), "rmse_bps": float(np.sqrt(np.mean((y - pred) ** 2))),
            "mae_bps": float(np.mean(np.abs(y - pred))), "correlation": corr}


def choose_buffer(y: np.ndarray, pred: np.ndarray, spread: np.ndarray, fee: float) -> tuple[float, list]:
    # Validation-only proxy; execution-quality evidence comes from Rust replay.
    scores = []
    for buffer in BUFFERS:
        take = np.abs(pred) > spread + 2 * fee + buffer
        net = np.where(take, np.sign(pred) * y - spread - 2 * fee, 0.0)
        scores.append({"buffer_bps": buffer, "mean_net_bps_per_opportunity": float(net.mean()), "opportunities": int(take.sum())})
    winner = max(scores, key=lambda row: (row["mean_net_bps_per_opportunity"], row["buffer_bps"]))
    return winner["buffer_bps"], scores


def train(dataset: Path, output: Path) -> dict:
    if output.exists() and any(output.iterdir()):
        raise ValueError("training output must be empty; preserve prior experiments")
    d, manifest = load(dataset)
    masks, boundaries = split_masks(d["recv_ns"], d["label_end_ns"])
    data_hash = digest(dataset)
    fee = float(manifest["config"]["fee_bps"])
    report = {"dataset_sha256": data_hash, "splits": boundaries, "symbols": {}, "model_sha256": {},
              "test_start_ns": int(d["recv_ns"][masks["test"]].min()),
              "test_end_ns": int(d["label_end_ns"][masks["test"]].max()),
              "buffer_selection": "validation mid-return proxy after spread and assumed fees; overlapping opportunities are not an executable P&L estimate"}
    for i, symbol in enumerate(("BTCUSDT", "ETHUSDT")):
        sm = {name: mask & (d["symbol"] == symbol) for name, mask in masks.items()}
        if any(mask.sum() < 10 for mask in sm.values()):
            raise ValueError(f"insufficient {symbol} rows")
        x, y = d["features"], d["target_bps"]
        result = {}
        for name, columns in (("cross_asset", None), ("own_asset", np.arange(i * 17, (i + 1) * 17))):
            candidates = [fit(x[sm["train"]], y[sm["train"]], a, columns) for a in ALPHAS]
            m = min(candidates, key=lambda m: float(np.mean((predict(m, x[sm["validation"]]) - y[sm["validation"]]) ** 2)))
            val_pred = predict(m, x[sm["validation"]])
            buffer, scores = choose_buffer(y[sm["validation"]], val_pred, d["spread_bps"][sm["validation"]], fee)
            m.update(version=1, symbol=symbol, features=manifest["features"], horizon_ms=1000,
                     trained_through_ns=int(d["label_end_ns"][sm["train"]].max()),
                     selected_through_ns=int(d["label_end_ns"][sm["validation"]].max()),
                     dataset_sha256=data_hash, entry_buffer_bps=buffer)
            filename = f"{symbol}.json" if name == "cross_asset" else f"{symbol}.own.json"
            write_json(output / filename, m)
            report["model_sha256"][filename] = digest(output / filename)
            result[name] = {"validation": metrics(y[sm["validation"]], val_pred), "alpha": m["alpha"], "buffer_scores": scores}
        result["zero_return"] = {"validation": metrics(y[sm["validation"]], np.zeros(sm["validation"].sum()))}
        result["rows"] = {name: int(mask.sum()) for name, mask in sm.items()}
        report["symbols"][symbol] = result
    # Test predictions are deliberately computed only by the separate evaluate command.
    write_json(output / "training.json", report)
    return report


def evaluate(dataset: Path, models: Path, output: Path, events: Path | None = None,
             engine: Path | None = None, config: Path | None = None) -> dict:
    d, manifest = load(dataset)
    training = json.loads((models / "training.json").read_text())
    if training["dataset_sha256"] != digest(dataset):
        raise ValueError("evaluation dataset differs from frozen experiment")
    masks, boundaries = split_masks(d["recv_ns"], d["label_end_ns"])
    if boundaries != training["splits"]:
        raise ValueError("split mismatch")
    report = {"dataset_sha256": training["dataset_sha256"], "symbols": {}, "simulation": "not requested"}
    for symbol in ("BTCUSDT", "ETHUSDT"):
        mask = masks["test"] & (d["symbol"] == symbol)
        result = {}
        for name, suffix in (("cross_asset", ""), ("own_asset", ".own")):
            filename = f"{symbol}{suffix}.json"
            if digest(models / filename) != training["model_sha256"][filename]:
                raise ValueError("model changed after experiment was frozen")
            model = json.loads((models / filename).read_text())
            if model["dataset_sha256"] != training["dataset_sha256"] or model["features"] != manifest["features"]:
                raise ValueError("model provenance/schema mismatch")
            if model["selected_through_ns"] >= int(d["recv_ns"][mask].min()):
                raise ValueError("model selection overlaps test period")
            result[name] = metrics(d["target_bps"][mask], predict(model, d["features"][mask]))
        result["zero_return"] = metrics(d["target_bps"][mask], np.zeros(mask.sum()))
        report["symbols"][symbol] = result
    if events is not None:
        if engine is None or config is None:
            raise ValueError("execution evaluation needs --engine and --config")
        if digest(events) != manifest["source_sha256"]:
            raise ValueError("event log differs from dataset source")
        output.parent.mkdir(parents=True, exist_ok=True)
        # Append model_paths to a copied config after removing its original declaration.
        lines = [line for line in config.read_text().splitlines() if not line.strip().startswith("model_paths")]
        paths = [str((models / f"{s}.json").resolve()) for s in ("BTCUSDT", "ETHUSDT")]
        replay_config = output.with_suffix(".replay.toml")
        replay_config.write_text("\n".join(lines) + "\nmodel_paths = " + json.dumps(paths) + "\n")
        replay_report = output.with_suffix(".execution.json")
        subprocess.run([str(engine.resolve()), "--config", str(replay_config), "replay", "--input", str(events),
                        "--journal", str(output.with_suffix(".journal.jsonl")), "--report", str(replay_report),
                        "--start-ns", str(training["test_start_ns"]), "--end-ns", str(training["test_end_ns"])], check=True)
        report["simulation"] = json.loads(replay_report.read_text())
    write_json(output, report)
    return report


def train_cli() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(train(args.dataset, args.output), indent=2))


def evaluate_cli() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--models", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--events", type=Path)
    p.add_argument("--engine", type=Path)
    p.add_argument("--config", type=Path)
    args = p.parse_args()
    print(json.dumps(evaluate(args.dataset, args.models, args.output, args.events, args.engine, args.config), indent=2))


if __name__ == "__main__":
    import sys
    command = sys.argv.pop(1)
    if command == "train":
        train_cli()
    elif command == "evaluate":
        evaluate_cli()
    elif command == "parquet":
        p = argparse.ArgumentParser()
        p.add_argument("--rows", type=Path, required=True)
        p.add_argument("--output", type=Path, required=True)
        args = p.parse_args()
        convert(args.rows, args.output)
    else:
        raise SystemExit(f"unknown command: {command}")
