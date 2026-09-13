import json
from pathlib import Path
import subprocess

import numpy as np
import pytest

from research.pipeline import evaluate, fit, load, predict, split_masks, train

ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "target/debug/microengine"


def run(*args):
    return subprocess.run([str(ENGINE), *map(str, args)], cwd=ROOT, text=True, capture_output=True, check=True)


@pytest.fixture(scope="module")
def experiment(tmp_path_factory):
    assert ENGINE.exists(), "run cargo build before pytest"
    folder = tmp_path_factory.mktemp("experiment")
    events = folder / "events.jsonl"
    dataset = folder / "dataset.parquet"
    models = folder / "models"
    run("fixture", "--output", events, "--seconds", 180)
    run("dataset", "--input", events, "--output", dataset)
    report = train(dataset, models)
    return folder, events, dataset, models, report


def test_purged_partitions_and_train_only_preprocessing(experiment):
    _, _, dataset, models, _ = experiment
    d, _ = load(dataset)
    masks, _ = split_masks(d["recv_ns"], d["label_end_ns"])
    assert d["label_end_ns"][masks["train"]].max() < d["recv_ns"][masks["validation"]].min()
    assert d["label_end_ns"][masks["validation"]].max() < d["recv_ns"][masks["test"]].min()
    model = json.loads((models / "BTCUSDT.json").read_text())
    mask = masks["train"] & (d["symbol"] == "BTCUSDT")
    np.testing.assert_allclose(model["mean"], d["features"][mask].mean(axis=0), rtol=0, atol=0)
    x = d["features"].copy()
    x[masks["test"]] = 1e12
    refit = fit(x[mask], d["target_bps"][mask], model["alpha"])
    np.testing.assert_allclose(refit["coefficients"], model["coefficients"], rtol=0, atol=0)


def test_rust_python_prediction_parity(experiment):
    folder, _, dataset, models, _ = experiment
    d, _ = load(dataset)
    x = d["features"][::37]
    inputs = folder / "vectors.jsonl"
    inputs.write_text("".join(json.dumps(row.tolist()) + "\n" for row in x))
    for symbol in ("BTCUSDT", "ETHUSDT"):
        path = models / f"{symbol}.json"
        rust = np.asarray([float(v) for v in run("score", "--model", path, "--input", inputs).stdout.splitlines()])
        python = predict(json.loads(path.read_text()), x)
        np.testing.assert_allclose(rust, python, rtol=1e-10, atol=1e-10)


def test_frozen_test_evaluation_and_execution(experiment):
    folder, events, dataset, models, _ = experiment
    report = evaluate(dataset, models, folder / "test.json", events, ENGINE, ROOT / "config/default.toml")
    assert report["simulation"]["samples"] > 0
    for symbol in ("BTCUSDT", "ETHUSDT"):
        assert report["symbols"][symbol]["cross_asset"]["rows"] > 10
    # Loading trained models for the earlier training period must fail explicitly.
    cfg = folder / "test.replay.toml"
    with pytest.raises(subprocess.CalledProcessError, match="returned non-zero"):
        run("--config", cfg, "replay", "--input", events, "--journal", folder / "invalid.jsonl", "--report", folder / "invalid.json")


def test_dataset_features_and_labels_are_prefix_invariant(experiment):
    folder, events, dataset, _, _ = experiment
    original = events.read_text().splitlines()
    prefix = folder / "prefix.jsonl"
    prefix.write_text("\n".join(original[:len(original)//2]) + "\n")
    partial = folder / "prefix.parquet"
    run("dataset", "--input", prefix, "--output", partial)
    a, _ = load(partial)
    b, _ = load(dataset)
    for name in a:
        np.testing.assert_array_equal(a[name], b[name][:len(a[name])])


def test_tampered_model_is_rejected(experiment, tmp_path):
    _, _, dataset, models, _ = experiment
    import shutil
    copy = tmp_path / "models"
    shutil.copytree(models, copy)
    path = copy / "BTCUSDT.json"
    m = json.loads(path.read_text())
    m["intercept"] += 1
    path.write_text(json.dumps(m))
    with pytest.raises(ValueError, match="changed"):
        evaluate(dataset, copy, tmp_path / "bad.json")


def test_small_dataset_fails_after_purging():
    t = np.arange(30, dtype=np.uint64) * 100_000_000
    with pytest.raises(ValueError, match="insufficient"):
        split_masks(t, t + 1_000_000_000)
