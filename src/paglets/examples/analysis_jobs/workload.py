# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import io
import sqlite3
import time
from pathlib import Path

import pandas as pd
from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from .locking import CrossProcessFileLock, default_lock_path


def download_data(*, job_id: str, seed: int, row_count: int, feature_count: int) -> pd.DataFrame:
    _ = job_id
    informative = max(2, min(feature_count, feature_count // 2))
    x, y = make_classification(
        n_samples=max(100, row_count),
        n_features=max(4, feature_count),
        n_informative=informative,
        n_redundant=max(0, min(feature_count - informative, feature_count // 5)),
        n_classes=3,
        random_state=seed,
    )
    columns = [f"feature_{index:03d}" for index in range(x.shape[1])]
    frame = pd.DataFrame(x, columns=columns)
    frame["target"] = y
    return frame


def process_data_to_frames(
    data: pd.DataFrame,
    *,
    job_id: str,
    host_name: str,
    seed: int,
    target_runtime_seconds: float,
    estimator_trees: int,
    cpu_core_ids: list[int] | None = None,
    cpu_affinity_supported: bool = False,
    cpu_affinity_enforced: bool = False,
    cpu_affinity_error: str = "",
) -> dict[str, pd.DataFrame]:
    started = time.perf_counter()
    feature_columns = [column for column in data.columns if column != "target"]
    train_x, test_x, train_y, test_y = train_test_split(
        data[feature_columns],
        data["target"],
        test_size=0.25,
        random_state=seed,
        stratify=data["target"],
    )
    model = RandomForestClassifier(
        n_estimators=max(1, estimator_trees),
        random_state=seed,
        n_jobs=1,
        max_depth=None,
    )
    model.fit(train_x, train_y)
    predicted = model.predict(test_x)
    score = float(accuracy_score(test_y, predicted))
    feature_summary = data[feature_columns].agg(["mean", "std", "min", "max"]).transpose().reset_index()
    feature_summary = feature_summary.rename(columns={"index": "feature"})
    if hasattr(model, "feature_importances_"):
        feature_summary["importance"] = list(model.feature_importances_)
    else:
        feature_summary["importance"] = 0.0
    prediction_summary = (
        pd.DataFrame({"target": test_y.to_numpy(), "prediction": predicted})
        .groupby(["target", "prediction"])
        .size()
        .reset_index(name="count")
    )
    busy_sleep(max(0.0, target_runtime_seconds - (time.perf_counter() - started)))
    duration = time.perf_counter() - started
    job_summary = pd.DataFrame(
        [
            {
                "job_id": job_id,
                "host_name": host_name,
                "rows": len(data),
                "features": len(feature_columns),
                "duration_seconds": float(duration),
                "model_score": score,
                "cpu_core_ids": ",".join(str(cpu_id) for cpu_id in (cpu_core_ids or [])),
                "cpu_affinity_supported": bool(cpu_affinity_supported),
                "cpu_affinity_enforced": bool(cpu_affinity_enforced),
                "cpu_affinity_error": cpu_affinity_error,
                "error": "",
            }
        ]
    )
    feature_summary.insert(0, "job_id", job_id)
    prediction_summary.insert(0, "job_id", job_id)
    return {
        "job_summary": job_summary,
        "feature_summary": feature_summary,
        "prediction_summary": prediction_summary,
    }


def busy_sleep(seconds: float) -> None:
    deadline = time.perf_counter() + max(0.0, seconds)
    value = 0
    while time.perf_counter() < deadline:
        for index in range(20_000):
            value ^= (index * 2654435761) & 0xFFFFFFFF
    if value == -1:  # pragma: no cover - keeps the loop observable to the optimizer
        raise RuntimeError("unreachable")


def frames_to_payloads(frames: dict[str, pd.DataFrame]) -> dict[str, bytes]:
    payloads: dict[str, bytes] = {}
    for name, frame in frames.items():
        buffer = io.BytesIO()
        frame.to_pickle(buffer)
        payloads[name] = buffer.getvalue()
    return payloads


def payloads_to_frames(payloads: dict[str, bytes]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for name, payload in payloads.items():
        frames[name] = pd.read_pickle(io.BytesIO(payload))
    return frames


def append_frames_to_sqlite(
    db_path: str | Path,
    payloads: dict[str, bytes],
    *,
    lock_timeout_seconds: float = 30.0,
) -> None:
    path = Path(db_path).expanduser().resolve(strict=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    frames = payloads_to_frames(payloads)
    with (
        CrossProcessFileLock(default_lock_path(path), timeout=lock_timeout_seconds),
        sqlite3.connect(path) as connection,
    ):
        connection.execute("BEGIN IMMEDIATE")
        try:
            for table_name, frame in frames.items():
                frame.to_sql(table_name, connection, if_exists="append", index=False)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
