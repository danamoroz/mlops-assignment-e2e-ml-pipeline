#!/usr/bin/env python3
"""Log a pipeline run to MLflow (invoked from Airflow via project venv)."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import mlflow


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to run config.json")
    parser.add_argument("--metrics", required=True, help="Path to metrics.json")
    parser.add_argument("--manifest", required=True, help="Path to manifest.json")
    args = parser.parse_args()

    run_config = json.loads(Path(args.config).read_text())
    metrics = json.loads(Path(args.metrics).read_text())
    manifest = json.loads(Path(args.manifest).read_text())

    local_uri = manifest["artifacts"]["local_run_dir"]
    remote_uri = manifest["artifacts"].get("remote_uri")

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("evaluate_agent")

    with mlflow.start_run(run_name=run_config["run_id"]):
        for key, value in run_config.items():
            mlflow.log_param(key, value)
        mlflow.log_param("artifact_uri", local_uri)
        if remote_uri:
            mlflow.log_param("remote_artifact_uri", remote_uri)
            mlflow.set_tag("remote_artifact_uri", remote_uri)
        mlflow.log_metrics({k: float(v) for k, v in metrics.items() if k != "aggregate_report"})
        run_dir = Path(local_uri)
        mlflow.log_artifact(str(run_dir / "manifest.json"), artifact_path=run_config["run_id"])
        mlflow.log_artifact(str(run_dir / "metrics.json"), artifact_path=run_config["run_id"])
        mlflow.log_artifact(str(run_dir / "config.json"), artifact_path=run_config["run_id"])


if __name__ == "__main__":
    main()
