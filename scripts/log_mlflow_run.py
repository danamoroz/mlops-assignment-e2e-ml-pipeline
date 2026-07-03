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
    args = parser.parse_args()

    run_config = json.loads(Path(args.config).read_text())
    metrics = json.loads(Path(args.metrics).read_text())
    artifact_uri = run_config["run_dir"]

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("evaluate_agent")

    with mlflow.start_run(run_name=run_config["run_id"]):
        for key, value in run_config.items():
            mlflow.log_param(key, value)
        mlflow.log_param("artifact_uri", artifact_uri)
        mlflow.log_metrics({k: float(v) for k, v in metrics.items() if k != "aggregate_report"})
        mlflow.log_artifacts(artifact_uri, artifact_path=run_config["run_id"])


if __name__ == "__main__":
    main()
