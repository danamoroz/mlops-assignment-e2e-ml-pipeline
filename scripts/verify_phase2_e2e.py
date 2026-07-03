#!/usr/bin/env python3
"""Run evaluate_agent pipeline steps locally (no Airflow)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.run_durable import (  # noqa: E402
    artifact_upload_enabled,
    build_manifest,
    finalize_run,
    upload_run_artifacts,
    write_manifest,
)

RUNS_ROOT = ROOT / "runs"
SUBSET_TO_DATASET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "SWE-bench/SWE-bench_Lite",
}


def load_dotenv_into(env: dict[str, str]) -> dict[str, str]:
    env_file = ROOT / ".env"
    if not env_file.exists():
        return env
    merged = dict(env)
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        merged.setdefault(key.strip(), value.strip())
    return merged


def get_task_env() -> dict[str, str]:
    return load_dotenv_into({**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"})


def resolve_swebench_config_path() -> Path:
    for path in (
        ROOT / "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml",
        ROOT.parent / "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml",
    ):
        if path.exists():
            return path
    raise FileNotFoundError("mini-swe-agent swebench config not found")


def build_run_config(params: dict) -> dict:
    run_id = str(params.get("run_id") or "").strip() or f"phase2-direct-{uuid.uuid4().hex[:6]}"
    subset = str(params.get("subset", "verified"))
    run_dir = RUNS_ROOT / run_id
    return {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "project_root": str(ROOT),
        "run_dir": str(run_dir),
        "agent_dir": str(run_dir / "run-agent"),
        "eval_dir": str(run_dir / "run-eval"),
        "split": str(params.get("split", "test")),
        "subset": subset,
        "workers": int(params.get("workers", 1)),
        "model": str(params.get("model", "nebius/moonshotai/Kimi-K2.6")),
        "task_slice": str(params.get("task_slice", "0:1")),
        "cost_limit": float(params.get("cost_limit", 0)),
        "dataset_name": SUBSET_TO_DATASET.get(subset, SUBSET_TO_DATASET["verified"]),
        "swebench_config": str(resolve_swebench_config_path()),
    }


def prepare_run_dir(run_config: dict) -> Path:
    run_dir = Path(run_config["run_dir"])
    Path(run_config["agent_dir"]).mkdir(parents=True, exist_ok=True)
    Path(run_config["eval_dir"]).mkdir(parents=True, exist_ok=True)
    (run_dir / "config.json").write_text(json.dumps(run_config, indent=2) + "\n")
    return run_dir


def run_agent_batch(run_config: dict) -> Path:
    env = get_task_env()
    agent_dir = Path(run_config["agent_dir"])
    subprocess.run(
        ["bash", str(ROOT / "scripts/mini-swe-bench-batch.sh")],
        cwd=ROOT,
        env={
            **env,
            "SUBSET": run_config["subset"],
            "SPLIT": run_config["split"],
            "MODEL": run_config["model"],
            "SLICE": run_config["task_slice"],
            "WORKERS": str(run_config["workers"]),
            "OUTPUT_DIR": str(agent_dir),
            "CONFIG_PATH": run_config["swebench_config"],
        },
        check=True,
    )
    preds_path = agent_dir / "preds.json"
    if not preds_path.is_file():
        raise FileNotFoundError(f"Missing {preds_path}")
    return preds_path


def run_swebench_eval(run_config: dict, preds_path: Path) -> Path:
    eval_dir = Path(run_config["eval_dir"])
    subprocess.run(
        ["bash", str(ROOT / "scripts/swe-bench-eval.sh")],
        cwd=ROOT,
        env={
            **get_task_env(),
            "PREDICTIONS_PATH": str(preds_path),
            "MAX_WORKERS": str(run_config["workers"]),
            "EVAL_RUN_ID": run_config["run_id"],
            "DATASET_NAME": run_config["dataset_name"],
            "SPLIT": run_config["split"],
            "OUTPUT_DIR": str(eval_dir),
        },
        check=True,
    )
    return eval_dir


def log_mlflow(run_config: dict, metrics_path: Path, manifest_path: Path) -> None:
    subprocess.run(
        [
            "uv", "run", "python", str(ROOT / "scripts/log_mlflow_run.py"),
            "--config", str(Path(run_config["run_dir"]) / "config.json"),
            "--metrics", str(metrics_path),
            "--manifest", str(manifest_path),
        ],
        cwd=ROOT,
        env=get_task_env(),
        check=True,
    )


def main() -> None:
    params = {
        "run_id": os.environ.get("RUN_ID", ""),
        "task_slice": os.environ.get("TASK_SLICE", "0:1"),
        "workers": int(os.environ.get("WORKERS", "1")),
    }
    run_config = build_run_config(params)
    print(f"run_id={run_config['run_id']}")

    prepare_run_dir(run_config)
    preds = run_agent_batch(run_config)
    eval_dir = run_swebench_eval(run_config, preds)
    finalized = finalize_run(run_config, airflow_run_id="local-verify")
    print(f"manifest: {finalized['manifest_path']}")

    if artifact_upload_enabled(get_task_env()):
        upload_result = upload_run_artifacts(run_config)
        metrics = json.loads(Path(finalized["metrics_path"]).read_text())
        write_manifest(
            Path(run_config["run_dir"]),
            build_manifest(run_config, metrics, upload_result=upload_result),
        )
        print(f"upload: {upload_result['remote_uri']}")

    log_mlflow(run_config, Path(finalized["metrics_path"]), Path(finalized["manifest_path"]))
    print("done")


if __name__ == "__main__":
    main()
