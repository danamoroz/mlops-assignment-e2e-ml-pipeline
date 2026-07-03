"""Airflow DAG: run mini-swe-agent batch, SWE-bench eval, durable artifacts, MLflow."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sdk import get_current_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.run_durable import (  # noqa: E402
    artifact_upload_enabled,
    build_manifest,
    finalize_run,
    upload_run_artifacts,
    write_manifest,
)

RUNS_ROOT = PROJECT_ROOT / "runs"

DEFAULT_PARAMS = {
    "split": "test",
    "subset": "verified",
    "workers": 5,
    "model": "nebius/moonshotai/Kimi-K2.6",
    "task_slice": "0:3",
    "run_id": "",
    "cost_limit": 0,
}

SUBSET_TO_DATASET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "SWE-bench/SWE-bench_Lite",
}


def load_dotenv_into(env: dict[str, str]) -> dict[str, str]:
    env_file = PROJECT_ROOT / ".env"
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
    return load_dotenv_into(
        {
            **os.environ,
            "MSWEA_COST_TRACKING": "ignore_errors",
        }
    )


def resolve_swebench_config_path() -> Path:
    candidates = [
        PROJECT_ROOT / "mini-swe-agent" / "src/minisweagent/config/benchmarks/swebench.yaml",
        PROJECT_ROOT.parent / "mini-swe-agent" / "src/minisweagent/config/benchmarks/swebench.yaml",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        "mini-swe-agent swebench config not found. Clone mini-swe-agent as a sibling repo "
        "or under the project root (see README)."
    )


def build_run_config(params: dict) -> dict:
    run_id = str(params.get("run_id") or "").strip()
    if not run_id:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_id = f"{ts}-{uuid.uuid4().hex[:6]}"

    subset = str(params.get("subset", DEFAULT_PARAMS["subset"]))
    split = str(params.get("split", DEFAULT_PARAMS["split"]))
    model = str(params.get("model", DEFAULT_PARAMS["model"]))
    task_slice = str(params.get("task_slice", DEFAULT_PARAMS["task_slice"]))

    workers = int(params.get("workers", DEFAULT_PARAMS["workers"]))
    cost_limit = float(params.get("cost_limit", DEFAULT_PARAMS["cost_limit"]))

    run_dir = RUNS_ROOT / run_id
    agent_dir = run_dir / "run-agent"
    eval_dir = run_dir / "run-eval"

    dataset_name = SUBSET_TO_DATASET.get(subset, SUBSET_TO_DATASET["verified"])

    return {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "project_root": str(PROJECT_ROOT),
        "run_dir": str(run_dir),
        "agent_dir": str(agent_dir),
        "eval_dir": str(eval_dir),
        "split": split,
        "subset": subset,
        "workers": workers,
        "model": model,
        "task_slice": task_slice,
        "cost_limit": cost_limit,
        "dataset_name": dataset_name,
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
    if not env.get("NEBIUS_API_KEY"):
        raise RuntimeError(
            "NEBIUS_API_KEY is not set. Add it to .env or export it before starting Airflow."
        )

    agent_dir = Path(run_config["agent_dir"])
    subprocess.run(
        ["bash", str(PROJECT_ROOT / "scripts/mini-swe-bench-batch.sh")],
        cwd=PROJECT_ROOT,
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
    if not preds_path.is_file() or preds_path.stat().st_size == 0:
        raise FileNotFoundError(f"Agent did not produce preds.json at {preds_path}")

    json.loads(preds_path.read_text())
    return preds_path


def run_swebench_eval(run_config: dict, preds_path: Path) -> Path:
    eval_dir = Path(run_config["eval_dir"])
    subprocess.run(
        ["bash", str(PROJECT_ROOT / "scripts/swe-bench-eval.sh")],
        cwd=PROJECT_ROOT,
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

    logs_dir = eval_dir / "logs" / "run_evaluation"
    if not logs_dir.is_dir():
        raise FileNotFoundError(
            f"SWE-bench eval logs not found under {logs_dir}. "
            "Check Docker is running and eval completed."
        )
    return eval_dir


def log_mlflow_run(run_config: dict, metrics_path: Path, manifest_path: Path) -> None:
    subprocess.run(
        [
            "uv",
            "run",
            "python",
            str(PROJECT_ROOT / "scripts/log_mlflow_run.py"),
            "--config",
            str(Path(run_config["run_dir"]) / "config.json"),
            "--metrics",
            str(metrics_path),
            "--manifest",
            str(manifest_path),
        ],
        cwd=PROJECT_ROOT,
        env=get_task_env(),
        check=True,
    )


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params=DEFAULT_PARAMS,
    tags=["swe-bench", "phase-2"],
)
def evaluate_agent_dag():
    @task(retries=0)
    def prepare_run() -> dict:
        context = get_current_context()
        params = dict(context["params"])
        conf = context["dag_run"].conf or {}
        if conf:
            params.update(conf)
        run_config = build_run_config(params)
        prepare_run_dir(run_config)
        return run_config

    @task(retries=0, execution_timeout=timedelta(hours=4))
    def run_agent(run_config: dict) -> str:
        preds_path = run_agent_batch(run_config)
        return str(preds_path)

    @task(retries=0, execution_timeout=timedelta(hours=4))
    def run_eval(run_config: dict, preds_path: str) -> str:
        eval_dir = run_swebench_eval(run_config, Path(preds_path))
        return str(eval_dir)

    @task(retries=0)
    def finalize_run_task(run_config: dict, eval_dir: str) -> dict:
        context = get_current_context()
        dag_run = context.get("dag_run")
        airflow_run_id = str(dag_run.run_id) if dag_run else None
        result = finalize_run(run_config, airflow_run_id=airflow_run_id)
        return result

    @task(retries=0)
    def upload_artifacts(finalize_result: dict) -> dict:
        env = get_task_env()
        run_config = finalize_result["run_config"]
        if not artifact_upload_enabled(env):
            return {"enabled": False, "remote_uri": None, "remote_archive_uri": None, "uploaded_at": None}

        upload_result = upload_run_artifacts(run_config)
        metrics = json.loads(Path(finalize_result["metrics_path"]).read_text())
        manifest = build_manifest(
            run_config,
            metrics,
            upload_result=upload_result,
            airflow_run_id=finalize_result.get("airflow_run_id"),
        )
        write_manifest(Path(run_config["run_dir"]), manifest)
        return upload_result

    @task(retries=0)
    def summarize_and_log(finalize_result: dict, _upload_result: dict) -> None:
        run_config = finalize_result["run_config"]
        log_mlflow_run(
            run_config,
            Path(finalize_result["metrics_path"]),
            Path(finalize_result["manifest_path"]),
        )

    cfg = prepare_run()
    preds = run_agent(cfg)
    eval_out = run_eval(cfg, preds)
    finalized = finalize_run_task(cfg, eval_out)
    uploaded = upload_artifacts(finalized)
    summarize_and_log(finalized, uploaded)


evaluate_agent_dag()
