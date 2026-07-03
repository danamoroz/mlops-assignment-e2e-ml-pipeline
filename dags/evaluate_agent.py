"""Airflow DAG: run mini-swe-agent batch, SWE-bench eval, MLflow logging."""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sdk import get_current_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
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


def _find_aggregate_report(eval_dir: Path) -> Path:
    reports_dir = eval_dir / "reports"
    search_dirs = [reports_dir, eval_dir] if reports_dir.is_dir() else [eval_dir]
    for directory in search_dirs:
        for path in sorted(directory.glob("*.json")):
            if path.name == "metrics.json":
                continue
            try:
                data = json.loads(path.read_text())
            except json.JSONDecodeError:
                continue
            if "resolved_instances" in data:
                return path
    raise FileNotFoundError(
        f"No SWE-bench aggregate report JSON found under {eval_dir}. "
        f"Looked in {[str(d) for d in search_dirs]}."
    )


def collect_metrics(eval_dir: Path) -> dict:
    eval_dir = Path(eval_dir)
    report_path = _find_aggregate_report(eval_dir)
    data = json.loads(report_path.read_text())

    submitted = int(data.get("submitted_instances", 0))
    resolved = int(data.get("resolved_instances", 0))
    metrics = {
        "submitted_instances": submitted,
        "completed_instances": int(data.get("completed_instances", 0)),
        "resolved_instances": resolved,
        "unresolved_instances": int(data.get("unresolved_instances", 0)),
        "error_instances": int(data.get("error_instances", 0)),
        "resolved_rate": (resolved / submitted) if submitted else 0.0,
        "aggregate_report": str(report_path),
    }
    return metrics


def log_mlflow_run(run_config: dict, metrics_path: Path) -> None:
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
    tags=["swe-bench", "phase-1"],
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
    def summarize_and_log(run_config: dict, eval_dir: str) -> None:
        eval_path = Path(eval_dir)
        metrics = collect_metrics(eval_path)
        metrics_path = Path(run_config["run_dir"]) / "metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2) + "\n")
        log_mlflow_run(run_config, metrics_path)

    cfg = prepare_run()
    preds = run_agent(cfg)
    eval_out = run_eval(cfg, preds)
    summarize_and_log(cfg, eval_out)


evaluate_agent_dag()
