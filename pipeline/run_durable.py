"""Phase 2: normalize run layout, build manifest, optional S3/MinIO upload."""

from __future__ import annotations

import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

INSTANCE_DIR_PATTERN = re.compile(r"^.+__.+-[0-9]+$")

AGENT_ROOT_KEEP = frozenset({"preds.json", "trajectories"})


def _is_trajectory_instance_dir(path: Path) -> bool:
    if not path.is_dir() or path.name in AGENT_ROOT_KEEP or path.name.startswith("."):
        return False
    if INSTANCE_DIR_PATTERN.match(path.name):
        return True
    return any(path.glob("*.traj.json"))


def normalize_run_layout(run_config: dict) -> None:
    """Move agent trajectories and eval aggregate reports into canonical subdirs."""
    agent_dir = Path(run_config["agent_dir"])
    eval_dir = Path(run_config["eval_dir"])

    trajectories_dir = agent_dir / "trajectories"
    trajectories_dir.mkdir(parents=True, exist_ok=True)

    for entry in sorted(agent_dir.iterdir()):
        if entry.is_dir() and _is_trajectory_instance_dir(entry):
            if entry.parent == trajectories_dir:
                continue
            dest = trajectories_dir / entry.name
            if dest.exists():
                shutil.rmtree(entry, ignore_errors=True)
                continue
            try:
                shutil.move(str(entry), str(dest))
            except PermissionError:
                shutil.copytree(entry, dest, dirs_exist_ok=True)
                shutil.rmtree(entry, ignore_errors=True)

    reports_dir = eval_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    for path in sorted(eval_dir.glob("*.json")):
        if path.name == "metrics.json":
            continue
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        if "resolved_instances" not in data:
            continue
        dest = reports_dir / path.name
        if dest.exists():
            continue
        shutil.move(str(path), str(dest))


def _find_aggregate_report(eval_dir: Path) -> Path:
    reports_dir = eval_dir / "reports"
    if not reports_dir.is_dir():
        raise FileNotFoundError(
            f"SWE-bench reports dir not found at {reports_dir}. "
            "Run normalize_run_layout first."
        )
    for path in sorted(reports_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        if "resolved_instances" in data:
            return path
    raise FileNotFoundError(
        f"No SWE-bench aggregate report JSON found under {reports_dir}."
    )


def collect_metrics(run_dir: Path, eval_dir: Path | None = None) -> dict:
    eval_dir = Path(eval_dir or run_dir / "run-eval")
    report_path = _find_aggregate_report(eval_dir)
    data = json.loads(report_path.read_text())

    submitted = int(data.get("submitted_instances", 0))
    resolved = int(data.get("resolved_instances", 0))
    rel_report = report_path.relative_to(run_dir)
    return {
        "submitted_instances": submitted,
        "completed_instances": int(data.get("completed_instances", 0)),
        "resolved_instances": resolved,
        "unresolved_instances": int(data.get("unresolved_instances", 0)),
        "error_instances": int(data.get("error_instances", 0)),
        "resolved_rate": (resolved / submitted) if submitted else 0.0,
        "aggregate_report": str(rel_report),
    }


def _rel(path: Path, run_dir: Path) -> str:
    return path.relative_to(run_dir).as_posix()


def _scan_trajectories(agent_dir: Path, run_dir: Path) -> tuple[list[str], list[str]]:
    trajectories_dir = agent_dir / "trajectories"
    instance_ids: list[str] = []
    trajectory_files: list[str] = []

    if not trajectories_dir.is_dir():
        return instance_ids, trajectory_files

    for instance_dir in sorted(trajectories_dir.iterdir()):
        if not instance_dir.is_dir():
            continue
        instance_ids.append(instance_dir.name)
        for traj in sorted(instance_dir.glob("*.traj.json")):
            trajectory_files.append(_rel(traj, run_dir))

    return instance_ids, trajectory_files


def _scan_per_instance_reports(eval_dir: Path, run_dir: Path) -> list[str]:
    logs_root = eval_dir / "logs"
    if not logs_root.is_dir():
        return []
    reports: list[str] = []
    for report in sorted(logs_root.rglob("report.json")):
        reports.append(_rel(report, run_dir))
    return reports


def _scan_agent_log_files(agent_dir: Path, run_dir: Path) -> list[str]:
    logs: list[str] = []
    for name in ("minisweagent.log",):
        path = agent_dir / name
        if path.is_file():
            logs.append(_rel(path, run_dir))
    for path in sorted(agent_dir.glob("exit_statuses_*.yaml")):
        logs.append(_rel(path, run_dir))
    return logs


def build_manifest(
    run_config: dict,
    metrics: dict,
    *,
    upload_result: dict | None = None,
    airflow_run_id: str | None = None,
) -> dict:
    run_dir = Path(run_config["run_dir"])
    agent_dir = Path(run_config["agent_dir"])
    eval_dir = Path(run_config["eval_dir"])

    instance_ids, trajectory_files = _scan_trajectories(agent_dir, run_dir)
    per_instance_reports = _scan_per_instance_reports(eval_dir, run_dir)
    aggregate_report = metrics.get("aggregate_report")

    upload_enabled = bool(upload_result and upload_result.get("enabled"))
    remote_uri = upload_result.get("remote_uri") if upload_result else None
    remote_archive_uri = upload_result.get("remote_archive_uri") if upload_result else None
    uploaded_at = upload_result.get("uploaded_at") if upload_result else None

    bucket = os.environ.get("S3_BUCKET", "mlops-runs")
    prefix = os.environ.get("S3_PREFIX", "runs")
    object_prefix = f"{prefix.rstrip('/')}/{run_config['run_id']}/"

    manifest = {
        "schema_version": "1",
        "run_id": run_config["run_id"],
        "created_at": run_config["created_at"],
        "status": "completed",
        "config": "config.json",
        "metrics": "metrics.json",
        "artifacts": {
            "local_run_dir": str(run_dir.resolve()),
            "remote_uri": remote_uri,
            "remote_archive_uri": remote_archive_uri,
        },
        "agent": {
            "preds_json": "run-agent/preds.json",
            "trajectories_dir": "run-agent/trajectories",
            "instance_ids": instance_ids,
            "trajectory_files": trajectory_files,
            "log_files": _scan_agent_log_files(agent_dir, run_dir),
        },
        "eval": {
            "logs_dir": "run-eval/logs",
            "reports_dir": "run-eval/reports",
            "aggregate_report": aggregate_report,
            "per_instance_reports": per_instance_reports,
        },
        "provenance": {
            "dag_id": "evaluate_agent",
            "airflow_run_id": airflow_run_id,
            "model": run_config["model"],
            "subset": run_config["subset"],
            "split": run_config["split"],
            "task_slice": run_config["task_slice"],
        },
        "upload": {
            "enabled": upload_enabled,
            "backend": "s3",
            "bucket": bucket,
            "prefix": object_prefix,
            "uploaded_at": uploaded_at,
            "notes": (
                "Uploaded to object storage."
                if upload_enabled
                else "Set ARTIFACT_UPLOAD=1 and start MinIO (or configure S3) to populate remote_uri."
            ),
        },
    }
    return manifest


def write_manifest(run_dir: Path, manifest: dict) -> Path:
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest_path


def artifact_upload_enabled(env: dict[str, str] | None = None) -> bool:
    env = env or os.environ
    return env.get("ARTIFACT_UPLOAD", "0").strip() == "1"


def _s3_client():
    import boto3

    kwargs: dict = {}
    endpoint = os.environ.get("S3_ENDPOINT_URL", "").strip()
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        **kwargs,
    )


def upload_run_artifacts(run_config: dict) -> dict:
    """Sync run directory to S3/MinIO. Raises on failure when upload is requested."""
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if not bucket:
        raise RuntimeError("S3_BUCKET is not set. Add it to .env for artifact upload.")

    prefix_root = os.environ.get("S3_PREFIX", "runs").strip().rstrip("/")
    run_dir = Path(run_config["run_dir"])
    run_id = run_config["run_id"]
    object_prefix = f"{prefix_root}/{run_id}"

    client = _s3_client()
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)

    for path in sorted(run_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.name.endswith(".tar.gz"):
            continue
        key = f"{object_prefix}/{path.relative_to(run_dir).as_posix()}"
        client.upload_file(str(path), bucket, key)

    uploaded_at = datetime.now(timezone.utc).isoformat()
    remote_uri = f"s3://{bucket}/{object_prefix}/"
    return {
        "enabled": True,
        "remote_uri": remote_uri,
        "remote_archive_uri": None,
        "uploaded_at": uploaded_at,
        "bucket": bucket,
        "prefix": f"{object_prefix}/",
    }


def finalize_run(
    run_config: dict,
    *,
    airflow_run_id: str | None = None,
) -> dict:
    """Normalize layout, write metrics.json and manifest.json."""
    run_dir = Path(run_config["run_dir"])
    eval_dir = Path(run_config["eval_dir"])

    normalize_run_layout(run_config)

    trajectories_dir = Path(run_config["agent_dir"]) / "trajectories"
    if not trajectories_dir.is_dir() or not any(trajectories_dir.iterdir()):
        raise FileNotFoundError(
            f"No trajectories found under {trajectories_dir}. "
            "Check that run_agent completed and produced instance directories."
        )

    metrics = collect_metrics(run_dir, eval_dir)
    metrics_path = run_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2) + "\n")

    manifest = build_manifest(run_config, metrics, upload_result=None, airflow_run_id=airflow_run_id)
    manifest_path = write_manifest(run_dir, manifest)

    return {
        "run_config": run_config,
        "metrics_path": str(metrics_path),
        "manifest_path": str(manifest_path),
        "airflow_run_id": airflow_run_id,
    }
