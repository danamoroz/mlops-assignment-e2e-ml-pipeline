#!/usr/bin/env python3
"""Upload an existing runs/<run-id>/ folder to MinIO/S3 and refresh manifest.json.

Use when ARTIFACT_UPLOAD was off during the DAG run, or to backfill the canonical
example run without re-running agent/eval. Patches container paths in config.json
(/opt/airflow/...) to the local repo path before upload.

Requires ARTIFACT_UPLOAD=1 and S3_* / AWS_* env vars (see docs/artifact_upload.md).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.run_durable import (  # noqa: E402
    artifact_upload_enabled,
    build_manifest,
    upload_run_artifacts,
    write_manifest,
)


def patch_config_for_host(config: dict, run_dir: Path) -> dict:
    patched = dict(config)
    patched["project_root"] = str(PROJECT_ROOT)
    patched["run_dir"] = str(run_dir)
    patched["agent_dir"] = str(run_dir / "run-agent")
    patched["eval_dir"] = str(run_dir / "run-eval")
    return patched


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="Run directory name under runs/")
    parser.add_argument(
        "--airflow-run-id",
        default=None,
        help="Optional Airflow DAG run id for manifest provenance",
    )
    args = parser.parse_args()

    if not artifact_upload_enabled():
        raise SystemExit("Set ARTIFACT_UPLOAD=1 and S3/AWS env vars before running.")

    run_dir = PROJECT_ROOT / "runs" / args.run_id
    if not run_dir.is_dir():
        raise SystemExit(f"Run directory not found: {run_dir}")

    config_path = run_dir / "config.json"
    metrics_path = run_dir / "metrics.json"
    manifest_path = run_dir / "manifest.json"

    config = patch_config_for_host(json.loads(config_path.read_text()), run_dir)
    metrics = json.loads(metrics_path.read_text())

    airflow_run_id = args.airflow_run_id
    if airflow_run_id is None and manifest_path.is_file():
        airflow_run_id = json.loads(manifest_path.read_text()).get("provenance", {}).get(
            "airflow_run_id"
        )

    result = upload_run_artifacts(config)
    manifest = build_manifest(
        config,
        metrics,
        upload_result=result,
        airflow_run_id=airflow_run_id,
    )
    write_manifest(run_dir, manifest)
    print(result["remote_uri"])


if __name__ == "__main__":
    main()
