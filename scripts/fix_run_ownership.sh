#!/usr/bin/env bash
# Pipeline containers run as root; hand ownership to Airflow for downstream @tasks.
set -euo pipefail

if [[ -z "${AIRFLOW_RUNS_OWNER:-}" || -z "${OUTPUT_DIR:-}" ]]; then
  exit 0
fi

run_root="$(cd "$(dirname "$OUTPUT_DIR")" && pwd)"
chown -R "${AIRFLOW_RUNS_OWNER}:${AIRFLOW_RUNS_GROUP:-0}" "$run_root"
