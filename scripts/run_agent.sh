#!/usr/bin/env bash
set -euo pipefail

: "${OUTPUT_DIR:?OUTPUT_DIR required}"
: "${CONFIG_PATH:?CONFIG_PATH required}"
: "${NEBIUS_API_KEY:?NEBIUS_API_KEY required — set in .env or Airflow environment}"

bash scripts/mini-swe-bench-batch.sh
bash scripts/fix_run_ownership.sh
