#!/usr/bin/env bash
set -euo pipefail

: "${PREDICTIONS_PATH:?PREDICTIONS_PATH required}"
: "${OUTPUT_DIR:?OUTPUT_DIR required}"

test -s "$PREDICTIONS_PATH"

bash scripts/swe-bench-eval.sh
bash scripts/fix_run_ownership.sh
