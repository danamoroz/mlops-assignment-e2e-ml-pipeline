#!/usr/bin/env bash
set -euo pipefail

PREDICTIONS_PATH="${PREDICTIONS_PATH:?PREDICTIONS_PATH required}"
MAX_WORKERS="${MAX_WORKERS:-5}"
EVAL_RUN_ID="${EVAL_RUN_ID:-eval}"
DATASET_NAME="${DATASET_NAME:-princeton-nlp/SWE-bench_Verified}"
SPLIT="${SPLIT:-test}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"

mkdir -p "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/reports"

if [[ "$PREDICTIONS_PATH" != /* ]]; then
  PREDICTIONS_PATH="$(pwd)/$PREDICTIONS_PATH"
fi

cd "$OUTPUT_DIR"

uv run python -m swebench.harness.run_evaluation \
  --dataset_name "$DATASET_NAME" \
  --split "$SPLIT" \
  --predictions_path "$PREDICTIONS_PATH" \
  --max_workers "$MAX_WORKERS" \
  --run_id "$EVAL_RUN_ID" \
  --report_dir reports
