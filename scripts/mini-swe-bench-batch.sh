#!/usr/bin/env bash
set -euo pipefail

SUBSET="${SUBSET:-verified}"
SPLIT="${SPLIT:-test}"
MODEL="${MODEL:-nebius/moonshotai/Kimi-K2.6}"
SLICE="${SLICE:-0:3}"
WORKERS="${WORKERS:-5}"
COST_LIMIT="${COST_LIMIT:-0}"
OUTPUT_DIR="${OUTPUT_DIR:?OUTPUT_DIR required}"
CONFIG_PATH="${CONFIG_PATH:?CONFIG_PATH required}"

mkdir -p "$OUTPUT_DIR"

MSWEA_COST_TRACKING='ignore_errors' mini-extra swebench \
  --subset "$SUBSET" \
  --split "$SPLIT" \
  --model "$MODEL" \
  --slice "$SLICE" \
  --workers "$WORKERS" \
  --config "$CONFIG_PATH" \
  -o "$OUTPUT_DIR"
