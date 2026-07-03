#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://127.0.0.1:5000}"

mkdir -p mlruns

echo "MLflow tracking URI: $MLFLOW_TRACKING_URI"
echo "Open http://localhost:5000 after port-forwarding (ssh -L 5000:localhost:5000 ...)"

uv run mlflow server \
  --host 0.0.0.0 \
  --port 5000 \
  --backend-store-uri "sqlite:///$ROOT/mlflow.db" \
  --default-artifact-root "$ROOT/mlruns" \
  --allowed-hosts '*' \
  --cors-allowed-origins '*'
