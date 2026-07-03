#!/usr/bin/env bash
# Phase 6 — smoke tests (run ON the Nebius VM, after NEBIUS_API_KEY is in .env).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f .env ]] || grep -q 'NEBIUS_API_KEY=XXX' .env 2>/dev/null; then
  echo "ERROR: Set a real NEBIUS_API_KEY in ${REPO_ROOT}/.env first."
  exit 1
fi

# shellcheck disable=SC1091
set -a
source .env
set +a

if [[ -z "${NEBIUS_API_KEY:-}" || "${NEBIUS_API_KEY}" == "XXX" ]]; then
  echo "ERROR: NEBIUS_API_KEY is empty or placeholder."
  exit 1
fi

# shellcheck disable=SC1091
source .venv/bin/activate

echo "==> Phase 6.1: mini-swe-bench-single.sh"
bash scripts/mini-swe-bench-single.sh

echo ""
echo "==> Phase 6.2: Airflow standalone"
echo "In another terminal on the laptop, forward port 8080:"
echo "  ssh -L 8080:localhost:8080 nebius-mlops-assignment"
echo "Then open http://localhost:8080 (login: admin / admin)"
echo ""
echo "Starting Airflow now (Ctrl+C to stop)..."
bash run-airflow-standalone.sh
