#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="${REPO_ROOT}/docs/smoke-airflow-watch.log"

log() { echo "$(date -Is) $*" | tee -a "$LOG"; }

log "Watching for mini-extra swebench-single to finish..."

while pgrep -f '[m]ini-extra swebench-single' >/dev/null 2>&1; do
  pid_info=$(pgrep -af '[m]ini-extra swebench-single' | head -1 || true)
  log "still running: ${pid_info:-unknown}"
  sleep 30
done

log "mini-extra finished"

cd "$REPO_ROOT"

if python3 - <<'PY'
import json, sys
from pathlib import Path
p = Path("trajectory.json")
d = json.loads(p.read_text())
info = d.get("info", {})
status = info.get("exit_status", "")
msgs = len(d.get("messages", []))
api_calls = info.get("model_stats", {}).get("api_calls", "?")
print(f"exit_status={status!r}, messages={msgs}, api_calls={api_calls}")
if msgs < 1:
    sys.exit(1)
PY
then
  log "trajectory.json validated"
else
  log "WARNING: trajectory.json validation failed (check file manually)"
fi

if pgrep -f '[a]pache-airflow' >/dev/null 2>&1 || pgrep -f '[a]irflow standalone' >/dev/null 2>&1; then
  log "Airflow already running; skipping start"
  exit 0
fi

log "Starting Airflow standalone (logs appended here)..."
exec bash run-airflow-standalone.sh >>"$LOG" 2>&1
