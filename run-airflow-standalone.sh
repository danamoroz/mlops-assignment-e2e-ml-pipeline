set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://127.0.0.1:5000}"
export PYTHONPATH="${PYTHONPATH:-}:$ROOT"

mkdir -p "$AIRFLOW_HOME" "$ROOT/runs" "$ROOT/logs"

echo '{"admin": "admin"}' > "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"

# Dev fallback (Phase 1–2 style). Production: docker compose (see docs/compose.md).
# DockerOperator requires the docker provider in the Airflow tool env.
if ! uv tool run python -c "from airflow.providers.docker.operators.docker import DockerOperator" 2>/dev/null; then
  echo "Installing apache-airflow with Docker provider for evaluate_agent DAG..."
  uv tool install --force apache-airflow==3.2.2 --with apache-airflow-providers-docker
fi

if ! docker image inspect "${PIPELINE_IMAGE:-mlops-pipeline:latest}" >/dev/null 2>&1; then
  echo "Building pipeline image ${PIPELINE_IMAGE:-mlops-pipeline:latest} (required for DockerOperator)..."
  docker build -t "${PIPELINE_IMAGE:-mlops-pipeline:latest}" .
fi

uv tool run apache-airflow standalone
