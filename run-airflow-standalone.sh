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

mkdir -p "$AIRFLOW_HOME"

echo '{"admin": "admin"}' > "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"

uv tool run apache-airflow standalone
