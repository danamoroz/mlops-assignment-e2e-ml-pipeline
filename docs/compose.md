# Docker Compose deployment (Phase 3)

Production-style setup: Airflow 3 + MLflow via Compose, agent/eval via `DockerOperator` and the project `Dockerfile`.

Easy-mode fallback: `bash run-airflow-standalone.sh` (see README).

## Prerequisites (manual)

Complete these on the VM before starting Compose:

1. **Docker** installed and your user in the `docker` group (`docker ps` works without `sudo`).
2. **`.env`** copied from `.env.example` with a valid `NEBIUS_API_KEY`.
3. **`mini-swe-agent` cloned** as a sibling repo (default mount `../mini-swe-agent`). Adjust `MINI_SWE_AGENT_DIR` in `.env` if needed.
4. **Pipeline image built** (see below).
5. **`AIRFLOW_UID`** set in `.env` to your host UID (`id -u`) so bind-mounted `logs/` and `runs/` are writable.

For Compose, set in `.env`:

```bash
MLFLOW_TRACKING_URI=http://mlflow:5000
DOCKER_NETWORK=mlops-assignment_default
AIRFLOW_UID=1001   # output of: id -u

# Required for Airflow 3 multi-container auth (generate once, keep stable):
# python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# openssl rand -base64 32
AIRFLOW__CORE__FERNET_KEY=<fernet-key>
AIRFLOW__API_AUTH__JWT_SECRET=<jwt-secret>
```

Keep `http://127.0.0.1:5000` only when using standalone Airflow on the host.

## Build pipeline image

From the repo root:

```bash
docker build -t mlops-pipeline:latest .
```

## Start stack

First-time init (creates DB + admin user):

```bash
docker compose up airflow-init
```

Start services:

```bash
docker compose up -d
```

Services:

| Service | URL (on VM) |
|---------|-------------|
| Airflow UI | http://localhost:8080 (admin / admin) |
| MLflow | http://localhost:5000 |

From your laptop, port-forward SSH: `-L 8080:localhost:8080 -L 5000:localhost:5000`.

## Trigger a run

1. Open Airflow → DAG `evaluate_agent`.
2. Unpause and trigger with params (e.g. `task_slice=0:1` for a quick test).
3. Confirm `runs/<run-id>/manifest.json` on the host and the run in MLflow.

## Optional: MinIO upload

```bash
# In .env: ARTIFACT_UPLOAD=1 and S3_ENDPOINT_URL=http://minio:9000
docker compose --profile upload up -d

# Create bucket once (from host with AWS CLI):
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://mlops-runs
```

See [artifact_upload.md](artifact_upload.md) for verification steps.

## Stop / reset

```bash
docker compose down
# Full reset including DB:
docker compose down -v
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Permission denied on `logs/` | Set `AIRFLOW_UID=$(id -u)` in `.env`, recreate containers |
| `mini-swe-agent repo not found` | Clone repo; check `MINI_SWE_AGENT_DIR` mount |
| DockerOperator cannot connect | Ensure `/var/run/docker.sock` is mounted in Airflow services |
| `Image not found mlops-pipeline` | Run `docker build -t mlops-pipeline:latest .` |
| MLflow unreachable from DAG | Use `MLFLOW_TRACKING_URI=http://mlflow:5000` inside Compose |
