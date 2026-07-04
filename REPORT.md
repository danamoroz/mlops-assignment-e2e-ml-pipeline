# MLOps Assignment Report — End-to-End SWE-bench Pipeline

This report describes the production-style deployment: Airflow 3 and MLflow via Docker Compose, with agent and evaluation steps isolated in `DockerOperator` containers built from the project `Dockerfile`.

For step-by-step Compose commands see [docs/compose.md](docs/compose.md). For object storage upload see [docs/artifact_upload.md](docs/artifact_upload.md).

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  docker compose                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐                   │
│  │   Airflow    │  │  scheduler / │  │   MLflow    │                   │
│  │  UI :8080    │  │  workers     │  │   :5000     │                   │
│  └──────┬───────┘  └──────┬───────┘  └──────▲──────┘                   │
│         │                 │                  │                          │
│         │    mounts: dags/, pipeline/, runs/, /var/run/docker.sock     │
└─────────┼─────────────────┼──────────────────┼──────────────────────────┘
          │                 │                  │
          ▼                 ▼                  log metrics
┌─────────────────────────────────────────────────────────────────────────┐
│  DAG evaluate_agent                                                      │
│                                                                          │
│  @task → PipelineDockerOperator → @task verify → PipelineDockerOperator  │
│       → @task finalize → @task upload → @task summarize_and_log         │
│                     │                  │                                 │
│                     ▼                  ▼                                 │
│              mlops-pipeline:latest (same image, docker.sock + runs/)    │
└─────────────────────────────────────────────────────────────────────────┘
          │
          ├──────────────────────────────┐
          ▼                              ▼
   runs/<run-id>/                  MinIO (optional)
   (bind-mounted on host)          s3://mlops-runs/runs/<run-id>/
          │
          └──────────────────────────────► MLflow experiment evaluate_agent
```

**Task chain**

| Task | Operator | Role |
|------|----------|------|
| `prepare_run` | `@task` | Create `runs/<run-id>/`, write `config.json` |
| `run_agent` | `PipelineDockerOperator` | mini-swe-agent batch → `run-agent/preds.json` |
| `verify_agent` | `@task` | Assert `preds.json` exists and is valid JSON |
| `run_eval` | `PipelineDockerOperator` | SWE-bench harness → logs/reports under `run-eval/` |
| `verify_eval` | `@task` | Assert eval logs exist |
| `finalize_run_task` | `@task` | Normalize layout, write `metrics.json` + `manifest.json` |
| `upload_artifacts` | `@task` | S3/MinIO upload when `ARTIFACT_UPLOAD=1` |
| `summarize_and_log` | `@task` | Log params, metrics, and artifact refs to MLflow |

Agent and eval containers need the **host Docker socket** because mini-swe-agent and SWE-bench spawn nested containers for each instance.

**Dev fallback:** `bash run-airflow-standalone.sh` runs Airflow on the host without Compose (useful when debugging DAG logic).

**Production scale-out:** At larger scale, `DockerOperator` can be replaced with `KubernetesPodOperator`; the pipeline image and env contract stay the same.

---

## How to reproduce (Compose)

### Prerequisites

1. Docker installed; user in the `docker` group.
2. Clone [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent) as a sibling of this repo (or set `MINI_SWE_AGENT_DIR` in `.env`).
3. Copy `.env.example` → `.env` and set at minimum:
   - `NEBIUS_API_KEY`
   - `MLFLOW_TRACKING_URI=http://mlflow:5000`
   - `AIRFLOW_UID=$(id -u)`
   - `DOCKER_GID=$(getent group docker | cut -d: -f3)`
   - `AIRFLOW_PROJ_DIR` — absolute path to this repo on the host
   - `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__API_AUTH__JWT_SECRET` (generate once; see `.env.example`)

### Build and start

```bash
docker build -t mlops-pipeline:latest .
docker compose up airflow-init    # once
docker compose up -d
```

| Service | URL on VM |
|---------|-----------|
| Airflow | http://localhost:8080 (admin / admin) |
| MLflow | http://localhost:5000 |
| MinIO console (optional) | http://localhost:9001 (minioadmin / minioadmin) |

From a laptop: `ssh -L 8080:localhost:8080 -L 5000:localhost:5000 -L 9001:localhost:9001 user@vm`.

### Trigger a run

1. Airflow UI → DAG **`evaluate_agent`** → Unpause.
2. **Trigger DAG w/ config**, for example:

```json
{
  "task_slice": "0:1",
  "workers": 1,
  "subset": "verified",
  "split": "test"
}
```

3. Wait for all tasks green (~5–10 min for one instance).
4. Confirm on the host: `runs/<run-id>/manifest.json` with `"status": "completed"`.
5. MLflow → experiment **`evaluate_agent`** → run named `<run-id>`.

**DAG parameters:** `split`, `subset`, `workers` (required for grading); optional: `model`, `task_slice`, `run_id`, `cost_limit`.

To upload artifacts in the same DAG run, set `ARTIFACT_UPLOAD=1` and start MinIO (`docker compose --profile upload up -d`). See [docs/artifact_upload.md](docs/artifact_upload.md).

---

## Artifact layout

Each run writes a durable tree under `runs/<run-id>/`:

```text
runs/<run-id>/
  config.json           ← full run config (params + paths)
  manifest.json         ← index of all important files (start here)
  metrics.json          ← parsed SWE-bench aggregate metrics
  run-agent/
    preds.json
    trajectories/
  run-eval/
    logs/run_evaluation/…
    reports/…
```

`manifest.json` records provenance (DAG id, Airflow run id, model, slice), `artifacts.local_run_dir`, and `artifacts.remote_uri` when object storage upload is enabled.

---

## Example completed run

**Run ID:** `20260704T004726Z-7ea9eb`  
**Airflow DAG run:** `manual__2026-07-04T00:47:25.479293+00:00`  
**Config:** `task_slice=0:1`, `workers=1`, subset `verified`, split `test`, model `nebius/moonshotai/Kimi-K2.6`

**Instance:** `astropy__astropy-12907` (1 of SWE-bench Verified test set)

**Results** (`metrics.json`):

| Metric | Value |
|--------|------:|
| submitted_instances | 1 |
| completed_instances | 1 |
| resolved_instances | 1 |
| resolved_rate | 1.0 |
| error_instances | 0 |

The agent produced a patch in `run-agent/preds.json`; SWE-bench eval confirmed the instance **resolved** (tests pass after the submitted patch).

**MLflow:** experiment `evaluate_agent`, run name `20260704T004726Z-7ea9eb` — params include `run_id`, `model`, `artifact_uri`; metrics mirror `metrics.json`. Artifacts: `config.json`, `metrics.json`, `manifest.json`.

**Object storage:** the canonical run was backfilled to MinIO with `scripts/backfill_object_storage_upload.py` (same code path as the DAG `upload_artifacts` task). Future runs upload automatically when `ARTIFACT_UPLOAD=1`.

| Field | Value |
|-------|-------|
| Remote URI | `s3://mlops-runs/runs/20260704T004726Z-7ea9eb/` |
| Endpoint | `http://127.0.0.1:9000` (MinIO) |
| Objects | 13 files (config, manifest, metrics, preds, trajectories, eval logs/reports) |
| Uploaded at | `2026-07-04T01:38:15Z` (see `manifest.json` → `upload.uploaded_at`) |

Verify on the VM:

```bash
aws s3 ls s3://mlops-runs/runs/20260704T004726Z-7ea9eb/ --recursive \
  --endpoint-url http://127.0.0.1:9000
```

The committed folder `runs/20260704T004726Z-7ea9eb/` in this repo matches the remote copy for offline review without re-running the pipeline.

---

## Rerun by `run-id`

To re-evaluate or extend an existing folder without generating a new id, trigger with a fixed `run_id`:

```json
{
  "run_id": "20260704T004726Z-7ea9eb",
  "task_slice": "0:1",
  "workers": 1
}
```

`prepare_run` reuses `runs/<run-id>/`. Clear `run-agent/` or `run-eval/` manually first if you need a clean retry of agent or eval only.

---

## Screenshots

| File | Description |
|------|-------------|
| [screenshots/airflow_dag.png](screenshots/airflow_dag.png) | Graph view — green run, `PipelineDockerOperator` on `run_agent` / `run_eval` |
| [screenshots/mlflow_runs.png](screenshots/mlflow_runs.png) | MLflow experiment `evaluate_agent`, run `20260704T004726Z-7ea9eb` |
| [screenshots/object_storage_artifacts.png](screenshots/object_storage_artifacts.png) | MinIO Object Browser — bucket `mlops-runs`, prefix `runs/20260704T004726Z-7ea9eb/` |

---

## Implementation notes

- **Pipeline image:** `mlops-pipeline:latest` from the project `Dockerfile` (Ubuntu + `uv sync`, scripts, pipeline code, Docker CLI for nested containers).
- **Pipeline containers** run as root so the image’s uv-managed Python is accessible; `scripts/fix_run_ownership.sh` `chown`s the run folder to `AIRFLOW_UID` after agent/eval so downstream `@task` steps can normalize files.
- **MLflow experiment name** is `evaluate_agent` (not `Default`). Logs go to `mlruns/` on the host, served by the Compose MLflow service.
- **Object storage:** set `ARTIFACT_UPLOAD=1`, `S3_ENDPOINT_URL=http://minio:9000`, and `docker compose --profile upload up -d`. The DAG `upload_artifacts` task syncs each run; use `scripts/backfill_object_storage_upload.py` to upload an existing folder without re-running agent/eval.

---

## File map (submission)

| Path | Purpose |
|------|---------|
| `dags/evaluate_agent.py` | Configurable DAG (DockerOperator + retries + upload) |
| `docker-compose.yaml` | Airflow + Postgres + MLflow (+ optional MinIO) |
| `Dockerfile` / `Dockerfile.airflow` | Pipeline runtime + Airflow image with Docker provider |
| `.env.example` | Non-secret env template |
| `runs/20260704T004726Z-7ea9eb/` | Example completed run (committed; mirrors MinIO copy) |
| `scripts/backfill_object_storage_upload.py` | Backfill upload for existing runs on the host |
| `docs/compose.md`, `docs/artifact_upload.md` | Deployment and upload guides |
| `screenshots/` | Airflow, MLflow, and object storage evidence |
| `REPORT.md` | This document |
