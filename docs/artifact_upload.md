# Artifact upload to object storage

Phase 2 keeps a complete local copy under `runs/<run-id>/` for every evaluation. Upload to object storage is **optional** (`ARTIFACT_UPLOAD=0` by default). Per course coordinator guidance, local durability is sufficient; MinIO on your VM is an optional way to demonstrate the S3 upload pattern.

## Local run layout

After a successful DAG run:

```text
runs/<run-id>/
  config.json
  manifest.json          ← start here
  metrics.json
  run-agent/
    preds.json
    trajectories/
  run-eval/
    logs/
    reports/
```

`manifest.json` lists every important file and records `artifacts.local_run_dir`. When upload is enabled it also sets `artifacts.remote_uri`.

## Upload disabled (default)

1. Trigger DAG `evaluate_agent` in Airflow.
2. Inspect `runs/<run-id>/manifest.json`.
3. Compare runs in MLflow (`artifact_uri` param points to the local run directory).

No MinIO or cloud credentials required.

## Upload enabled (optional — MinIO on Docker)

### 1. Start MinIO

```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/data \
  quay.io/minio/minio server /data --console-address ":9001"
```

Create the bucket once (requires AWS CLI):

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://mlops-runs
```

Console UI: http://localhost:9001 (port-forward from VM if needed).

### 2. Configure environment

Copy from `.env.example` into `.env`:

```bash
ARTIFACT_UPLOAD=1
S3_ENDPOINT_URL=http://127.0.0.1:9000
S3_BUCKET=mlops-runs
S3_PREFIX=runs
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_DEFAULT_REGION=us-east-1
```

Restart Airflow so tasks pick up the new variables.

### 3. Run the pipeline

Trigger DAG `evaluate_agent`. The `upload_artifacts` task syncs `runs/<run-id>/` to `s3://mlops-runs/runs/<run-id>/` and updates `manifest.json` and MLflow (`remote_artifact_uri` param).

### 4. Verify

```bash
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://mlops-runs/runs/<run-id>/ --recursive
```

Or open the MinIO console and browse bucket `mlops-runs`.

## Production (AWS / Nebius Object Storage)

Same code path:

1. Obtain bucket name and credentials from your platform admin.
2. Set `ARTIFACT_UPLOAD=1`, `S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
3. **Unset** `S3_ENDPOINT_URL` for real AWS S3, or set it to your provider's S3-compatible endpoint.
4. Trigger the DAG.

## Manual upload of an existing run

```bash
cd /path/to/repo
export ARTIFACT_UPLOAD=1
# ... S3_* and AWS_* vars ...

uv run python - <<'PY'
import json
import os
from pathlib import Path
from pipeline.run_durable import upload_run_artifacts, build_manifest, write_manifest, collect_metrics

run_dir = Path("runs/<run-id>")
config = json.loads((run_dir / "config.json").read_text())
result = upload_run_artifacts(config)
metrics = json.loads((run_dir / "metrics.json").read_text())
manifest = build_manifest(config, metrics, upload_result=result)
write_manifest(run_dir, manifest)
print(result["remote_uri"])
PY
```

Replace `<run-id>` with your run directory name.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `upload_artifacts` fails with connection error | Start MinIO or set `ARTIFACT_UPLOAD=0` |
| `S3_BUCKET is not set` | Add `S3_BUCKET` to `.env` |
| `NoSuchBucket` | Run `aws s3 mb` against your endpoint |
| MLflow missing `remote_artifact_uri` | Confirm `ARTIFACT_UPLOAD=1` and upload task succeeded |
