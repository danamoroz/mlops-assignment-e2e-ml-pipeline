# Phase 2 Plan: Make The Run Durable

**Source:** README §Suggested Implementation Path — Phase 2 (lines 101–120)

**Goal:** A teammate can understand the whole run from one folder **or** one artifact URI — without asking which script wrote what, or hunting across the repo.

**Prerequisite:** Phase 1 complete — `dags/evaluate_agent.py` runs end-to-end and writes `runs/<run-id>/` with `config.json`, agent outputs, eval outputs, and `metrics.json`.

---

## Success Criteria

When Phase 2 is done, you can:

1. Hand someone `runs/<run-id>/` (or a tarball of it) and they can reconstruct inputs, outputs, trajectories, predictions, eval logs/reports, and metrics from that folder alone.
2. Open `runs/<run-id>/manifest.json` and immediately see pointers to every important file plus where the full artifact bundle lives (local path and, if enabled, remote URI).
3. See the same run in MLflow with params, metrics, a local artifact reference, and (when upload is enabled) a remote `s3://…` or `s3://…`-compatible URI.
4. Document how artifacts would be uploaded to object storage even when upload is skipped — so the path to production is clear.

---

## Object Storage Policy (Course Guidance)

The README asks for S3/object storage for the **full solution**, but also explicitly allows skipping remote storage in the first iteration:

> If you skip remote storage in the first iteration, still write a clear local `runs/<run-id>/` folder and **document how it would be uploaded**.

Course discussion (26–27 Jun 2026) — note who said what:

| Source | Role | Guidance |
|--------|------|----------|
| **README** | Assignment spec | Full solution → remote long-term storage (S3). First iteration → local `runs/<run-id>/` + upload documentation. |
| **Pini Koplovitch** | **Course coordinator** | **Use local** — setting Nebius/cloud object storage requires admin permissions students do not have. **This is the operative guidance for the class.** |
| **Amit Barletz / Ilya Kartchevski** | Fellow students | Suggested **MinIO on Docker** as a local S3-compatible stand-in — a peer workaround, not official course policy. Still runs on your VM (local infra), no cloud admin needed. |

**How to reconcile:** Pini’s answer settles the baseline — focus on a complete local `runs/<run-id>/` folder. Amit/Ilya’s MinIO idea is optional stretch work that stays local while demonstrating the S3 upload pattern the README describes for a “full solution.” Real Nebius/AWS object storage remains out of scope unless you already have admin access.

**Phase 2 decision (recommended):**

1. **Required (Pini + README):** Canonical local `runs/<run-id>/` tree + `manifest.json` + MLflow logging with local artifact path + **written upload procedure** (how you would push to S3/MinIO when enabled).
2. **Optional stretch / README extra credit:** MinIO in Docker on the VM; upload run folder via boto3/AWS CLI; log `s3://bucket/runs/<run-id>/` to MLflow. Peer-suggested, aligns with “use local” since MinIO is self-hosted on the VM.
3. **Not required:** Nebius Object Storage / AWS account provisioning — document env vars and steps for when admin access exists.
4. **Gate upload behind an env flag** so the DAG works without MinIO: `ARTIFACT_UPLOAD=0` (default) vs `ARTIFACT_UPLOAD=1`.

Default path: **local durability first** (coordinator guidance). MinIO upload only if you want extra credit or S3 API practice.

---

## Starting Point (What Phase 1 Already Produces)

Phase 1 is working (`dags/evaluate_agent.py`, `scripts/log_mlflow_run.py`). Example run `runs/20260703T213539Z-e90f8f/`:

```text
runs/<run-id>/
  config.json
  metrics.json
  run-agent/
    preds.json
    astropy__astropy-12907/          # trajectory dir (flat, not under trajectories/)
    minisweagent.log
    exit_statuses_*.yaml
  run-eval/
    nebius__moonshotai__Kimi-K2.6.<run-id>.json   # aggregate report at eval root
    logs/run_evaluation/<run-id>/...              # per-instance logs ✓
```

**Gaps vs Phase 2 target layout:**

| Target | Current | Action |
|--------|---------|--------|
| `run-agent/trajectories/` | Instance dirs at `run-agent/` root | Normalize after agent step |
| `run-eval/reports/` | Aggregate JSON sometimes at `run-eval/` root | Move/copy aggregate into `reports/`; `swe-bench-eval.sh` already passes `--report_dir reports` — enforce in post-processing |
| `manifest.json` | Missing | New helper + DAG step |
| Remote artifact URI in MLflow | Only local `artifact_uri` param | Extend after optional upload |
| Object storage upload | None | Optional MinIO task |

---

## Target Run Directory Layout

Every run must converge to this shape (README Phase 2):

```text
runs/<run-id>/
  config.json
  run-agent/
    preds.json
    trajectories/
      <instance_id>/
        <instance_id>.traj.json
        ...
    minisweagent.log              # optional but useful; reference from manifest
    exit_statuses_*.yaml          # optional
  run-eval/
    logs/
      run_evaluation/<run-id>/...
    reports/
      <model>.<run-id>.json       # SWE-bench aggregate summary
  metrics.json
  manifest.json
```

Optional when upload is enabled:

```text
runs/<run-id>/
  manifest.json                   # includes remote_artifact_uri
  <run-id>.tar.gz                 # optional compressed bundle uploaded to object storage
```

Reference formats unchanged from Phase 1 — see `sample/` and `docs/plan_phase_1.md`.

---

## Target Architecture

Extend the Phase 1 linear DAG with layout normalization, manifest generation, and optional upload:

```text
Airflow UI (params)
       │
       ▼
┌──────────────────┐
│   prepare_run    │  config.json + mkdir scaffold
└────────┬─────────┘
         ▼
┌──────────────────┐
│    run_agent     │  → run-agent/ (raw mini-swe-agent output)
└────────┬─────────┘
         ▼
┌──────────────────┐
│    run_eval      │  → run-eval/logs/, run-eval/reports/
└────────┬─────────┘
         ▼
┌──────────────────┐
│  finalize_run    │  normalize layout, metrics.json, manifest.json
└────────┬─────────┘
         ▼
┌──────────────────┐     ARTIFACT_UPLOAD=0 → skip
│ upload_artifacts │  tar + upload to MinIO/S3 (optional)
└────────┬─────────┘
         ▼
┌──────────────────┐
│ summarize_and_log│  MLflow: params, metrics, local + remote URIs
└──────────────────┘
```

**Alternative (minimal diff):** Keep four tasks and fold `finalize_run` + optional upload into an expanded `summarize_and_log`. Prefer a separate `finalize_run` task so layout is correct before upload and MLflow logging.

Task dependency chain remains linear.

---

## `manifest.json` Schema

Write `runs/<run-id>/manifest.json` as the single entry point for humans and tooling.

Suggested structure:

```json
{
  "schema_version": "1",
  "run_id": "20260703T213539Z-e90f8f",
  "created_at": "2026-07-03T21:35:39.961537+00:00",
  "status": "completed",
  "config": "config.json",
  "metrics": "metrics.json",
  "artifacts": {
    "local_run_dir": "/abs/path/to/runs/<run-id>",
    "remote_uri": null,
    "remote_archive_uri": null
  },
  "agent": {
    "preds_json": "run-agent/preds.json",
    "trajectories_dir": "run-agent/trajectories",
    "instance_ids": ["astropy__astropy-12907"],
    "trajectory_files": [
      "run-agent/trajectories/astropy__astropy-12907/astropy__astropy-12907.traj.json"
    ],
    "log_files": ["run-agent/minisweagent.log"]
  },
  "eval": {
    "logs_dir": "run-eval/logs",
    "reports_dir": "run-eval/reports",
    "aggregate_report": "run-eval/reports/nebius__moonshotai__Kimi-K2.6.<run-id>.json",
    "per_instance_reports": [
      "run-eval/logs/run_evaluation/<run-id>/nebius__moonshotai__Kimi-K2.6/astropy__astropy-12907/report.json"
    ]
  },
  "provenance": {
    "dag_id": "evaluate_agent",
    "airflow_run_id": "<from context if available>",
    "model": "nebius/moonshotai/Kimi-K2.6",
    "subset": "verified",
    "split": "test",
    "task_slice": "0:1"
  },
  "upload": {
    "enabled": false,
    "backend": "minio",
    "bucket": "mlops-runs",
    "prefix": "runs/<run-id>/",
    "uploaded_at": null,
    "notes": "Set ARTIFACT_UPLOAD=1 and start MinIO to populate remote_uri"
  }
}
```

**Rules:**

- All paths under `run_id` are **relative to `runs/<run-id>/`** unless `artifacts.local_run_dir` (absolute, for copy-paste).
- Populate `instance_ids`, `trajectory_files`, and `per_instance_reports` by scanning directories — do not hard-code instance names.
- After upload, set `artifacts.remote_uri` (directory prefix or archive URI) and mirror the same value in MLflow.
- If upload is skipped, keep `remote_uri: null` and fill `upload.notes` with one-line instructions (see §Upload Documentation).

---

## Helper Functions (New / Extended)

Implement in `dags/evaluate_agent.py` first; extract to `src/pipeline/` or `scripts/` if the file grows.

### `normalize_run_layout(run_config) -> None`

Run once after `run_eval`, before metrics/manifest/upload.

1. **`run-agent/trajectories/`**
   - Create `run-agent/trajectories/`.
   - Move every subdirectory matching SWE-bench instance IDs (e.g. `astropy__astropy-12907/`) from `run-agent/` into `trajectories/`.
   - Leave `preds.json`, `minisweagent.log`, and `exit_statuses_*.yaml` at `run-agent/` root (or list them only in manifest).

2. **`run-eval/reports/`**
   - Ensure directory exists.
   - Move any aggregate `*.json` with `resolved_instances` from `run-eval/` root into `reports/`.
   - Leave `logs/` tree untouched.

3. **Idempotency:** If normalization already ran (e.g. retry), detect existing `trajectories/` and skip moves.

### `build_manifest(run_config, metrics: dict, upload_result: dict | None) -> dict`

- Scan normalized tree; build manifest dict per schema above.
- Merge `upload_result` (`remote_uri`, `uploaded_at`, `enabled`) when present.
- Write `runs/<run-id>/manifest.json`.

### `collect_metrics(eval_dir) -> dict` (update)

- After normalization, resolve aggregate report only from `run-eval/reports/`.
- Keep existing metric fields; set `aggregate_report` to the relative path under the run dir.

### `upload_run_artifacts(run_config, manifest_path: Path) -> dict`

**Only when `ARTIFACT_UPLOAD=1` and MinIO/S3 env vars are set.**

1. Optionally create `runs/<run-id>/<run-id>.tar.gz` (exclude the tarball itself from the archive).
2. Upload using **boto3** (`upload_file` / `upload_fileobj`) with `endpoint_url` for MinIO.
3. Upload strategy (pick one, document in manifest):
   - **A — Directory sync:** `aws s3 sync runs/<run-id>/ s3://bucket/runs/<run-id>/ --exclude '*.tar.gz'`
   - **B — Archive only:** upload single `.tar.gz` to `s3://bucket/archives/<run-id>.tar.gz` (smaller object-store footprint)
   - **Recommended for assignment:** **A** for browsable prefix; mention **B** in docs as alternative for huge trajectories.

4. Return `{ "remote_uri": "s3://mlops-runs/runs/<run-id>/", "remote_archive_uri": null, "uploaded_at": "...", "enabled": true }`.

5. On failure: fail the Airflow task with a clear message, or (if you prefer soft failure) log warning, leave `remote_uri` null, and still write manifest — **prefer hard fail when `ARTIFACT_UPLOAD=1`** so misconfiguration is obvious.

### `log_mlflow_run(run_config, metrics_path, manifest_path) -> None`

Extend `scripts/log_mlflow_run.py`:

- Read `manifest.json` for `artifacts.local_run_dir` and `artifacts.remote_uri`.
- Log params: `artifact_uri` (local), `remote_artifact_uri` (if set).
- Keep `mlflow.log_artifacts(run_dir)` for local MLflow artifact store **or** switch to logging only manifest + metrics when run dir is huge — document choice in `REPORT.md`.
- If remote URI is set, log it as a param/tag; optionally register external artifact location if your MLflow version supports it.

---

## MinIO Setup (Optional — Peer-Suggested Local S3)

No cloud admin permissions required. MinIO speaks the S3 API.

### Docker (standalone, quick start)

Add to repo or document in `docs/plan_phase_2.md` / future `REPORT.md`:

```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/data \
  quay.io/minio/minio server /data --console-address ":9001"
```

Create bucket once:

```bash
# AWS CLI with MinIO endpoint
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://mlops-runs
```

Console UI: http://localhost:9001 (port-forward from VM like Airflow).

### Environment variables (extend `.env.example`)

```bash
# Object storage (MinIO local or real S3)
ARTIFACT_UPLOAD=0
S3_ENDPOINT_URL=http://127.0.0.1:9000
S3_BUCKET=mlops-runs
S3_PREFIX=runs
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_DEFAULT_REGION=us-east-1
# For real AWS S3: omit S3_ENDPOINT_URL; use IAM credentials instead
```

Load these in `get_task_env()` / `load_dotenv_into()` like `NEBIUS_API_KEY`.

### Python upload snippet (boto3)

```python
import boto3
from pathlib import Path

def s3_client():
    kwargs = {}
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)

def upload_run_dir(run_dir: Path, bucket: str, prefix: str) -> str:
    client = s3_client()
    for path in run_dir.rglob("*"):
        if path.is_file():
            key = f"{prefix}/{path.relative_to(run_dir).as_posix()}"
            client.upload_file(str(path), bucket, key)
    return f"s3://{bucket}/{prefix}/"
```

Add dependency: `uv add boto3`.

---

## Upload Documentation (Required Even When Skipped)

When `ARTIFACT_UPLOAD=0`, the repo must still explain how upload would work. Add a short section to `REPORT.md` (or `docs/artifact_upload.md` until `REPORT.md` exists):

1. Start MinIO (or configure AWS credentials + bucket).
2. Set `.env`: `ARTIFACT_UPLOAD=1`, `S3_*`, `AWS_*`.
3. Re-trigger DAG `evaluate_agent` — `upload_artifacts` uploads `runs/<run-id>/` to `s3://mlops-runs/runs/<run-id>/`.
4. Verify in MinIO console or `aws s3 ls s3://mlops-runs/runs/<run-id>/ --endpoint-url http://127.0.0.1:9000`.
5. Confirm MLflow run shows `remote_artifact_uri`.

**Production (Nebius Object Storage / AWS):** Same code path — unset `S3_ENDPOINT_URL`, use real credentials and bucket name from platform admin.

---

## DAG Task Breakdown

### Task 1–3: unchanged

`prepare_run` → `run_agent` → `run_eval` (Phase 1 behavior).

### Task 4: `finalize_run` (new)

**Input:** XCom `run_config`, `eval_dir`.

**Actions:**

1. `normalize_run_layout(run_config)`.
2. `metrics = collect_metrics(eval_dir)`.
3. Write `runs/<run-id>/metrics.json`.
4. `build_manifest(run_config, metrics, upload_result=None)` — first pass without remote URIs.
5. Return `run_config` + paths for downstream tasks.

**Output:** Normalized tree, `metrics.json`, draft `manifest.json`.

### Task 5: `upload_artifacts` (new, optional)

**Input:** XCom from `finalize_run`.

**Behavior:**

```python
if os.environ.get("ARTIFACT_UPLOAD", "0") != "1":
    return {"enabled": False, "remote_uri": None}
return upload_run_artifacts(...)
```

**Actions:**

1. If enabled, upload run directory (or tarball).
2. Update `manifest.json` with remote fields (`build_manifest` again or patch in place).

**Output:** Upload result dict for MLflow.

### Task 6: `summarize_and_log` (renamed scope)

**Input:** `run_config`, upload result.

**Actions:**

1. `log_mlflow_run(run_config, metrics_path, manifest_path)`.

**Output:** MLflow run with local + optional remote artifact references.

Update DAG tag: `phase-2`.

---

## Script / Config Changes

| File | Change |
|------|--------|
| `dags/evaluate_agent.py` | Add `normalize_run_layout`, `build_manifest`, `upload_run_artifacts`; new tasks |
| `scripts/log_mlflow_run.py` | Accept `--manifest`; log `remote_artifact_uri` |
| `.env.example` | MinIO/S3 vars + `ARTIFACT_UPLOAD` |
| `pyproject.toml` / `uv.lock` | Add `boto3` when upload implemented |
| `.gitignore` | Ignore `runs/*/`, `*.tar.gz`, MinIO data; commit one small sample manifest or trimmed run |
| `docs/` or `REPORT.md` | Upload procedure (required by README even if upload skipped) |

**Optional:** `docker-compose.minio.yaml` or a MinIO service in Phase 3 compose file — not blocking for Phase 2 if standalone `docker run` is documented.

---

## Implementation Checklist

### Step 0 — Verify Phase 1 baseline

- [ ] Trigger `evaluate_agent` with `task_slice=0:1`; confirm green run.
- [ ] Note current layout under `runs/<run-id>/` for comparison.

### Step 1 — Layout normalization

- [ ] Implement `normalize_run_layout`.
- [ ] Run manually on existing run dir; confirm `trajectories/` and `reports/` layout.
- [ ] Update `collect_metrics` to read only from `run-eval/reports/`.

### Step 2 — Manifest

- [ ] Implement `build_manifest` + schema_version `1`.
- [ ] Wire into `finalize_run` task.
- [ ] Validate manifest paths exist on disk after a full DAG run.

### Step 3 — MinIO (optional stretch / extra credit)

- [ ] Start MinIO container on VM; create bucket `mlops-runs`.
- [ ] Extend `.env.example` and load vars in DAG env.
- [ ] `uv add boto3`; implement `upload_run_artifacts`.
- [ ] Manual test: `ARTIFACT_UPLOAD=1` upload of one completed run.

### Step 4 — DAG integration

- [ ] Add `finalize_run`, `upload_artifacts`, update `summarize_and_log`.
- [ ] Default `ARTIFACT_UPLOAD=0` — DAG must pass without MinIO.
- [ ] With `ARTIFACT_UPLOAD=1` — full path including remote URI in manifest + MLflow.

### Step 5 — Documentation & evidence

- [ ] Document upload procedure (MinIO + production S3 notes).
- [ ] Keep one sample `runs/<run-id>/manifest.json` (trimmed run or full small run).
- [ ] Screenshot: MinIO console or `aws s3 ls` showing uploaded prefix (extra credit).
- [ ] Screenshot: MLflow run with `remote_artifact_uri` (extra credit).

---

## Testing Strategy

| Level | What to verify |
|-------|----------------|
| Unit-ish | `normalize_run_layout` on a copied Phase 1 run dir; manifest paths resolve |
| Manifest | JSON schema valid; all listed files exist relative to run root |
| Upload off | `ARTIFACT_UPLOAD=0` → DAG green; manifest `remote_uri` null; MLflow has local `artifact_uri` |
| Upload on | MinIO running → objects under `s3://mlops-runs/runs/<run-id>/`; manifest + MLflow updated |
| Re-run safety | Normalization idempotent if task retries |

**Failure modes:**

- MinIO not running but `ARTIFACT_UPLOAD=1` → clear task failure.
- Empty `trajectories/` after agent → fail `finalize_run` with hint to check agent output.
- Aggregate report missing from `reports/` → fail metrics collection with glob hints.

---

## Grading Alignment

| README criterion | Phase 2 contribution |
|------------------|------------------------|
| Artifact structure and reproducibility (20%) | Canonical tree + `manifest.json` — **core deliverable** |
| Extra credit: S3 upload | Optional MinIO upload + MLflow remote URI — peer-suggested stretch; **not required** per course coordinator (local folder + upload docs is enough) |
| MLflow tracking (15%) | Log both local and remote artifact references |
| Report (10%) | Document local layout + how to upload |

---

## Explicitly Out of Scope (Phase 3)

| Item | Phase |
|------|-------|
| `DockerOperator` for agent/eval/upload | Phase 3 |
| Full `docker-compose.yaml` for Airflow + MLflow + MinIO | Phase 3 (MinIO can be standalone in Phase 2) |
| Retries/timeouts on upload | Phase 3 |
| Final `REPORT.md` polish | Final deliverable (draft upload section after Phase 2) |

---

## Estimated Effort

| Work item | Time (rough) |
|-----------|--------------|
| Layout normalization + manifest | 2–3 h |
| MinIO setup + boto3 upload (optional) | 2–3 h |
| DAG tasks + MLflow extension | 1–2 h |
| Docs + screenshots | 1 h |
| **Total Phase 2** | **~1 day** |

---

## Open Questions to Resolve During Implementation

1. **Tarball vs directory upload** — default to directory sync for MinIO browser UX; document tarball for very large trajectory sets.
2. **MLflow artifact size** — full `log_artifacts(run_dir)` may duplicate huge trajectories; consider logging only `manifest.json`, `metrics.json`, and `config.json` to MLflow while pointing to S3 for full bundle.
3. **Instance dir detection** — treat dirs under `run-agent/` matching `*_-*` (SWE-bench id pattern) as trajectories; exclude known files by name.
4. **Compose MinIO now or Phase 3** — standalone Docker is enough for Phase 2; fold into compose when Airflow/MLflow move to Docker.

---

## Summary

Phase 2 makes runs **durable and portable**: normalize the Phase 1 output into the README directory shape, add a **`manifest.json` index** that points to every important artifact, and document how upload would work. Per course coordinator guidance, **local storage is sufficient** — skip cloud object storage unless you already have admin access. MinIO on the VM is an optional peer-suggested way to demo S3 upload for extra credit while still keeping everything local.
