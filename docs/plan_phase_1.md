# Phase 1 Plan: Speedrun Working DAG

**Source:** README §Suggested Implementation Path — Phase 1 (lines 75–99)

**Goal:** One Airflow trigger starts a small SWE-bench batch, evaluates it, and writes a reproducible run directory under `runs/<run-id>/`.

**Mode:** Easy mode — subprocess / BashOperator-style calls to existing scripts; no `DockerOperator` or S3 upload yet (those belong to Phase 3 and Phase 2 respectively).

---

## Success Criteria

When Phase 1 is done, you can:

1. Open the Airflow UI, trigger DAG `evaluate_agent`, set params, and get a green run.
2. Find a complete folder at `runs/<run-id>/` with `config.json`, agent outputs, eval outputs, and `metrics.json`.
3. See the same run in MLflow with logged params, metrics, and a reference to the artifact path.

---

## Starting Point (What Already Exists)

| Asset | Role | Gap for Phase 1 |
|-------|------|-----------------|
| `dags/mini-swe-bench-single.py` | Example DAG: one hard-coded `mini-extra swebench-single` call | Hard-coded subset/split/model/instance; no eval, no `runs/` layout, no MLflow |
| `scripts/mini-swe-bench-batch.sh` | Batch agent: `mini-extra swebench` with `--slice 0:3`, `--workers 5` | Writes to `trajectories/` in CWD; not parameterized; not wired to `runs/<run-id>/run-agent/` |
| `scripts/swe-bench-eval.sh` | Eval harness: `python -m swebench.harness.run_evaluation` | Hard-coded paths and `--run_id test`; not parameterized |
| `sample/` | Example trajectories, `preds.json`, eval logs/reports | Reference for expected formats |
| `run-airflow-standalone.sh` | Local Airflow with `dags/` folder | No MLflow server yet — start a simple local MLflow for Phase 1 |
| `.env` / `NEBIUS_API_KEY` | Inference credentials for mini-swe-agent | Required before `run_agent` |

**Decision:** Create a new DAG `dags/evaluate_agent.py` rather than overloading `mini-swe-bench-single.py`. Keep the old DAG as a smoke-test reference.

---

## Target Architecture

```text
Airflow UI (params)
       │
       ▼
┌──────────────────┐
│   prepare_run    │  build config, mkdir runs/<run-id>/, write config.json
└────────┬─────────┘
         ▼
┌──────────────────┐
│    run_agent     │  scripts/mini-swe-bench-batch.sh → runs/<run-id>/run-agent/
└────────┬─────────┘
         ▼
┌──────────────────┐
│    run_eval      │  scripts/swe-bench-eval.sh → runs/<run-id>/run-eval/
└────────┬─────────┘
         ▼
┌──────────────────┐
│ summarize_and_log│  metrics.json + MLflow logging
└──────────────────┘
```

Task dependency chain: linear — each step consumes outputs from the previous one.

---

## Airflow Parameters

Define DAG `params` (with sensible defaults for a fast first run):

| Param | Required | Default (suggested) | Used by |
|-------|----------|---------------------|---------|
| `split` | yes | `"test"` | Agent + eval dataset split |
| `subset` | yes | `"verified"` | Agent SWE-bench subset |
| `workers` | yes | `5` | Agent parallelism + eval `--max_workers` |
| `model` | optional | `"nebius/moonshotai/Kimi-K2.6"` | Agent `--model` |
| `task_slice` | optional | `"0:3"` | Agent `--slice` (keep batch small for speedrun) |
| `run_id` | optional | auto-generated timestamp UUID if empty | Directory name + MLflow run name |
| `cost_limit` | optional | `0` | Agent `--cost-limit` |

**Run ID generation:** If `run_id` param is empty, generate something like `20260704T120000Z-a1b2c3` and persist it in `config.json` so the folder name is stable for the whole DAG run.

**No hard-coding in task bodies:** Read all experiment values from `context["params"]` (or `@dag(params=...)` defaults).

---

## Run Directory Layout (Phase 1 Minimum)

Phase 2 adds `manifest.json` and tighter structure; Phase 1 should already aim for this shape:

```text
runs/<run-id>/
  config.json                 # frozen copy of all params + metadata
  run-agent/
    preds.json                # required input for eval
    astropy__astropy-12907/   # per-instance trajectory dirs (from mini-swe-agent)
    ...
  run-eval/
    logs/                     # SWE-bench harness logs (mirror sample/logs/...)
    reports/                  # optional: aggregated report JSON if you copy/symlink
  metrics.json                # parsed summary metrics
```

Reference formats:

- `preds.json`: see `sample/trajectories/preds.json` — map of `instance_id → {model_name_or_path, instance_id, model_patch}`.
- Eval summary: SWE-bench writes aggregate JSON like `sample/nebius__moonshotai__Kimi-K2.6.test.json` with `resolved_instances`, `submitted_instances`, etc.
- Per-instance reports: `sample/logs/.../report.json` with `resolved` boolean per instance.

---

## Helper Functions

Implement inside `dags/evaluate_agent.py` first; extract to `src/pipeline/` only if the file grows past ~200 lines.

### `build_run_config(params) -> dict`

- Merge Airflow params with derived fields: `run_id`, `created_at`, `project_root`, paths for `run_dir`, `agent_dir`, `eval_dir`.
- Normalize types (`workers` → int, `cost_limit` → float).
- Return a JSON-serializable dict.

### `prepare_run_dir(run_config) -> Path`

- `run_dir = PROJECT_ROOT / "runs" / run_config["run_id"]`.
- Create `run_dir`, `run_dir / "run-agent"`, `run_dir / "run-eval"`.
- Write `run_dir / "config.json"`.
- Return `run_dir`.

### `run_agent_batch(run_config, run_dir) -> Path`

- Invoke batch agent with params mapped to CLI flags.
- **Preferred for Phase 1:** extend `scripts/mini-swe-bench-batch.sh` to accept env vars or positional args, e.g.:

  ```bash
  SPLIT=test SUBSET=verified MODEL=... SLICE='0:3' WORKERS=5 COST_LIMIT=0 \
    OUTPUT_DIR=runs/<run-id>/run-agent \
    bash scripts/mini-swe-bench-batch.sh
  ```

- Ensure output lands in `run-agent/` including `preds.json` (mini-swe-agent typically writes `preds.json` alongside trajectory dirs when `-o` points at the output dir).
- Return path to `preds.json`; fail the task if file missing.

### `run_swebench_eval(run_config, preds_path, run_dir) -> Path`

- Extend `scripts/swe-bench-eval.sh` to accept:
  - `PREDICTIONS_PATH`
  - `MAX_WORKERS` (from `workers`)
  - `RUN_ID` (use pipeline `run_id` or a sanitized eval sub-id)
  - `OUTPUT_DIR=runs/<run-id>/run-eval`
- Map `subset=verified` → `--dataset_name princeton-nlp/SWE-bench_Verified` (current script default).
- Return path to eval output root (logs dir or aggregate report).

### `collect_metrics(eval_dir) -> dict`

- Parse SWE-bench aggregate report JSON (glob for `*.test.json` or known path under `run-eval/logs/`).
- Extract at minimum:
  - `resolved_instances`
  - `submitted_instances`
  - `completed_instances`
  - `resolved_rate` = `resolved_instances / submitted_instances` (if submitted > 0)
- Optionally aggregate per-instance `resolved` from `report.json` files.
- Write `runs/<run-id>/metrics.json` and return the metrics dict.

### `log_mlflow_run(run_config, metrics, artifact_uri) -> None`

- Use `mlflow` Python client (add `mlflow` to project dependencies if not present).
- Set tracking URI via env `MLFLOW_TRACKING_URI` (default `http://127.0.0.1:5000` for local standalone).
- `mlflow.start_run(run_name=run_config["run_id"])`.
- Log all params from `config.json` (flatten or prefix nested keys).
- Log metrics from `collect_metrics`.
- Log artifact path: `mlflow.log_param("artifact_uri", artifact_uri)` and/or `mlflow.log_artifacts(run_dir)` if MLflow server is local.
- End run.

**Phase 1 MLflow setup (minimal):** In a separate terminal on the VM:

```bash
uv add mlflow
mlflow server --host 0.0.0.0 --port 5000 --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlruns
```

Port-forward `:5000` from laptop like Airflow. Document URI in comments; full Compose deployment is Phase 3.

---

## DAG Task Breakdown

### Task 1: `prepare_run`

**Input:** Airflow context params.

**Actions:**

1. Call `build_run_config(params)`.
2. Call `prepare_run_dir(run_config)`.
3. Return `run_config` (or serializable subset) via XCom for downstream tasks.

**Output:** `runs/<run-id>/config.json` exists; directories created.

### Task 2: `run_agent`

**Input:** XCom `run_config` from `prepare_run`.

**Actions:**

1. Resolve `run_dir` from config.
2. Call `run_agent_batch(run_config, run_dir)`.
3. Verify `run-agent/preds.json` exists and is non-empty JSON.

**Output:** Trajectories + `preds.json` under `run-agent/`.

**Runtime note:** This is the long step (LLM API calls). Set a generous task timeout in Airflow (e.g. 2–4 hours for larger slices; 30–60 min for `0:3`).

**Env:** Pass through `NEBIUS_API_KEY` from `.env` / Airflow env; set `MSWEA_COST_TRACKING=ignore_errors` like existing scripts.

### Task 3: `run_eval`

**Input:** XCom `run_config`; `preds_path = run_dir / "run-agent" / "preds.json"`.

**Actions:**

1. Call `run_swebench_eval(run_config, preds_path, run_dir)`.
2. Verify eval logs exist under `run-eval/`.

**Output:** SWE-bench logs and reports under `run-eval/`.

**Runtime note:** Docker-heavy; ensure Docker daemon available on VM (already a prerequisite).

### Task 4: `summarize_and_log`

**Input:** XCom `run_config`.

**Actions:**

1. `metrics = collect_metrics(run_dir / "run-eval")`.
2. Write `runs/<run-id>/metrics.json`.
3. `log_mlflow_run(run_config, metrics, artifact_uri=str(run_dir))`.

**Output:** `metrics.json` + MLflow run.

---

## Script Changes (Small, Explicit)

### `scripts/mini-swe-bench-batch.sh`

Replace hard-coded values with variables and a required `OUTPUT_DIR`:

```bash
SUBSET="${SUBSET:-verified}"
SPLIT="${SPLIT:-test}"
MODEL="${MODEL:-nebius/moonshotai/Kimi-K2.6}"
SLICE="${SLICE:-0:3}"
WORKERS="${WORKERS:-5}"
COST_LIMIT="${COST_LIMIT:-0}"
OUTPUT_DIR="${OUTPUT_DIR:?OUTPUT_DIR required}"

MSWEA_COST_TRACKING='ignore_errors' mini-extra swebench \
  --subset "$SUBSET" \
  --split "$SPLIT" \
  --model "$MODEL" \
  --slice "$SLICE" \
  --workers "$WORKERS" \
  --cost-limit "$COST_LIMIT" \
  --config mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml \
  -o "$OUTPUT_DIR"
```

**Note:** Confirm on VM that `mini-swe-agent` config path resolves (sibling clone per README). If not, pass absolute path from `PROJECT_ROOT/../mini-swe-agent/...`.

### `scripts/swe-bench-eval.sh`

Parameterize:

```bash
PREDICTIONS_PATH="${PREDICTIONS_PATH:?required}"
MAX_WORKERS="${MAX_WORKERS:-5}"
EVAL_RUN_ID="${EVAL_RUN_ID:-eval}"
DATASET_NAME="${DATASET_NAME:-princeton-nlp/SWE-bench_Verified}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"

python -m swebench.harness.run_evaluation \
  --dataset_name "$DATASET_NAME" \
  --predictions_path "$PREDICTIONS_PATH" \
  --max_workers "$MAX_WORKERS" \
  --run_id "$EVAL_RUN_ID"
```

Check SWE-bench CLI for an explicit log/output directory flag; if none, run with `cwd=OUTPUT_DIR` or move harness output into `run-eval/logs/` post-hoc to match the target layout.

---

## DAG Skeleton (`dags/evaluate_agent.py`)

High-level structure to implement:

```python
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"

@dag(
    dag_id="evaluate_agent",
    schedule=None,
    catchup=False,
    params={...defaults...},
    tags=["swe-bench", "phase-1"],
)
def evaluate_agent_dag():
    @task
    def prepare_run(**context) -> dict: ...

    @task
    def run_agent(run_config: dict) -> str: ...  # return preds_path

    @task
    def run_eval(run_config: dict, preds_path: str) -> str: ...

    @task
    def summarize_and_log(run_config: dict) -> None: ...

    cfg = prepare_run()
    preds = run_agent(cfg)
    eval_out = run_eval(cfg, preds)
    summarize_and_log(cfg)
```

Use `subprocess.run(..., check=True, cwd=PROJECT_ROOT, env=...)` consistently with `mini-swe-bench-single.py`.

---

## Dependencies to Add

| Package | Why |
|---------|-----|
| `mlflow` | `summarize_and_log` |
| `apache-airflow` | Already via `uv tool run` in standalone script; optional pin in dev group |

```bash
uv add mlflow
```

---

## Implementation Checklist

### Step 0 — Prerequisites

- [ ] VM setup complete (see `docs/plan_req.md` Phase 6).
- [ ] `bash scripts/mini-swe-bench-single.sh` succeeds.
- [ ] `mini-swe-agent` cloned as sibling repo; batch script config path works.

### Step 1 — Parameterize scripts

- [ ] Update `scripts/mini-swe-bench-batch.sh` with env vars + `OUTPUT_DIR`.
- [ ] Update `scripts/swe-bench-eval.sh` with env vars.
- [ ] Manual test from repo root:

  ```bash
  RUN_ID=manual-test-001
  mkdir -p "runs/$RUN_ID/run-agent" "runs/$RUN_ID/run-eval"
  OUTPUT_DIR="runs/$RUN_ID/run-agent" SLICE='0:1' WORKERS=1 \
    bash scripts/mini-swe-bench-batch.sh
  PREDICTIONS_PATH="runs/$RUN_ID/run-agent/preds.json" MAX_WORKERS=1 \
    EVAL_RUN_ID="$RUN_ID" bash scripts/swe-bench-eval.sh
  ```

### Step 2 — Helpers + DAG

- [ ] Create `dags/evaluate_agent.py` with five helpers and four tasks.
- [ ] Wire linear dependencies.
- [ ] Add task timeouts and `retries=0` for Phase 1 (retries in Phase 3).

### Step 3 — MLflow

- [ ] `uv add mlflow`; start local MLflow server.
- [ ] Set `MLFLOW_TRACKING_URI` in environment before Airflow (or in DAG env).
- [ ] Implement `log_mlflow_run`.

### Step 4 — End-to-end Airflow test

- [ ] Restart / refresh Airflow standalone.
- [ ] Trigger `evaluate_agent` with defaults (`slice 0:3`).
- [ ] Confirm green tasks and inspect `runs/<run-id>/`.
- [ ] Confirm MLflow run with params + metrics.

### Step 5 — Evidence (feeds Final Deliverables)

- [ ] Keep one completed `runs/<run-id>/` (or a trimmed sample) for submission.
- [ ] Screenshot MLflow run (optional now; required in final `REPORT.md`).

---

## Testing Strategy

| Level | What to verify |
|-------|----------------|
| Unit-ish | `build_run_config` produces valid paths; `collect_metrics` parses `sample/nebius__moonshotai__Kimi-K2.6.test.json` |
| Script | Parameterized batch + eval scripts write to custom `OUTPUT_DIR` |
| DAG task isolation | Run each task function locally with a stub `run_config` before full trigger |
| E2E | Full DAG with `task_slice=0:1` for fastest API cost, then `0:3` for demo |

**Failure modes to handle explicitly:**

- Missing `NEBIUS_API_KEY` → clear error in `run_agent`.
- Missing `preds.json` → fail before eval.
- Empty metrics file → fail `summarize_and_log` with log path hints.

---

## Explicitly Out of Scope (Later Phases)

| Item | Phase |
|------|-------|
| `manifest.json` | Phase 2 |
| S3 / object storage upload | Phase 2 |
| `DockerOperator` | Phase 3 |
| `docker-compose.yaml` for Airflow + MLflow | Phase 3 |
| Retries, production timeouts | Phase 3 |
| `REPORT.md` | Final deliverable (draft after Phase 1 E2E works) |

---

## Estimated Effort

| Work item | Time (rough) |
|-----------|--------------|
| Script parameterization | 1–2 h |
| DAG + helpers | 2–4 h |
| MLflow wiring | 1 h |
| First E2E debug (paths, Docker, API) | 2–4 h |
| **Total Phase 1** | **~1 day** |

The first full agent+eval run dominates wall-clock time, not coding time.

---

## Open Questions to Resolve During Implementation

1. **Exact SWE-bench log output path** — confirm where `run_evaluation` writes logs relative to CWD; adjust `run_swebench_eval` to copy or set cwd so everything lands under `run-eval/logs/`.
2. **mini-swe-agent config path** — sibling clone vs bundled config; use absolute path from `PROJECT_ROOT`.
3. **Eval `--run_id` vs pipeline `run_id`** — SWE-bench uses `run_id` for log subfolders; prefer pipeline `run_id` for traceability.
4. **Airflow env loading** — ensure `.env` is sourced or `NEBIUS_API_KEY` exported before `run-airflow-standalone.sh` (Airflow does not auto-load `.env`).

---

## Summary

Phase 1 is an orchestration speedrun: parameterize the two existing shell scripts, add a four-task Airflow DAG with small Python helpers, write everything under `runs/<run-id>/`, parse eval reports into `metrics.json`, and log to a local MLflow server. Keep the DAG readable and explicit; defer Docker, S3, and manifest work to later phases.
