# Setup progress

Track implementation of [plan_req.md](./plan_req.md).

## Phase 0 — Laptop

| Step | Status | Notes |
|------|--------|-------|
| Nebius account | **YOU** | Create/login at Nebius Academy / cloud console |
| `NEBIUS_API_KEY` (laptop) | done | set in local `.env` (236 chars) |
| SSH key pair | done | `~/.ssh/id_ed25519_nebius` |
| Assignment repo (local) | done | This workspace |
| `.env` from example | done | Edit `NEBIUS_API_KEY` before agent runs |
| `uv sync` (local) | done | `.venv` created on laptop |
| SSH config | done | `nebius-mlops-assignment` → `danar@195.242.13.222` |

## Phase 1 — Create VM

| Step | Status | Notes |
|------|--------|-------|
| VM 8 CPU / 32 GB / public IP | done | `195.242.13.222`, user `danar` |
| SSH works | done | 8 CPUs, ~31 GiB RAM, 1.3T disk |

## Phases 2–5 — VM setup (automated)

| Step | Status | Notes |
|------|--------|-------|
| uv installed | done | 0.11.26 |
| Docker installed | done | 29.6.1 + compose 5.3.0 |
| docker without sudo | done | user in `docker` group |
| Assignment repo | done | `~/repos/mlops-assignment-e2e-ml-pipeline` |
| mini-swe-agent / SWE-bench | done | `~/repos/` |
| `uv sync` on VM | done | `.venv` created |
| `.env` on VM | done | copied via scp (236 chars) |

## Phase 6 — Verify (**YOU** triggers DAG in UI)

```bash
ssh nebius-mlops-assignment
cd ~/repos/mlops-assignment-e2e-ml-pipeline
bash docs/verify-setup.sh
```

Port-forward from laptop: `ssh -L 8080:localhost:8080 nebius-mlops-assignment`

## Phase 7 — Assignment

Begin `dags/evaluate_agent.py` after Phase 6 passes.
