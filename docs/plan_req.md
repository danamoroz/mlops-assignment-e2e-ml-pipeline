# Setup Plan: Prerequisites (README §148–239)

**Context:** You work on a laptop. The assignment expects a **remote CPU VM** (8 vCPU, 32 GB RAM, public IP) as the execution environment—not your laptop directly.

**Short answer:** Yes—create a VM on Nebius, SSH in from your laptop, and do almost all setup and runtime work **on the VM**. Use the laptop for editing (Cursor/VS Code Remote SSH), git, and port-forwarding to Airflow/MLflow UIs.

---

## Architecture: Laptop vs VM

```text
┌─────────────────────────────┐         SSH / port-forward          ┌──────────────────────────────┐
│  Your laptop                │  ─────────────────────────────────► │  Nebius VM (8 CPU, 32 GB)    │
│  - Cursor / VS Code         │         :8080 → Airflow UI          │  - Docker                    │
│  - git clone (optional)     │         :5000 → MLflow (later)      │  - uv + Python venv          │
│  - ~/.ssh/config            │                                     │  - Airflow standalone        │
│  - NEBIUS_API_KEY in .env   │                                     │  - mini-swe-agent / SWE-bench│
└─────────────────────────────┘                                     │  - runs/ artifacts           │
                                                                    └──────────────────────────────┘
                                                                              │
                                                                              ▼
                                                                    Nebius managed inference APIs
                                                                    (no GPU VM required for orchestration)
```

| Where | What |
|-------|------|
| **Laptop** | Nebius console access, SSH client, optional local repo clone, IDE via Remote SSH, port forwarding |
| **Nebius VM** | Docker, Airflow, agent runs, SWE-bench eval, artifact storage under `runs/` |
| **Nebius APIs** | LLM inference for mini-swe-agent (via `NEBIUS_API_KEY`) |

You do **not** need a GPU VM—the README states inference is handled by managed APIs.

---

## Phase 0 — Prepare on your laptop (before creating the VM)

### 0.1 Accounts and credentials

- [ ] Nebius Academy / Nebius cloud account with access to create VMs
- [ ] **`NEBIUS_API_KEY`** from [Nebius Token Factory](https://tokenfactory.nebius.com/) (or course-provided instructions)
- [ ] SSH key pair on laptop (`~/.ssh/id_ed25519` or `id_rsa`); add **public** key when creating the VM

### 0.2 Optional: clone assignment repo locally

Useful if you want to browse code before the VM exists, or push changes from laptop:

```bash
git clone <repo-url>
cd <repo-folder>
```

You will **repeat** clone/setup on the VM (or `git pull` if you rsync/scp). The canonical runtime environment is the VM.

### 0.3 SSH config template (fill in after VM is created)

Add to `~/.ssh/config` on your laptop:

```sshconfig
Host nebius-mlops-assignment
  HostName <VM_PUBLIC_IP>
  User <your-username>
  ForwardAgent yes
```

Test: `ssh nebius-mlops-assignment`

---

## Phase 1 — Create the Nebius VM

### 1.1 VM spec (from README)

| Resource | Value |
|----------|-------|
| CPUs | 8 |
| RAM | 32 GB |
| GPU | **None** (CPU-only) |
| Network | Public IP |
| OS | Ubuntu (recommended; Docker install steps assume Ubuntu) |
| Access | Your SSH public key |

### 1.2 Checklist

- [ ] Create VM in Nebius console (or CLI if you use it)
- [ ] Attach / note **public IP**
- [ ] Confirm SSH login works: `ssh <user>@<public-ip>`
- [ ] Update laptop `~/.ssh/config` with real `HostName` and `User`

**Blocker if skipped:** Airflow + Docker + parallel SWE-bench workers will be slow or fail on a typical laptop; the assignment is designed for this VM shape.

---

## Phase 2 — Connect and baseline the VM

```bash
ssh nebius-mlops-assignment   # or ssh <user>@<ip>
```

- [ ] `uname -a` / `free -h` — confirm ~32 GB RAM visible
- [ ] `nproc` — confirm 8 CPUs
- [ ] Ensure disk space is sufficient (tens of GB free for Docker images, SWE-bench caches, `runs/`)

---

## Phase 3 — Install tooling on the VM

Run on the **VM**, not the laptop.

### 3.1 Install `uv` (Python package manager)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# Reload shell or source profile so `uv` is on PATH
```

### 3.2 Install Docker (Ubuntu)

Follow README steps:

```bash
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

### 3.3 Docker permissions (no sudo for docker)

```bash
sudo usermod -aG docker "$USER"
# Log out and back in, OR:
newgrp docker
docker ps   # should work without sudo
```

**Checklist**

- [ ] `uv --version`
- [ ] `docker --version`
- [ ] `docker compose version`
- [ ] `docker ps` works without `sudo`

---

## Phase 4 — Clone repositories on the VM

### 4.1 Assignment repo

```bash
git clone <repo-url>
cd <repo-folder>
cp .env.example .env
```

### 4.2 Reference repos (read-only context; not used as-is in final pipeline)

```bash
cd ..
git clone https://github.com/SWE-agent/mini-swe-agent.git
git clone https://github.com/swe-bench/SWE-bench.git
cd <repo-folder>
```

**Why clone these:** Understand trajectory format, prediction JSON, and evaluation harness output—not to wire them directly into production DAG.

---

## Phase 5 — Python environment and secrets

On the VM, inside `<repo-folder>`:

```bash
uv sync
source .venv/bin/activate
```

Edit `.env`:

```bash
NEBIUS_API_KEY=<your-real-key>
```

- [ ] `.env` exists and is **not** committed (already in `.gitignore`)
- [ ] Virtualenv activates cleanly

---

## Phase 6 — Verify setup (README “Check your setup”)

### 6.1 Smoke test: single-instance script

```bash
bash scripts/mini-swe-bench-single.sh
```

Expect: agent run + eval artifacts (may take several minutes; uses Nebius API).

- [ ] Script completes without credential errors
- [ ] Outputs look reasonable (compare with `sample/` in repo)

### 6.2 Smoke test: Airflow standalone

On the VM:

```bash
bash run-airflow-standalone.sh
```

From **laptop**, forward port 8080:

```bash
ssh -L 8080:localhost:8080 nebius-mlops-assignment
# or rely on VS Code/Cursor automatic forwarding if using Remote SSH
```

Open: [http://localhost:8080](http://localhost:8080)

- [ ] Airflow UI loads
- [ ] Trigger example DAG `mini-swe-bench-single`
- [ ] DAG run succeeds (green tasks)

**Default Airflow login** (standalone): often `admin` / `admin`—confirm in `run-airflow-standalone.sh` if different.

---

## Phase 7 — After prerequisites: assignment work (pointer)

Once Phase 6 passes, you are “all set” per README. Next implementation phases (from README §Suggested Implementation Path):

| Phase | Goal |
|-------|------|
| **1 — Speedrun DAG** | `dags/evaluate_agent.py`: `prepare_run` → `run_agent` → `run_eval` → `summarize_and_log`; params: `split`, `subset`, `workers`, optional `model`, `task_slice`, `run_id`, `cost_limit` |
| **2 — Durable runs** | Structured `runs/<run-id>/` + `manifest.json`; optional S3 upload |
| **3 — Production polish** | `DockerOperator`, `docker-compose.yaml` for Airflow + MLflow, retries/timeouts |

Deliverables: see README §Final Deliverables (`REPORT.md`, MLflow logging, sample `runs/<run-id>/`, screenshots).

---

## Recommended laptop workflow

1. **Create VM** in Nebius; keep it running during development.
2. **Cursor/VS Code Remote SSH** to the VM—edit `dags/`, `scripts/`, `src/` on the VM filesystem directly.
3. **Terminal on VM** (integrated or separate SSH) for `uv sync`, `bash scripts/...`, Airflow.
4. **Port-forward** 8080 (and later 5000 for MLflow) to use web UIs on localhost.
5. **Git**: commit from VM or laptop; avoid copying large `runs/` or Docker volumes—use `.gitignore` and MLflow/S3 for artifacts.

---

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Running everything on laptop | Use Nebius VM; laptop is only a thin client |
| Forgot `NEBIUS_API_KEY` | Agent calls fail early—check `.env` on VM |
| Docker permission denied | Re-login after `usermod -aG docker` or `newgrp docker` |
| Cannot reach Airflow UI | Ensure `ssh -L 8080:localhost:8080` while Airflow runs on VM |
| Out of disk on VM | Prune Docker (`docker system prune`), avoid committing large artifacts |
| VM IP changes | Update `~/.ssh/config` HostName |

---

## Master checklist (copy for tracking)

```
Phase 0  [ ] NEBIUS_API_KEY   [ ] SSH key   [ ] SSH config drafted
Phase 1  [ ] VM 8 CPU / 32 GB / public IP   [ ] SSH works
Phase 2  [ ] Connected   [ ] Resources OK
Phase 3  [ ] uv   [ ] Docker   [ ] docker without sudo
Phase 4  [ ] Assignment repo   [ ] mini-swe-agent   [ ] SWE-bench
Phase 5  [ ] uv sync   [ ] .env with API key
Phase 6  [ ] mini-swe-bench-single.sh   [ ] Airflow UI   [ ] mini-swe-bench-single DAG
Phase 7  [ ] Begin evaluate_agent DAG implementation
```

---

## Summary

**Yes—you should create a Nebius CPU VM and continue from there.** Your laptop is the control plane (SSH, IDE, browser via port-forward); the VM is the data plane (Docker, Airflow, agents, evaluations, `runs/`). Complete Phases 0–6 before building the configurable evaluation pipeline in Phase 7.
