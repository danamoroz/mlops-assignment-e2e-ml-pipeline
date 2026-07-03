#!/usr/bin/env bash
# Phases 3–5 — run ON the Nebius VM (Ubuntu).
# From laptop: ssh nebius-mlops-assignment 'bash -s' < docs/setup-vm.sh
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/danamoroz/mlops-assignment-e2e-ml-pipeline.git}"
REPO_NAME="${REPO_NAME:-mlops-assignment-e2e-ml-pipeline}"
WORK_DIR="${WORK_DIR:-${HOME}/repos}"

echo "==> Phase 2: baseline"
uname -a
echo "CPUs: $(nproc)"
free -h
df -h "${HOME}"

echo "==> Phase 3.1: install uv"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # shellcheck disable=SC1091
  [[ -f "${HOME}/.local/bin/env" ]] && source "${HOME}/.local/bin/env"
  export PATH="${HOME}/.local/bin:${PATH}"
fi
uv --version

echo "==> Phase 3.2: install Docker"
if ! command -v docker >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y ca-certificates curl
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc

  sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

echo "==> Phase 3.3: docker group"
if ! groups | grep -q '\bdocker\b'; then
  sudo usermod -aG docker "$USER"
  echo "Added $USER to docker group — run: newgrp docker   (or log out and back in)"
fi

docker --version
docker compose version

echo "==> Phase 4: clone repositories"
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

if [[ ! -d "${REPO_NAME}/.git" ]]; then
  git clone "$REPO_URL" "$REPO_NAME"
fi

if [[ ! -d mini-swe-agent/.git ]]; then
  git clone https://github.com/SWE-agent/mini-swe-agent.git
fi

if [[ ! -d SWE-bench/.git ]]; then
  git clone https://github.com/swe-bench/SWE-bench.git
fi

cd "${REPO_NAME}"

echo "==> Phase 5: Python env"
if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env — set NEBIUS_API_KEY on the VM before smoke tests."
fi

uv sync

echo ""
echo "Setup complete on VM. Repo: ${WORK_DIR}/${REPO_NAME}"
echo "Next on VM:"
echo "  cd ${WORK_DIR}/${REPO_NAME}"
echo "  source .venv/bin/activate"
echo "  # edit .env with NEBIUS_API_KEY"
echo "  bash docs/verify-setup.sh"
