#!/usr/bin/env bash
# Phase 0 — laptop prep (run on your laptop / WSL, not on the Nebius VM).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Phase 0: laptop prep"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example — add your NEBIUS_API_KEY before running agents."
else
  echo ".env already exists"
fi

if command -v uv >/dev/null 2>&1; then
  echo "==> uv sync (optional local browse; canonical runtime is the VM)"
  uv sync
else
  echo "uv not installed locally — skip, or install: curl -LsSf https://astral.sh/uv/install.sh | sh"
fi

PUB_KEY="${HOME}/.ssh/id_ed25519_nebius.pub"
if [[ -f "$PUB_KEY" ]]; then
  echo ""
  echo "SSH public key for Nebius VM (paste when creating the VM):"
  cat "$PUB_KEY"
else
  echo "No ~/.ssh/id_ed25519_nebius.pub — generate one:"
  echo "  ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_nebius -C \"danar-nebius\""
fi

echo ""
echo "Next: create the Nebius VM (Phase 1), then run:"
echo "  bash docs/configure-ssh.sh <VM_PUBLIC_IP> <VM_USERNAME>"
echo "  ssh nebius-mlops-assignment 'bash -s' < docs/setup-vm.sh"
