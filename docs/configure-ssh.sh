#!/usr/bin/env bash
# Add Nebius VM entry to ~/.ssh/config (run on laptop after VM is created).
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <VM_PUBLIC_IP> <VM_USERNAME> [identity_file]"
  echo "Example: $0 89.169.100.8 danar ~/.ssh/id_ed25519_nebius"
  exit 1
fi

VM_IP="$1"
VM_USER="$2"
IDENTITY_FILE="${3:-${HOME}/.ssh/id_ed25519_nebius}"
SSH_CONFIG="${HOME}/.ssh/config"
HOST_ALIAS="nebius-mlops-assignment"

mkdir -p "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"
touch "$SSH_CONFIG"
chmod 600 "$SSH_CONFIG"

if grep -q "^Host ${HOST_ALIAS}$" "$SSH_CONFIG" 2>/dev/null; then
  echo "Host ${HOST_ALIAS} already in ${SSH_CONFIG} — remove it first or edit manually."
  exit 1
fi

cat >>"$SSH_CONFIG" <<EOF

Host ${HOST_ALIAS}
  HostName ${VM_IP}
  User ${VM_USER}
  IdentityFile ${IDENTITY_FILE}
  ForwardAgent yes
EOF

echo "Added ${HOST_ALIAS} -> ${VM_USER}@${VM_IP}"
echo "Test: ssh ${HOST_ALIAS}"
