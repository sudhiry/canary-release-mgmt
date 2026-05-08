#!/usr/bin/env bash
# deploy/kind/restate/install.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Installing Restate"

kubectl apply -f "${SCRIPT_DIR}/namespace.yaml"
kubectl apply -f "${SCRIPT_DIR}/service.yaml"
kubectl apply -f "${SCRIPT_DIR}/statefulset.yaml"

# Wait for the StatefulSet to be Ready (rollout status reports completion).
kubectl -n restate rollout status statefulset/restate --timeout=180s

echo "==> Restate Ready"
