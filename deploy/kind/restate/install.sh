#!/usr/bin/env bash
# deploy/kind/restate/install.sh
# Installs the Restate single-replica StatefulSet, services, and namespace.
# Stamps RESTATE_VERSION (from Makefile) into the StatefulSet image tag.

set -euo pipefail

: "${RESTATE_VERSION:?RESTATE_VERSION must be set}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Installing Restate"

kubectl apply -f "${SCRIPT_DIR}/namespace.yaml"
kubectl apply -f "${SCRIPT_DIR}/service.yaml"
sed "s/RESTATE_VERSION_PLACEHOLDER/${RESTATE_VERSION}/g" "${SCRIPT_DIR}/statefulset.yaml" | kubectl apply -f -

# Wait for the StatefulSet to be Ready (rollout status reports completion).
kubectl -n restate rollout status statefulset/restate --timeout=180s

echo "==> Restate Ready"
