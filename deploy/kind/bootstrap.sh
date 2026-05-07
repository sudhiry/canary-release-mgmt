#!/usr/bin/env bash
# deploy/kind/bootstrap.sh
# Bootstraps the local cluster. Idempotent: re-running on an existing cluster is a no-op for cluster creation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${KIND_CLUSTER_NAME:?KIND_CLUSTER_NAME must be set}"

echo "==> Creating kind cluster: ${KIND_CLUSTER_NAME}"
if kind get clusters | grep -qx "${KIND_CLUSTER_NAME}"; then
  echo "    cluster already exists; skipping create"
else
  kind create cluster --config "${SCRIPT_DIR}/cluster-config.yaml" --name "${KIND_CLUSTER_NAME}" --wait 120s
fi

kubectl config use-context "kind-${KIND_CLUSTER_NAME}"
kubectl cluster-info

echo "==> Cluster ready"

echo "==> Installing Istio"
bash "${SCRIPT_DIR}/istio/install.sh"
