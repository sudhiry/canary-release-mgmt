#!/usr/bin/env bash
# deploy/kind/teardown.sh

set -euo pipefail
: "${KIND_CLUSTER_NAME:?KIND_CLUSTER_NAME must be set}"

echo "==> Deleting kind cluster: ${KIND_CLUSTER_NAME}"
if kind get clusters | grep -qx "${KIND_CLUSTER_NAME}"; then
  kind delete cluster --name "${KIND_CLUSTER_NAME}"
else
  echo "    cluster does not exist; nothing to do"
fi
