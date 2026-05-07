#!/usr/bin/env bash
# deploy/kind/status.sh

set -euo pipefail

for ns in istio-system kafka restate default; do
  echo "==> Namespace: ${ns}"
  kubectl get pods -n "${ns}" 2>/dev/null || echo "    namespace not present"
  echo
done
