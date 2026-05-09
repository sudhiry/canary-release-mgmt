#!/usr/bin/env bash
# Build all 5 service images and (optionally) load them into the kind cluster's image cache.
# Usage:
#   build-and-load.sh build          # docker build only
#   build-and-load.sh load           # kind load only (assumes images exist)
#   build-and-load.sh all            # build + load

set -euo pipefail

SERVICES=(audit-service payment-service inventory-service order-service notification-service)
REPO="canary-release-mgmt"
TAG="dev"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

build_one() {
  local svc="$1"
  echo "==> Building image: ${REPO}/${svc}:${TAG}"
  docker build -f "${REPO_ROOT}/services/${svc}/Dockerfile" -t "${REPO}/${svc}:${TAG}" "${REPO_ROOT}"
}

load_one() {
  local svc="$1"
  : "${KIND_CLUSTER_NAME:?KIND_CLUSTER_NAME must be set (export from Makefile)}"
  echo "==> Loading into kind cluster '${KIND_CLUSTER_NAME}': ${REPO}/${svc}:${TAG}"
  kind load docker-image "${REPO}/${svc}:${TAG}" --name "${KIND_CLUSTER_NAME}"
}

cmd="${1:-all}"

case "$cmd" in
  build)
    for svc in "${SERVICES[@]}"; do build_one "$svc"; done
    ;;
  load)
    for svc in "${SERVICES[@]}"; do load_one "$svc"; done
    ;;
  all)
    for svc in "${SERVICES[@]}"; do build_one "$svc"; done
    for svc in "${SERVICES[@]}"; do load_one "$svc"; done
    ;;
  *)
    echo "Usage: $0 {build|load|all}" >&2
    exit 2
    ;;
esac

echo "==> $cmd complete"
