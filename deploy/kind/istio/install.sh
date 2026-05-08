#!/usr/bin/env bash
# deploy/kind/istio/install.sh
# Installs Istio with the IstioOperator config in iop-config.yaml.

set -euo pipefail
: "${ISTIO_VERSION:?ISTIO_VERSION must be set}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Installing Istio ${ISTIO_VERSION}"

# Idempotent: only install if istiod doesn't already exist.
if kubectl -n istio-system get deployment istiod >/dev/null 2>&1; then
  echo "    istiod already present; skipping install"
else
  istioctl install -f "${SCRIPT_DIR}/iop-config.yaml" -y
fi

# Label the default namespace for sidecar injection (services will land here in Plan 1.3).
kubectl label namespace default istio-injection=enabled --overwrite

echo "==> Istio installed"
