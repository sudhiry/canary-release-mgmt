#!/usr/bin/env bash
# deploy/kind/observability/install.sh
# Installs the Istio observability addons (Prometheus, Grafana, Kiali, Jaeger)
# from the matching Istio release. These are dev-grade single-replica deployments.

set -euo pipefail
: "${ISTIO_VERSION:?ISTIO_VERSION must be set}"

BASE_URL="https://raw.githubusercontent.com/istio/istio/${ISTIO_VERSION}/samples/addons"

echo "==> Installing Istio observability addons (${ISTIO_VERSION})"

for addon in prometheus grafana kiali jaeger; do
  echo "    applying ${addon}"
  kubectl apply -f "${BASE_URL}/${addon}.yaml"
done

# Wait for each addon Deployment to be Ready before returning.
for addon in prometheus grafana kiali jaeger; do
  kubectl -n istio-system rollout status "deployment/${addon}" --timeout=180s
done

echo "==> Observability addons Ready"
