#!/usr/bin/env bash
# deploy/kind/observability/install.sh
# Installs the Istio observability addons (Prometheus, Grafana, Kiali, Jaeger)
# from the matching Istio release. These are dev-grade single-replica deployments.

set -euo pipefail
: "${ISTIO_VERSION:?ISTIO_VERSION must be set}"

BASE_URL="https://raw.githubusercontent.com/istio/istio/${ISTIO_VERSION}/samples/addons"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_DIR="${SCRIPT_DIR}/dashboards"

echo "==> Installing Istio observability addons (${ISTIO_VERSION})"

for addon in prometheus grafana kiali jaeger; do
  echo "    applying ${addon}"
  kubectl apply -f "${BASE_URL}/${addon}.yaml"
done

# Wait for each addon Deployment to be Ready before returning.
for addon in prometheus grafana kiali jaeger; do
  kubectl -n istio-system rollout status "deployment/${addon}" --timeout=180s
done

echo "==> Applying canary dashboards ConfigMap"
TMP_CM="$(mktemp)"
trap 'rm -f "$TMP_CM"' EXIT
kubectl create configmap canary-dashboards \
  --namespace istio-system \
  --from-file="${DASHBOARD_DIR}/canary-overview.json" \
  --from-file="${DASHBOARD_DIR}/canary-substrates.json" \
  --from-file="${DASHBOARD_DIR}/canary-traces.json" \
  --dry-run=client -o yaml > "$TMP_CM"
kubectl apply -f "$TMP_CM"
kubectl label configmap -n istio-system canary-dashboards grafana_dashboard=1 --overwrite

echo "==> Observability addons + dashboards Ready"
