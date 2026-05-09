#!/usr/bin/env bash
# Inverse of deploy.sh: removes routing, helm releases, and topics.
# Leaves the services namespace in place for fast re-deploy.

set -euo pipefail

SERVICES=(audit-service payment-service inventory-service notification-service order-service)
NAMESPACE="services"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> 1. Delete edge ingress"
kubectl delete --ignore-not-found -f "${REPO_ROOT}/deploy/routing/ingress/"

echo "==> 2. Delete routing config"
kubectl delete --ignore-not-found -f "${REPO_ROOT}/deploy/routing/virtual-services/"
kubectl delete --ignore-not-found -f "${REPO_ROOT}/deploy/routing/destination-rules/"

echo "==> 3. Helm uninstall all 5 services"
for svc in "${SERVICES[@]}"; do
  if helm status "$svc" -n "$NAMESPACE" >/dev/null 2>&1; then
    helm uninstall "$svc" -n "$NAMESPACE"
  fi
done

echo "==> 4. Delete KafkaTopics"
kubectl delete --ignore-not-found -f "${REPO_ROOT}/deploy/kafka/topics/"

echo "==> undeploy-services complete (services namespace preserved)"
