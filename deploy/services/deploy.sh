#!/usr/bin/env bash
# Deploy the 5 domain services + KafkaTopics + Istio routing to the kind cluster.
# Idempotent: re-running upgrades existing releases.

set -euo pipefail

SERVICES=(audit-service payment-service inventory-service notification-service order-service)
NAMESPACE="services"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> 1. Create services namespace with istio-injection label"
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"
kubectl label namespace "$NAMESPACE" istio-injection=enabled --overwrite

echo "==> 2. Apply KafkaTopic CRDs"
kubectl apply -f "${REPO_ROOT}/deploy/kafka/topics/"
echo "    waiting for KafkaTopics to be Ready..."
kubectl wait --for=condition=Ready --timeout=60s -n kafka kafkatopics --all

echo "==> 3. Helm install/upgrade all 5 services"
for svc in "${SERVICES[@]}"; do
  echo "    --- $svc ---"
  helm upgrade --install "$svc" "${REPO_ROOT}/deploy/helm/service-chart" \
    -f "${REPO_ROOT}/deploy/helm/values/${svc}.yaml" \
    -n "$NAMESPACE" \
    --wait --timeout 3m
done

echo "==> 4. Wait for stable Deployments to be Available"
kubectl wait --for=condition=Available --timeout=180s -n "$NAMESPACE" deployment --all

echo "==> 5. Apply routing config (DestinationRules + default-only VirtualServices)"
kubectl apply -f "${REPO_ROOT}/deploy/routing/destination-rules/"
kubectl apply -f "${REPO_ROOT}/deploy/routing/virtual-services/"

echo "==> 6. Apply edge ingress (Gateway + edge VirtualService)"
kubectl apply -f "${REPO_ROOT}/deploy/routing/ingress/"

echo "==> deploy-services complete"
