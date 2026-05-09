#!/usr/bin/env bats
# Smoke test for Plan 1.3.b deployment.
# Prerequisites (the user runs in order):
#   make up                  # 1.1 cluster + Istio + Strimzi + Restate
#   make build-services      # 1.3.a compile (Java + Node)
#   make build-images        # 1.3.b docker build
#   make load-images         # 1.3.b kind load
#   make deploy-services     # 1.3.b helm install + routing

SERVICES="audit-service payment-service inventory-service notification-service order-service"
TOPICS="audit.events inventory.events notifications.events orders.events payments.events"

@test "all 5 KafkaTopics are Ready" {
  for topic in $TOPICS; do
    run kubectl get -n kafka kafkatopic "$topic" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
    [ "$status" -eq 0 ]
    [ "$output" = "True" ] || { echo "topic $topic not Ready: $output" >&3; false; }
  done
}

@test "all 5 stable Deployments are Available" {
  for svc in $SERVICES; do
    run kubectl get -n services deployment "${svc}-stable" -o jsonpath='{.status.conditions[?(@.type=="Available")].status}'
    [ "$status" -eq 0 ]
    [ "$output" = "True" ] || { echo "deployment ${svc}-stable not Available: $output" >&3; false; }
  done
}

@test "all 5 Services have at least one endpoint" {
  # Use the EndpointSlice API (v1 Endpoints is deprecated in 1.33+, prints a
  # warning to stderr that pollutes parsed output). Suppress kubectl stderr
  # to keep the IP-count parse clean.
  for svc in $SERVICES; do
    run bash -c "kubectl get -n services endpointslices.discovery.k8s.io -l kubernetes.io/service-name='$svc' -o jsonpath='{.items[*].endpoints[*].addresses[*]}' 2>/dev/null | wc -w | tr -d ' '"
    [ "$status" -eq 0 ]
    [ "$output" -ge 1 ] || { echo "service $svc has no endpoints (got: '$output')" >&3; false; }
  done
}

@test "Restate Admin reports 5 deployments registered" {
  # kind exposes Restate admin on host port 9070 (1.1's NodePort mapping)
  run bash -c "curl -sf http://localhost:9070/deployments | jq '.deployments | length'"
  [ "$status" -eq 0 ]
  [ "$output" = "5" ] || { echo "expected 5 Restate deployments, got: $output" >&3; false; }
}

@test "POST /api/orders via Istio Ingress returns 2xx with an order id" {
  # kind exposes Istio ingress on host port 8080 (1.1's NodePort mapping for ingressgateway)
  run bash -c "curl -sf -X POST -H 'content-type: application/json' \
    -d '{\"userId\":\"u1\",\"sku\":\"sku-1\",\"quantity\":1,\"amount\":100}' \
    http://localhost:8080/api/orders | jq -r '.id'"
  [ "$status" -eq 0 ]
  [ -n "$output" ] && [ "$output" != "null" ] || { echo "no order id in response: $output" >&3; false; }
}
