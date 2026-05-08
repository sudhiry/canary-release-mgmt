#!/usr/bin/env bats
# tests/infra/smoke.bats

load helpers
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-canary-release-mgmt}"

@test "kind cluster is running" {
  assert_kind_cluster_running
}

@test "istiod is Ready" {
  wait_for_pod_ready istio-system "app=istiod" 60
}

@test "istio-ingressgateway is Ready" {
  wait_for_pod_ready istio-system "app=istio-ingressgateway" 60
}

@test "strimzi cluster operator is Ready" {
  wait_for_pod_ready kafka "name=strimzi-cluster-operator" 120
}

@test "kafka cluster reports Ready" {
  run kubectl -n kafka get kafka my-cluster \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
  [ "$status" -eq 0 ]
  [ "$output" = "True" ]
}

@test "restate server is Ready" {
  wait_for_pod_ready restate "app=restate" 120
}

@test "restate admin endpoint responds" {
  run kubectl -n restate exec restate-0 -- \
    curl -sf -o /dev/null -w '%{http_code}' http://localhost:9070/health
  [ "$status" -eq 0 ]
  [ "$output" = "200" ]
}
