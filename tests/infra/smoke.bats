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
