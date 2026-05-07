#!/usr/bin/env bats
# tests/infra/smoke.bats

load helpers
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-canary-release-mgmt}"

@test "kind cluster is running" {
  assert_kind_cluster_running
}
