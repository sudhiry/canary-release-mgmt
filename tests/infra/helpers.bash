# tests/infra/helpers.bash
# Shared assertions for infra smoke tests.

# wait_for_pod_ready <namespace> <label-selector> <timeout-seconds>
wait_for_pod_ready() {
  local ns="$1"
  local selector="$2"
  local timeout="${3:-180}"
  kubectl -n "$ns" wait --for=condition=Ready \
    --selector="$selector" --timeout="${timeout}s" pod
}

# assert_kind_cluster_running
assert_kind_cluster_running() {
  kubectl cluster-info --context "kind-${KIND_CLUSTER_NAME}" >/dev/null 2>&1
}
