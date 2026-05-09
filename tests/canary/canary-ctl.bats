#!/usr/bin/env bats
# tests/canary/canary-ctl.bats
# Smoke test: full canary lifecycle against a real kind cluster.
# Pre-req: make up && make build-services && make build-images && make load-images && make deploy-services

load helpers

setup_file() {
  setup_canary_state_dir
  # Build the tools (idempotent if already built).
  pnpm --filter @canary/canary-ctl build >/dev/null
  pnpm --filter @canary/traffic-cli build >/dev/null
}

teardown_file() {
  # Best-effort cleanup in case a test left state behind.
  canary_ctl rollback payment-service >/dev/null 2>&1 || true
  teardown_canary_state_dir
}

@test "status on a clean cluster reports no canary" {
  run canary_ctl status payment-service
  [ "$status" -eq 0 ]
  [[ "$output" == *"state file: absent"* ]]
  [[ "$output" == *"helm release payment-service-canary: absent"* ]]
  [[ "$output" == *"drift: none"* ]]
}

@test "deploy-canary payment-service dev succeeds, header rule applied, state=active" {
  run canary_ctl deploy-canary payment-service dev
  [ "$status" -eq 0 ]

  # State file written.
  run canary_ctl status payment-service
  [ "$status" -eq 0 ]
  [[ "$output" == *"state file: active"* ]]
  [[ "$output" == *"virtualservice header rule: present"* ]]
  [[ "$output" == *"drift: none"* ]]
}

@test "traffic-cli order --canary returns 2xx end-to-end" {
  run $TRAFFIC_CLI order --canary --user u-smoke
  [ "$status" -eq 0 ]
  [[ "$output" == *"\"status\": 2"* ]]  # status 2xx
}

@test "rollback removes header rule, uninstalls release, clears state" {
  run canary_ctl rollback payment-service
  [ "$status" -eq 0 ]

  run canary_ctl status payment-service
  [ "$status" -eq 0 ]
  [[ "$output" == *"state file: absent"* ]]
  [[ "$output" == *"helm release payment-service-canary: absent"* ]]
  [[ "$output" == *"drift: none"* ]]
}

@test "deploy-canary with bad image tag auto-rolls back, status clean, exit nonzero" {
  run canary_ctl deploy-canary payment-service nope-tag-does-not-exist
  [ "$status" -ne 0 ]

  run canary_ctl status payment-service
  [ "$status" -eq 0 ]
  [[ "$output" == *"state file: absent"* ]]
  [[ "$output" == *"helm release payment-service-canary: absent"* ]]
  [[ "$output" == *"drift: none"* ]]
}
