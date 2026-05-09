# tests/canary/helpers.bash
# Shared helpers for canary-ctl smoke tests.

CANARY_CTL="node tools/canary-ctl/bin/canary-ctl"
TRAFFIC_CLI="node tools/traffic-cli/bin/traffic-cli"
STATE_DIR="${BATS_TMPDIR:-/tmp}/canary-ctl-state"

setup_canary_state_dir() {
  rm -rf "$STATE_DIR"
  mkdir -p "$STATE_DIR"
}

teardown_canary_state_dir() {
  rm -rf "$STATE_DIR"
}

canary_ctl() {
  $CANARY_CTL --state-dir "$STATE_DIR" --repo-root "$PWD" "$@"
}
