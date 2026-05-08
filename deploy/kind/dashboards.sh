#!/usr/bin/env bash
# deploy/kind/dashboards.sh
# Manages kubectl port-forwards for the Istio observability dashboards.
# Usage: dashboards.sh {start|stop|status}
#
# Each dashboard runs as a background kubectl port-forward whose PID is
# tracked in .dashboards/<name>.pid (in the project root, gitignored).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PID_DIR="${REPO_ROOT}/.dashboards"
mkdir -p "$PID_DIR"

# name | service | local:remote | url
DASHBOARDS=(
  "kiali|svc/kiali|20001:20001|http://localhost:20001"
  "grafana|svc/grafana|3000:3000|http://localhost:3000"
  "prometheus|svc/prometheus|9090:9090|http://localhost:9090"
  "jaeger|svc/tracing|16686:80|http://localhost:16686"
)

start_one() {
  local name="$1" svc="$2" ports="$3" url="$4"
  local pidfile="$PID_DIR/$name.pid"
  local logfile="$PID_DIR/$name.log"

  if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    printf "    %-12s already running (pid %s) -> %s\n" "$name" "$(cat "$pidfile")" "$url"
    return 0
  fi

  : > "$logfile"
  nohup kubectl -n istio-system port-forward "$svc" "$ports" >>"$logfile" 2>&1 &
  local pid=$!
  echo "$pid" > "$pidfile"

  # Brief settle time so we can detect immediate failures.
  sleep 1
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pidfile"
    printf "    %-12s FAILED to start (see %s)\n" "$name" "$logfile"
    return 1
  fi
  printf "    %-12s started (pid %s) -> %s\n" "$name" "$pid" "$url"
}

stop_one() {
  local name="$1"
  local pidfile="$PID_DIR/$name.pid"

  if [[ ! -f "$pidfile" ]]; then
    printf "    %-12s not running\n" "$name"
    return 0
  fi
  local pid
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    printf "    %-12s stopped (was pid %s)\n" "$name" "$pid"
  else
    printf "    %-12s pid %s already gone\n" "$name" "$pid"
  fi
  rm -f "$pidfile"
}

status_one() {
  local name="$1" url="$4"
  local pidfile="$PID_DIR/$name.pid"
  if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    printf "    %-12s running (pid %s) -> %s\n" "$name" "$(cat "$pidfile")" "$url"
  else
    printf "    %-12s not running\n" "$name"
  fi
}

cmd="${1:-start}"
case "$cmd" in
  start)
    echo "==> Starting dashboard port-forwards"
    for entry in "${DASHBOARDS[@]}"; do
      IFS='|' read -r name svc ports url <<< "$entry"
      start_one "$name" "$svc" "$ports" "$url"
    done
    echo
    echo "Logs in $PID_DIR/<name>.log. Stop with: make dashboards-stop"
    ;;
  stop)
    echo "==> Stopping dashboard port-forwards"
    for entry in "${DASHBOARDS[@]}"; do
      IFS='|' read -r name svc ports url <<< "$entry"
      stop_one "$name"
    done
    ;;
  status)
    echo "==> Dashboard port-forward status"
    for entry in "${DASHBOARDS[@]}"; do
      IFS='|' read -r name svc ports url <<< "$entry"
      status_one "$name" "$svc" "$ports" "$url"
    done
    ;;
  *)
    echo "Usage: $0 {start|stop|status}" >&2
    exit 1
    ;;
esac
