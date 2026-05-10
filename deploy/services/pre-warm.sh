#!/usr/bin/env bash
# Pre-warm Kafka topics by sending baseline (non-canary) orders through the
# saga. This is a workaround for the Phase 2.b cold-cluster boot deadlock:
# canary pods' readiness probe is gated on KafkaConsumerHealthIndicator
# (Java) / createKafkaHealthState (Node), which only flips to UP after
# `recordPoll` fires — and recordPoll only fires when a real Kafka message
# is delivered.
#
# On a fully cold cluster with no traffic, a freshly-deployed canary pod
# never reaches Ready=true → never enters service endpoints → stable's
# pod-watch never sees `canaryReady=true` → events with x-canary header
# fall through to stable as if no canary existed.
#
# Sending 2-3 baseline orders here flows messages through orders.events,
# payments.events, inventory.events, and notifications.events, which is
# enough to satisfy every consumer's recordPoll ahead of any canary deploy.

set -euo pipefail

URL="${PRE_WARM_URL:-http://localhost:8080}"
COUNT="${PRE_WARM_COUNT:-3}"
DELAY_MS="${PRE_WARM_DELAY_MS:-500}"

echo "==> Pre-warming Kafka topics via ${URL} (${COUNT} baseline orders)"
for i in $(seq 1 "$COUNT"); do
  echo "    --- order $i/$COUNT ---"
  status=$(curl -s -o /dev/null -w "%{http_code}" -m 10 \
    -X POST "${URL}/api/orders" \
    -H 'content-type: application/json' \
    -d "{\"userId\":\"warmup-$i\",\"sku\":\"sku-1\",\"quantity\":1,\"amount\":100}")
  if [ "$status" -lt "200" ] || [ "$status" -ge "300" ]; then
    echo "    !! warmup order $i failed: HTTP $status" >&2
    exit 1
  fi
  echo "    HTTP $status"
  if [ "$i" -lt "$COUNT" ]; then
    sleep "$(awk "BEGIN { print $DELAY_MS / 1000 }")"
  fi
done

echo "==> pre-warm complete; canary pods can now be deployed safely"
