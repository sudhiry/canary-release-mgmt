#!/usr/bin/env bash
# Optional: seeds consumer offsets so e2e suites can measure lag from a
# known baseline (lag=0). With heartbeat-based readiness, canary deploys
# no longer require this.
#
# Sends a few baseline (non-canary) orders through the saga, flowing
# messages through orders.events, payments.events, inventory.events, and
# notifications.events so every consumer group commits offsets up to the
# current log-end before an e2e run begins.

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

echo "==> pre-warm complete; consumer groups now at lag=0"
