# Canary Release Management — Cold-cluster pre-warm fix (heartbeat-based Kafka readiness)

**Status:** Approved (awaiting user review)
**Date:** 2026-05-10
**Predecessor:** Phase 2.b (Kafka canary integration) — merged 2026-05-10 (commit 2b068eb)
**Related history:** [docs/history.md](../../history.md) Item 1 ("Cold-cluster pre-warm" workaround, commit 295c3f6)

## Project context

After Phase 2.b cluster verification, five issues surfaced (Items 1–5 in `docs/history.md`). Items 2–5 were fixed at the root cause; Item 1 ("cold-cluster pre-warm") shipped only as a documented workaround: `make pre-warm` sends 3 baseline orders before the first `canary-deploy`, because canary readiness gates on `kafkaConsumer` health, which today goes UP only after the first delivered message — and on a fresh cluster with no traffic, no message is ever delivered, so `helm install --wait` deadlocks.

This spec fixes Item 1 at the root cause: replace the "first poll received" liveness signal with "consumer joined + heartbeat fresh."

## Locked decisions (from brainstorming)

The following are settled before this design and not revisited:

- **Signal:** membership + heartbeat freshness (not membership-only). Membership-only would break K5 because a `SIGSTOP`'d local consumer's `assigned=true` flag stays true even after the broker has expelled it from the group.
- **Threshold:** `15s` (5 missed heartbeats at the default `heartbeat.interval.ms=3000`).
- **`make pre-warm`:** kept but de-emphasized. Drop "⚠ REQUIRED" framing and the post-deploy reminder; reframe as optional e2e setup ("seeds consumer offsets to lag=0 for traffic-shape testing").
- **Stable readiness:** unchanged (Option 1 from brainstorming). Asymmetry preserved — stable uses `readinessState` only; canary keeps `readinessState + kafkaConsumer`. Re-gating stable would convert Kafka outages into full-service outages with no covering acceptance scenario; out of scope here.
- **Env-var/property naming:** rename to reflect the new semantic; honor old names as deprecated aliases. Alias removal is a follow-up tracked separately.
  - Java: `canary.kafka-health-timeout-ms` → `canary.kafka-heartbeat-stale-ms`
  - Node: `KAFKA_HEALTH_TIMEOUT_MS` → `KAFKA_HEARTBEAT_STALE_MS`

## Goals

1. Eliminate the cold-cluster `canary-deploy` deadlock at the root cause: a brand-new canary pod must reach Ready inside `helm install --wait`'s 3-min timeout on a cluster with zero Kafka traffic.
2. Preserve the K5 contract: a `SIGSTOP`'d canary still flips to NotReady within the existing test budget (15s detection + kubelet probe failureThreshold × periodSeconds + watch propagation; comfortably inside the test's 60s wait).
3. Use the right liveness signal for what the probe is actually trying to detect: "is the consumer's Kafka client thread alive?" — not "have we recently received a message?"

## Non-goals

- Re-gating stable readiness on Kafka (Option 1 — see Locked decisions).
- Removing `make pre-warm` entirely (kept as optional e2e helper).
- Reworking the presence watcher; it observes readiness, which is the thing being fixed.
- Changing the canary-flagged-event routing rules, header propagation, consumer-group-ID resolution, or any Phase 2 contract.
- Schema evolution / Phase 2.c.

## Architecture

Two libraries change. Five services consume the new signal via dependency injection. No service business logic changes. No Helm chart changes. Documentation updated to remove the cold-cluster warning.

### Component map

| Layer | File(s) | Change |
|---|---|---|
| Java lib | `platform/lib-java/.../KafkaConsumerHealthIndicator.java` | New state machine: `assigned` flag + `lastHeartbeatMs` from kafka-clients metric |
| Java lib | `platform/lib-java/.../autoconfigure/XCanaryAutoConfiguration.java` | Register `ApplicationListener` for partition-assigned/revoked events; wire heartbeat-stale property with deprecated alias |
| Java services | `audit-service`, `payment-service`, `inventory-service` `*KafkaListener.onMessage` | Delete `health.recordPoll()` line (library handles it) |
| Node lib | `platform/lib-node/src/kafka-consumer-health.ts` | New API: `markAssigned/markRevoked/recordHeartbeat`; remove `recordPoll` |
| Node services | `order-service`, `notification-service` `kafka.ts` | Subscribe to `consumer.events.HEARTBEAT/GROUP_JOIN/REBALANCING`; remove `recordPoll` from `eachMessage` |
| Node services | `order-service`, `notification-service` `config.ts` | Honor old `KAFKA_HEALTH_TIMEOUT_MS` with deprecation warn |
| Docs | `README.md`, `docs/operations.md`, `docs/canary-mechanics.md`, `docs/development.md`, `docs/history.md` | Drop cold-cluster warnings; reframe pre-warm; record this fix |
| Tooling | `Makefile` (`deploy-services` target) | Drop the post-deploy "remember to run `make pre-warm`" reminder |
| E2E | `tests/e2e/k6-cold-cluster-bringup.test.ts` (new) | Cold-cluster canary deploy without pre-warm; helm timeout assertion |

### Java probe — KafkaConsumerHealthIndicator

```java
public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final long heartbeatStaleMs;
    private final AtomicBoolean assigned = new AtomicBoolean(false);
    private final Supplier<OptionalLong> lastHeartbeatMsSupplier;
    // (one supplier per registered consumer; the indicator picks the freshest)

    public void onPartitionsAssigned() { assigned.set(true); }
    public void onPartitionsRevoked()  { assigned.set(false); }

    @Override
    public Health health() {
        if (!assigned.get()) {
            return Health.outOfService().withDetail("reason", "no partitions assigned").build();
        }
        OptionalLong lastHbMs = lastHeartbeatMsSupplier.get();
        if (lastHbMs.isEmpty()) {
            return Health.outOfService().withDetail("reason", "no heartbeat yet").build();
        }
        long ageMs = System.currentTimeMillis() - lastHbMs.getAsLong();
        if (ageMs > heartbeatStaleMs) {
            return Health.outOfService().withDetail("heartbeatStaleMs", ageMs).build();
        }
        return Health.up().withDetail("heartbeatAgeMs", ageMs).build();
    }
}
```

The `lastHeartbeatMsSupplier` reads the kafka-clients consumer metric `last-heartbeat-seconds-ago` (confirmed present in kafka-clients 4.1.2). Wiring lives in `XCanaryAutoConfiguration` and uses Spring Kafka's `KafkaListenerEndpointRegistry` to enumerate active consumers.

The `assigned` flag is updated by an `ApplicationListener<ConsumerPartitionsAssignedEvent>` and `ApplicationListener<ConsumerPartitionsRevokedEvent>`. Both events are emitted by Spring Kafka's `MessageListenerContainer` when the underlying kafka-clients consumer's `ConsumerRebalanceListener` fires.

### Node probe — kafka-consumer-health.ts

```ts
export interface KafkaHealthState {
  markAssigned(): void;
  markRevoked(): void;
  recordHeartbeat(): void;
  isHealthy(): boolean;
  report(): KafkaHealthReport;
}

export function createKafkaHealthState(heartbeatStaleMs: number = 15_000): KafkaHealthState {
  let assigned = false;
  let lastHeartbeatMs = 0;
  return {
    markAssigned() { assigned = true; },
    markRevoked()  { assigned = false; },
    recordHeartbeat() { lastHeartbeatMs = Date.now(); },
    isHealthy() {
      if (!assigned) return false;
      if (lastHeartbeatMs === 0) return false;
      return Date.now() - lastHeartbeatMs <= heartbeatStaleMs;
    },
    report() { /* parallel to isHealthy with reason strings */ },
  };
}
```

Service-level wiring in `kafka.ts`:

```ts
const c = kafka.consumer({ groupId });
c.on(c.events.GROUP_JOIN,    () => health.markAssigned());
c.on(c.events.REBALANCING,   () => health.markRevoked());
c.on(c.events.HEARTBEAT,     () => health.recordHeartbeat());
c.on(c.events.DISCONNECT,    () => health.markRevoked());
// eachMessage no longer calls health.recordPoll(); the runner
// already polls and the heartbeat thread is what we care about.
```

### Configuration

| Old name | New name | Default | Alias support |
|---|---|---|---|
| `canary.kafka-health-timeout-ms` (Java) | `canary.kafka-heartbeat-stale-ms` | `15000` | Old name read with deprecated-config log on startup; alias removal is a follow-up tracked separately (not part of this PR) |
| `KAFKA_HEALTH_TIMEOUT_MS` (Node) | `KAFKA_HEARTBEAT_STALE_MS` | `15000` | Old env var read in `config.ts` with `console.warn`; alias removal is a follow-up tracked separately (not part of this PR) |

The default drops from `30000` to `15000`. K5 still passes (15s heartbeat-staleness + 15s kubelet probe failureThreshold × periodSeconds + ~1s watch propagation ≈ 31s, well inside the test's 60s wait + 30s downstream assertion).

## Data flow

### Cold-cluster boot (the case being fixed)

```
make deploy-services
  → helm install order-service-stable --wait
    → stable pod starts → readiness = readinessState only (Item 4)
    → readiness 200 within 5–10s → helm wait succeeds

make canary-deploy SVC=audit-service
  → helm install audit-service-canary --wait
    → canary pod starts → kafka.consumer().connect()
    → JoinGroup → SyncGroup → onPartitionsAssigned fires
    → assigned=true; heartbeat thread starts; emits within 3s
    → indicator: assigned && heartbeatAgeMs<15000 → UP
    → readiness 200 within ~5–10s → helm wait succeeds
    → no pre-warm needed
```

### K5 (canary Kafka unhealthy → stable takeover)

```
sendSignalToPod(canaryPod, "STOP")
  → kafka-clients heartbeat thread frozen on canary
  → after 15s, indicator: heartbeatStaleMs > 15000 → DOWN
  → readiness 503
  → kubelet probe failureThreshold=3 × periodSeconds=5 = 15s
  → endpointslices update; presence watcher on stable fires
  → next flagged event: stable's filter sees canaryReady=false → processes
```

Total detection time: ~15s (heartbeat) + ~15s (kubelet) + ~1s (watch) = ~31s. K5 test waits 60s before sending the next flagged event; assertion has a 30s waitForConsumed budget. Comfortable.

## Error handling

- **No heartbeat yet at startup.** `lastHeartbeatMs == 0` → DOWN with reason `"no heartbeat yet"`. Mirrors today's `"no poll yet"` initial state. Expected to clear within 3–5s of `GROUP_JOIN`.
- **Consumer disconnected (kafkajs `DISCONNECT` / Java `ConsumerStoppedEvent`).** `markRevoked()` / `assigned=false` → DOWN.
- **Broker unreachable on startup.** Existing background-retry behavior in `setupKafka` / Spring Kafka container preserved. `assigned=false` keeps readiness DOWN until connectivity returns. No regression vs. today.
- **Brief rebalance during normal operation.** `REBALANCING` event flips `assigned=false` for the duration; heartbeats continue; once `GROUP_JOIN` fires again, back to UP. Window is typically <5s — well inside kubelet probe failureThreshold (3 × 5s = 15s) so a healthy rebalance does NOT flap readiness in practice.
- **Kafka metric supplier returns empty mid-life (Java).** Treated as DOWN with `"no heartbeat yet"`. Indicates the consumer was unregistered from the listener registry; same effective handling as `assigned=false`.

## Testing

### Library unit tests

`platform/lib-java/.../KafkaConsumerHealthIndicatorTest.java` — replace existing tests with the full state machine:
- `notAssigned_returnsDown`
- `assignedButNoHeartbeat_returnsDown`
- `assignedAndFreshHeartbeat_returnsUp`
- `assignedAndStaleHeartbeat_returnsDown`
- `revokedAfterAssigned_returnsDown`
- `deprecatedTimeoutMsPropertyHonored` (binds old `canary.kafka-health-timeout-ms` to the new field)

`platform/lib-node/src/__tests__/kafka-consumer-health.test.ts` — equivalent set against the new TS API:
- not assigned → unhealthy
- assigned, no heartbeat → unhealthy
- assigned, fresh heartbeat → healthy
- assigned, stale heartbeat → unhealthy
- revoked after assigned → unhealthy

`services/*/src/__tests__/config.test.ts` (Node) — assert `KAFKA_HEALTH_TIMEOUT_MS` is honored as alias with a deprecation warn.

### Service-level tests

- **Java listener gating tests** (`*KafkaListenerGatingTest.java`): unchanged. They invoke `onMessage` directly; no probe involvement.
- **Node `kafka.test.ts`** (order, notification): replace the `recordPoll`-spy assertion with a `recordHeartbeat`-spy assertion driven by emitting the `HEARTBEAT` event. Cover GROUP_JOIN → markAssigned, REBALANCING → markRevoked.
- **Node `http.test.ts`** (order, notification): the `staleHealth` setup currently calls `createKafkaHealthState(1)` then `recordPoll()`. Replace with `markAssigned() + recordHeartbeat()` then await past the 1ms threshold.

### E2E

- **K1, K2, K3, K4:** unchanged. No probe semantic change visible to these.
- **K5:** unchanged. Assertion: stable processes a flagged event after canary SIGSTOP within 30s of the 60s wait. Faster heartbeat-based detection (15s vs 30s) only widens the margin.
- **K6 (new) — cold-cluster bring-up without pre-warm.** Asserts:
  1. On a clean cluster (`ensureCleanBaseline`), `make deploy-services` completes within helm's default 3-min `--wait`.
  2. Without invoking `make pre-warm`, `make canary-deploy SVC=audit-service` completes within helm's 3-min `--wait`.
  3. Canary pod's `/actuator/health/readiness` returns 200 within 30s of the pod becoming `Running`.

K6 lives at `tests/e2e/k6-cold-cluster-bringup.test.ts` and uses the existing helpers (`ensureCleanBaseline`, `deployCanary`, `findPodByLabel`, `kubectl exec ... actuator/health/readiness`).

## Documentation changes

| File | Change |
|---|---|
| `README.md` | Drop "⚠ REQUIRED before first canary on a cold cluster" on `make pre-warm`. Replace with "Optional: seed consumer offsets to lag=0 (useful before running e2e suites that measure lag)." Update Known issues table to remove "Cold-cluster pre-warm." |
| `docs/operations.md` | Rewrite "Cold-cluster pre-warm" section: remove the deadlock explanation; keep `make pre-warm` documented as an optional e2e helper. Update "Troubleshooting" — remove the `helm install --wait timed out` ⇒ "you skipped pre-warm" entry. |
| `docs/canary-mechanics.md` | Update the cold-start mechanics paragraph: replace "the other half of the cold-start fix is `make pre-warm`" with the new probe explanation. |
| `docs/development.md` | Drop the "REQUIRED before first canary on a cold cluster" annotation on the `make pre-warm` row. Update "Known Spring Boot 4 quirks" if any reference to `recordPoll` exists. |
| `docs/history.md` | Append a new entry: post-merge fix replacing `recordPoll`-based readiness with heartbeat-based readiness; commit hash + date. |

## Out of scope (explicit)

- Stable readiness gating (Option 1 preserved).
- Removing `make pre-warm` entirely.
- Presence-watcher rework.
- Heartbeat freshness as input to canary auto-rollback decisions (just readiness; canary-ctl still requires explicit rollback).
- Per-consumer (rather than per-listener-container) health detail.

## Migration / rollout

- Single PR: lib-java + lib-node + 5 services + docs + K6 + Makefile reminder removal. Atomic by nature — the new API replaces the old.
- Old env-var/property names accepted with a deprecation log; alias removal is a follow-up tracked separately (not part of this PR).
- No state migration. Consumer offsets, group membership, and Helm release names unchanged.
- Verification path: run K1–K5 + new K6 against a fresh kind cluster; confirm `kafka-consumer-groups.sh --list` shows all 6 expected groups with lag=0 after K6.

## Risks

- **kafka-clients `last-heartbeat-seconds-ago` metric semantics.** The metric is on the per-consumer JMX tree. If the listener container is stopped (not just rebalancing), the metric supplier may go empty before `assigned=false` propagates. Mitigation: treat empty as DOWN; verify with a unit test that exercises the empty-supplier path.
- **kafkajs `HEARTBEAT` event volume.** Heartbeats fire every 3s per consumer. Calling `recordHeartbeat()` per event is a single timestamp write; no measured concern, but flagged for awareness during code review.
- **Rebalance flap.** A normal rebalance briefly clears `assigned`. If a rebalance lasts longer than kubelet's probe failureThreshold × periodSeconds (15s default), the canary will flap. This is unlikely under normal load — the project's consumer groups are single-partition per-subset — but a long rebalance during a topic resize could trip readiness. Acceptable since the canary's whole job is to be sensitive; flagged.
