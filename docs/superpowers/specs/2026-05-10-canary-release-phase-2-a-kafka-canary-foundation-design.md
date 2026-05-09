# Canary Release Management — Phase 2.a Design (Kafka canary consumer foundation)

**Status:** Approved (awaiting user review)
**Date:** 2026-05-10
**Phase:** Phase 2, sub-plan a of (a + b)
**Umbrella spec:** `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`
**Phase 1 (predecessor):** Substrate, lib propagation, services, deployments, canary-ctl, e2e harness — all merged.

## Project context

Phase 1 shipped the working substrate and HTTP-axis canary mechanics. The Phase 1 substrate simplification said: **canary pods do not consume Kafka** (`KAFKA_CONSUMERS_ENABLED=false`). Phase 1's S10 acceptance scenario enforced this: canary did NOT join any consumer group.

Phase 2 lifts that simplification. The user-confirmed core requirement extends the HTTP rules to Kafka:

1. Event has `x-canary: true` header AND canary pod is alive → only canary pod's consumer processes it
2. Event has `x-canary: true` header AND canary pod is NOT alive → stable pod's consumer processes it (graceful fallback)
3. Event has no `x-canary` header → only stable pod's consumer processes it (always)
4. Whichever pod processes the event must propagate the `x-canary` header onto downstream HTTP, Kafka, and Restate calls

Phase 2 is decomposed into two sub-plans:

- **Plan 2.a (this spec)** — Foundation: lib-java + lib-node abstractions for canary-aware Kafka consumption (per-subset consumer-group ID, presence detection, readiness gating, header propagation into the consume context); Helm chart RBAC for the k8s API watch. Lib unit tests only.
- **Plan 2.b** — Service integration + e2e scenarios: each service wires the new abstractions; canary-overlay flips `KAFKA_CONSUMERS_ENABLED=true`; new K1–K5 scenarios verify the four rules end-to-end.

Schema evolution (Phase 2's third sub-area per the umbrella spec) is deferred to a future Plan 2.c.

## Goals

1. Build a `lib-java` + `lib-node` abstraction that lets each service join a per-subset Kafka consumer group with a per-message header filter that enforces rules 1–3 above.
2. Detect canary subset presence via a long-lived **k8s API watch** on the canary's `EndpointSlice`. Maintain an in-memory `canaryReady` flag on each stable pod, updated push-style by the watch. Hot-path filter is O(1) atomic read.
3. Gate the canary pod's **readiness probe** on Kafka consumer health. If the consumer disconnects (broker unreachable, group session expired, panic in poll loop), the readiness probe fails → kubelet drops the pod from the EndpointSlice → the watch on the stable side fires → stable's `canaryReady` flag flips to false → next canary-flagged event reaches stable.
4. Propagate the `x-canary` Kafka header into the consume request context so outbound HTTP/Kafka/Restate calls inherit it via the existing Phase 1 lib mechanism.
5. Add the Helm chart RBAC needed for the k8s API watch (ServiceAccount role granting `endpointslices` get/watch in the service's namespace).
6. Ship Plan 2.a as a pure-library + chart-RBAC change. NO service code changes; NO canary-overlay flag flip; NO e2e scenarios. Those land in Plan 2.b.

## Non-goals (Plan 2.a)

- Wiring the new abstraction into any of the 5 services — Plan 2.b.
- Flipping `KAFKA_CONSUMERS_ENABLED=true` on canary — Plan 2.b (must land atomically with service migration to avoid canary pods stealing partitions from stable groups).
- E2E acceptance scenarios for Phase 2 — Plan 2.b.
- Kafka schema evolution / schema registry — Plan 2.c (future).
- Restate canary handler versioning — Phase 3.
- Argo Rollouts / percent-split / GitHub Actions CI — Phase 4.
- Production-grade k8s informer cache, leader election, etc. — Phase 4 (controller).
- Auto-recovery from canary crash by stable picking up canary group's offset (offset transfer). Out of scope; the design accepts brief queue-then-drain on canary restart, and operator-triggered rollback for permanent canary loss.

## Locked decisions (from brainstorming)

The following are settled before this design and not revisited:

- **Per-subset consumer groups**: `<svc>-events-stable` and `<svc>-events-canary`. Each subset's pods join the matching group. Both groups subscribe to the same topic; each group receives every message independently.
- **Per-message filter logic**:
  - Stable: process if `x-canary != "true"` OR `canaryReady == false`. Skip otherwise.
  - Canary: process if `x-canary == "true"`. Skip otherwise.
- **Presence detection: k8s API watch on `Pods` with label selector `app=<svc>,version=canary`.** Push-based, ~1s detection. In-memory atomic flag updated by watch events. Hot-path filter is O(1). (Initial design considered EndpointSlice but the Phase 1 per-service Service selects both subsets, so its slice mixes them; Pod-level watch is simpler.)
- **Canary readiness gating**: canary pod's readiness probe returns 200 only if (HTTP server up) AND (Kafka consumer is connected and last poll within `KAFKA_HEALTH_TIMEOUT` seconds, default 30). Liveness probe is NOT gated on Kafka — let the pod stay alive and reconnect.
- **Race window during canary becoming Ready**: accepted. Brief duplicate processing (sub-second) is documented; downstream handlers must be idempotent. Covered by the existing Phase 1 substrate (Restate and most downstreams already idempotent).
- **Header propagation into Kafka consume context**: each consume callback opens a `runWithCanary(headerValue, () => handler())` frame (same primitive as Phase 1's HTTP middleware, just triggered by the consume callback instead of HTTP). Outbound HTTP/Kafka/Restate calls inherit `x-canary` via existing Phase 1 interceptors.
- **Crash recovery for canary**: events queue on canary's group offset until canary restarts, OR operator runs `canary-ctl rollback` (which in Phase 2.a behaves identically to Phase 1.4 — explicit drain via Kafka admin is a future enhancement, not in 2.a or 2.b).

## Architecture

### Per-subset consumer-group ID resolver

A small lib helper computes the per-subset group ID from `<base-group-id>` + `<VERSION>` env var:

```
groupId(base, version) = base + "-" + version    // e.g. "orders-events-consumers-stable"
```

Java: `XCanaryConsumerGroupIdResolver` (utility class). Each `@KafkaListener` annotation reads the resolved group ID via Spring SpEL: `groupId = "#{xCanaryConsumerGroupIdResolver.resolve('orders-events-consumers')}"`. Plan 2.b updates each service's `@KafkaListener` to use the resolver.

Node: `resolveConsumerGroupId(base: string): string` — same function. Each service's `kafka.consumer({ groupId: resolveConsumerGroupId('orders-events-consumers') })` call uses it. Plan 2.b wires it into each service.

### Per-message filter (Java + Node)

The filter is a one-line decision applied at the start of each consume callback:

**Java** — new `XCanaryConsumeFilter` utility:

```java
public class XCanaryConsumeFilter {
    private final XCanaryPresenceWatcher presence;
    private final String ownVersion;        // "stable" or "canary"

    public boolean shouldProcess(Headers kafkaHeaders) {
        boolean carriesCanary = isCanaryHeader(kafkaHeaders);
        if ("canary".equals(ownVersion)) {
            return carriesCanary;                                  // canary: only x-canary events
        }
        return !carriesCanary || !presence.isCanaryReady();        // stable: non-canary OR canary absent
    }
}
```

**Node** — equivalent function:

```typescript
export function shouldProcess(kafkaHeaders: IHeaders | undefined, ownVersion: string, isCanaryReady: () => boolean): boolean {
  const carriesCanary = isCanaryHeaderValue(kafkaHeaders?.[X_CANARY_HEADER]);
  if (ownVersion === "canary") return carriesCanary;
  return !carriesCanary || !isCanaryReady();
}
```

Each service wraps its consume callback to gate on `shouldProcess`. Plan 2.b applies the wrapping; Plan 2.a only ships the helpers.

### XCanaryPresenceWatcher — k8s Pod watch

The presence watcher is a singleton per service that:

1. Reads the k8s service-account credentials from `/var/run/secrets/kubernetes.io/serviceaccount/`
2. Connects to the in-cluster Kubernetes API server (`https://kubernetes.default.svc`)
3. Opens a long-lived `watch` on `Pods` in the service's namespace with label selector `app=<svc>,version=canary` (where `<svc>` is e.g. `payment-service`)
4. For each watch event, inspects the Pod's `status.conditions`:
   - If at least one matching pod has `conditions[type=Ready].status == "True"` → set `canaryReady = true`
   - Else → set `canaryReady = false`
5. Maintains the watch indefinitely; on disconnect, reconnects with exponential backoff (initial 1s, max 30s)
6. Exposes `boolean isCanaryReady()` for the consume filter

Why Pod-level watch (not EndpointSlice or Deployment):
- The Phase 1 per-service Service selects both stable + canary subsets (`app=<svc>` only), so its EndpointSlice mixes both — we can't filter by version without extra lookups
- Watching Pods with `version=canary` selector gives us exactly the canary subset's readiness in one stream
- Per-pod granularity matches what we actually care about (any Ready canary pod = canary present)
- Single watch per service (one stream per stable pod, watching ≤2 canary pod objects in steady state)

**Java implementation**: Uses Kubernetes Java client (`io.kubernetes:client-java`) — already a transitive dep of Spring Boot for some integrations; standalone if not. Lightweight enough for the foundation.

Actually, on reflection, let's use the lighter-weight `fabric8 kubernetes-client` (`io.fabric8:kubernetes-client`) — more idiomatic in Spring Boot apps and well-supported.

**Node implementation**: Uses `@kubernetes/client-node`. Standard library for k8s interactions in Node. Long-lived informer pattern.

Both libraries handle: TLS to the in-cluster API, service-account token authentication, watch protocol, reconnect.

### Kafka consumer health indicator (for canary readiness probe)

The canary pod's readiness probe path (`/actuator/health/readiness` for Java, `/health` for Node) must include a check on Kafka consumer health.

**Java** — `KafkaConsumerHealthIndicator` implements Spring Actuator's `HealthIndicator`. Spring Actuator automatically aggregates all `HealthIndicator` beans into `/actuator/health/readiness`. The bean tracks:
- Whether the Kafka consumer container is running (Spring's `KafkaListenerEndpointRegistry` exposes this)
- Last successful poll timestamp (track via a `KafkaListenerErrorHandler` callback that updates a timestamp on each successful batch)
- Returns `Health.up()` if running AND last poll < `KAFKA_HEALTH_TIMEOUT` seconds ago. Else `Health.outOfService()`.

The `OUT_OF_SERVICE` status maps to a non-200 response on `/actuator/health/readiness`, which fails the readiness probe. Kubelet drops the pod from EndpointSlice within seconds.

**Node** — `kafkaConsumerHealth(): { ok: boolean, reason?: string }` factory function. Each service's `/health` endpoint includes the result in its response and returns 503 if any check is unhealthy. Plan 2.b wires this into each Node service's `/health` route.

### Header propagation through consume

Same pattern as Phase 1's HTTP middleware. Java:

```java
// Inside each @KafkaListener handler, BEFORE business logic:
String canaryHeader = headers.get(XCanaryConstants.HEADER_NAME);
boolean canary = XCanaryConstants.TRUE_VALUE.equals(canaryHeader);
boolean prior = XCanaryContext.isCanary();
XCanaryContext.set(canary);
try {
    // business logic — outbound calls inherit x-canary via existing interceptors
} finally {
    XCanaryContext.set(prior);
    if (!prior) XCanaryContext.clear();
}
```

Lib-java exports `XCanaryConsumeContext.runWith(headers, () -> handler.run())` as the helper. Plan 2.b wraps each service's `@KafkaListener` body with it.

Node equivalent: `runWithCanaryFromHeaders(kafkaHeaders, () => handler())` using AsyncLocalStorage.

### Helm chart RBAC

The k8s API watch needs:
- `ServiceAccount` (already exists per Plan 1.3.b)
- `Role` granting `get`, `list`, `watch` on `pods` in the service's namespace
- `RoleBinding` linking the SA to the Role

New chart template `deploy/helm/service-chart/templates/role.yaml`:

```yaml
{{- if .Values.canaryWatch.enabled }}
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "service-chart.resourceName" . }}-canary-watch
  namespace: {{ .Release.Namespace }}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "service-chart.resourceName" . }}-canary-watch
  namespace: {{ .Release.Namespace }}
subjects:
  - kind: ServiceAccount
    name: {{ .Values.serviceName }}
    namespace: {{ .Release.Namespace }}
roleRef:
  kind: Role
  name: {{ include "service-chart.resourceName" . }}-canary-watch
  apiGroup: rbac.authorization.k8s.io
{{- end }}
```

Conditional on `.Values.canaryWatch.enabled` (default `true`) so the chart can render in environments without RBAC if needed.

## Data flow (planned, implemented in 2.a as lib code; exercised in 2.b)

### Steady state — canary deployed and Ready

1. Producer emits an event with `x-canary: true` Kafka header (because the producer's request context had it; Phase 1 lib already stamps this).
2. Stable consumer (`<svc>-events-stable` group) receives the message via Kafka delivery.
3. Stable's `XCanaryConsumeFilter.shouldProcess(headers)`:
   - `carriesCanary = true`, `ownVersion = "stable"`, `presence.isCanaryReady() = true`
   - Returns `false` → message is acked but not processed.
4. Canary consumer (`<svc>-events-canary` group) ALSO receives the same message.
5. Canary's `XCanaryConsumeFilter.shouldProcess(headers)`:
   - `carriesCanary = true`, `ownVersion = "canary"`
   - Returns `true` → message is processed.
6. Inside canary's processing, `XCanaryConsumeContext.runWith(headers, ...)` opens a context frame with `x-canary: true`. Outbound HTTP/Kafka/Restate calls inherit it via Phase 1 interceptors.

### Canary not deployed (no canary group exists, or has zero members)

1. Producer emits event with `x-canary: true` (could come from stable HTTP-falling-back-when-no-canary chain).
2. Stable consumer receives it.
3. `presence.isCanaryReady() = false` (watch sees no Ready canary endpoint in EndpointSlice).
4. Filter returns `true` → stable processes the message. Graceful fallback achieved.

### Canary deployed but Kafka consumer unhealthy

1. Canary's Kafka consumer disconnects (broker timeout, partition revocation, panic).
2. Within `KAFKA_HEALTH_TIMEOUT` (30s default) after last successful poll, canary's `/actuator/health/readiness` (or `/health`) returns 503.
3. Kubelet flips the canary pod to `Ready=false` on its next probe (period typically 5–10s).
4. EndpointSlice update propagates: the canary endpoint's `conditions.ready` flips to `false`.
5. Stable pod's k8s watch fires within ~1s.
6. Stable's `presence.isCanaryReady()` flips to `false`.
7. Next canary-flagged event reaches stable: filter returns `true` → stable processes it.
8. Meanwhile, canary's Kafka client library auto-reconnects in the background. When successful, next poll updates the timestamp; readiness probe returns 200; kubelet flips back to `Ready=true`; EndpointSlice update; stable's flag flips back; canary resumes processing.

Brief duplicate processing during the recovery window is acceptable per the locked decision.

### Canary deploy — race window

1. Canary deploy starts. Pod boots, app initializes.
2. Kafka consumer joins `<svc>-events-canary` group. Begins receiving messages.
3. Some messages may be processed by canary BEFORE the readiness probe passes and the EndpointSlice is updated — during this ~100–500ms window, stable may also process the same canary-flagged message (because stable's flag is still `false`).
4. Once the readiness probe passes, EndpointSlice updates, stable's watch fires, flag flips. Steady state resumes.

This race is documented; downstream handlers must tolerate idempotent reprocessing.

## Repo additions (Plan 2.a)

```
platform/lib-java/                                                      # MODIFY
├── build.gradle.kts                                                    # MODIFY: add fabric8 kubernetes-client dep
├── src/main/java/com/canary/platform/lib/
│   ├── XCanaryConsumerGroupIdResolver.java                             # NEW
│   ├── XCanaryConsumeFilter.java                                       # NEW
│   ├── XCanaryConsumeContext.java                                      # NEW (helper around runWith)
│   ├── XCanaryPresenceWatcher.java                                     # NEW (k8s informer)
│   ├── KafkaConsumerHealthIndicator.java                               # NEW (Spring HealthIndicator)
│   └── autoconfigure/XCanaryAutoConfiguration.java                     # MODIFY: register the new beans
└── src/test/java/com/canary/platform/lib/                              # NEW tests
    ├── XCanaryConsumerGroupIdResolverTest.java
    ├── XCanaryConsumeFilterTest.java
    ├── XCanaryConsumeContextTest.java
    ├── XCanaryPresenceWatcherTest.java                                  (mocked k8s client)
    └── KafkaConsumerHealthIndicatorTest.java

platform/lib-node/                                                      # MODIFY
├── package.json                                                        # MODIFY: add @kubernetes/client-node dep
├── src/
│   ├── x-canary-consumer-group.ts                                      # NEW (resolveConsumerGroupId)
│   ├── x-canary-consume-filter.ts                                      # NEW (shouldProcess)
│   ├── x-canary-consume-context.ts                                     # NEW (runWithCanaryFromHeaders)
│   ├── x-canary-presence-watcher.ts                                    # NEW (k8s informer)
│   ├── kafka-consumer-health.ts                                        # NEW (factory + tracking)
│   └── index.ts                                                        # MODIFY: re-exports
└── src/__tests__/                                                       # NEW tests
    ├── x-canary-consumer-group.test.ts
    ├── x-canary-consume-filter.test.ts
    ├── x-canary-consume-context.test.ts
    ├── x-canary-presence-watcher.test.ts
    └── kafka-consumer-health.test.ts

deploy/helm/service-chart/templates/                                    # MODIFY
└── role.yaml                                                           # NEW: RBAC for endpointslices watch

deploy/helm/service-chart/values.yaml                                   # MODIFY: add canaryWatch.enabled (default true)
```

NO changes to:
- canary-overlay.yaml — flag flip lives in 2.b
- Any service code — lib consumption lives in 2.b
- canary-ctl — drain logic is a future enhancement, not in 2.a or 2.b
- E2E scenarios — Phase 2 scenarios live in 2.b

## Testing strategy

### Unit tests (all in 2.a)

| Module | Test focus |
|---|---|
| `XCanaryConsumerGroupIdResolver` | Resolves "stable" → "<base>-stable", "canary" → "<base>-canary"; defaults to "stable" when env unset |
| `XCanaryConsumeFilter` | All 4 cells of the (subset × canary-header × canaryReady) decision table |
| `XCanaryConsumeContext` | runWith opens context, inner block sees x-canary value, context cleaned on exit |
| `XCanaryPresenceWatcher` | Mocked k8s client. Watch event with Ready canary endpoint → flag true. Watch event without → flag false. Disconnect → reconnect with backoff. |
| `KafkaConsumerHealthIndicator` | Healthy when running + recent poll. Out-of-service when paused or stale. |

Equivalent Node tests using vitest + mocked `@kubernetes/client-node`.

Target: ~30 new unit tests across both libs (~6 per module × 5 modules).

### Manual verification (operator)

After 2.a merges + image rebuild + redeploy:

```bash
# 1. Verify stable pod's RBAC works
kubectl auth can-i watch pods -n services --as=system:serviceaccount:services:payment-service
# Expected: yes

# 2. Verify lib-java HealthIndicator surfaces (no Kafka consumer wired yet, so check just /actuator/health structure)
kubectl -n services exec deploy/payment-service-stable -- curl -s localhost:8081/actuator/health | jq '.components | keys'
# Expected: includes "kafkaConsumer" (the new HealthIndicator)
```

These are smoke checks. Real Phase 2 acceptance scenarios are 2.b's job.

### NOT tested in 2.a

- End-to-end Kafka routing (no service uses the new abstractions yet)
- Canary readiness gating against a real Kafka disconnect (no service wired yet)
- The 4-rule consume behavior against a real cluster

All of the above are 2.b's scope.

## Operator workflow (after Plan 2.a merges)

```
make verify                                         # all unit tests including new lib tests
make build-services                                 # rebuilds with new lib code
make build-images && make load-images               # refresh images
make deploy-services                                # apply Helm chart (now includes Role + RoleBinding)

# Smoke check RBAC:
kubectl auth can-i watch pods -n services --as=system:serviceaccount:services:payment-service
```

The behavior of the cluster does not change. No new e2e scenarios pass or fail — we just have new lib code + RBAC sitting unused. Plan 2.b consumes it.

## Done when

- All unit tests pass: `make verify` runs cleanly with the new lib tests included (~30 new tests across Java + Node).
- `pnpm --filter @canary/lib-node build` and `./gradlew :platform:lib-java:build` are clean.
- Helm chart renders cleanly with the new Role + RoleBinding (verify with `helm template ... -f payment-service.yaml`).
- README has a `## Plan 2.a` section noting the foundation landed but not yet wired.
- All commits in the implementation plan's task list are present on `claude/phase-2.a-kafka-canary-foundation`.

## Open assumptions

- The kind cluster's k8s API server is reachable from in-cluster pods via the standard `https://kubernetes.default.svc` endpoint with the auto-mounted ServiceAccount token. (True for standard kind setup.)
- Strimzi's KafkaTopic CRDs already create the topics; per-subset consumer groups are created on first consumer poll (Kafka auto-creates groups). No new KafkaTopic CRDs needed in 2.a.
- The core `v1` Pod API is available (universally true in Kubernetes 1.x).
- `fabric8 kubernetes-client` works with Spring Boot 4 + Java 25 (verify during implementation; fall back to `io.kubernetes:client-java` if incompatible).
- `@kubernetes/client-node` works with Node 25 ESM (it does; standard).
- The 100–500ms canary-becoming-Ready race window is tolerated by downstream handlers. If downstream handlers prove non-idempotent during 2.b validation, the design adds a startup gate (canary consumer pauses until pod is Ready).

## Phase 2 sub-plan summary (for context)

**2.a (this spec) — Foundation, lib-only.**
**2.b — Service integration:** wire abstractions into all 5 services; flip `KAFKA_CONSUMERS_ENABLED=true` in canary-overlay; add Phase 2 e2e scenarios K1–K5.
**2.c (future) — Schema evolution:** schema registry, backward/forward-compat checks, canary-aware schema versions.
