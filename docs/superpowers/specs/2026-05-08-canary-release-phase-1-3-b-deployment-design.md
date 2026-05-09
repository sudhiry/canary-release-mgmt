# Canary Release Management — Plan 1.3.b Deployment Design

**Status:** Awaiting user review
**Date:** 2026-05-08
**Phase / sub-plan:** 1.3.b (Plan 1, sub-plan 3, part b)
**Depends on:** 1.1 (foundation), 1.2 (shared libs), 1.3.a (services code, all merged)
**Followed by:** 1.4 (canary-ctl + per-service VirtualService header rule)

## Project context

This sub-plan delivers everything needed to run the five domain services from Plan 1.3.a on the local kind cluster from Plan 1.1, behind Istio routing, with stable-only traffic. It is the substrate that Plan 1.4 (`canary-ctl`) will extend to add canary Deployments and the VirtualService header-match rule.

The Phase 1 design spec at `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md` is the contract this sub-plan delivers against. Where Phase 1 specifies a layout or convention, this design follows it; deviations are called out explicitly.

## Goals

1. Build a container image per service and load it into the kind cluster's image cache.
2. Provision the five Kafka topics consumed by the services (via Strimzi `KafkaTopic` CRDs) so consumers can subscribe at startup.
3. Deploy stable Deployments + Services + ConfigMaps + ServiceAccounts for the five services using one shared, parameterized Helm chart with per-service values files.
4. Apply per-service `DestinationRule` (with `stable` and `canary` subsets pre-defined) and a default-only `VirtualService` (routes 100% to `stable`) — so Plan 1.4 only needs to add the header-match rule.
5. Apply an Istio `Gateway` + edge `VirtualService` exposing `localhost:8080/api/orders` → order-service.
6. Register each stable service's Restate handlers with the Restate Admin API automatically via a per-service Helm post-install/post-upgrade hook Job.
7. Provide a checked-in `canary-overlay.yaml` Helm values file that hard-codes the canary contract (`KAFKA_CONSUMERS_ENABLED=false`, `RESTATE_REGISTER_HANDLERS=false`, no registration Job) — Plan 1.4 uses it.
8. Add `make` targets (`build-images`, `load-images`, `deploy-services`, `undeploy-services`, `smoke-services`) and a bats smoke test that verifies the deployed substrate works end to end (`POST /api/orders` returns 200 through Istio).

## Non-goals (1.3.b)

- Canary Deployment lifecycle and the VirtualService header-match rule — Plan 1.4 (`canary-ctl`).
- Cross-service e2e acceptance scenarios (S1–S13 from the Phase 1 spec) — Plan 1.5.
- Auto-rollback on health regression — Phase 4.
- Percent-split traffic routing — Phase 4.
- OPA/Kyverno admission policies for label hardening — Phase 4.
- Real cloud cluster deploys (EKS/GKE/AKS) — out of project scope.
- Production observability polish (Grafana dashboards, alerts, runbooks) — Phase 5.
- AuthN/AuthZ and persistent stores — out of project scope.

## High-level approach

**Helm for workloads, raw YAML for routing.** The Phase 1 spec at lines 110–112 explicitly puts routing config (`destination-rules/`, `virtual-services/`) outside the Helm chart and inside `deploy/routing/` as standalone files. We follow that. The reason matters: in Plan 1.4, `canary-ctl` will mutate each service's `VirtualService` (via JSON-merge patch) to add a header-match rule. If the VS were owned by a Helm release, the next `helm upgrade` would clobber the canary header rule, breaking the canary lifecycle. Keeping routing as `kubectl`-applied YAML cleanly separates workload ownership (Helm) from routing ownership (`canary-ctl`).

**One shared chart, five per-service values files.** Per the Phase 1 spec at line 90, all five services have the same Kubernetes shape (`Deployment + Service + ConfigMap + ServiceAccount`, plus the canary Deployment toggled by overlay). A single parameterized chart enforces consistency across the polyglot stack and is the standard idiom for a reference architecture. Per-service values files capture the small differences (ports, downstream URLs, probe paths).

**Local image flow only.** `docker build` produces images tagged `canary-release-mgmt/<svc>:dev`. `kind load docker-image` puts them in the cluster's image cache. Deployments use `imagePullPolicy: IfNotPresent`. No registry, no GitHub Actions, no remote anything in 1.3.b; CI is added in Phase 4 per the Phase 1 spec.

**Restate handler registration as a Helm post-install Job.** The 1.3.a code starts a Restate handler HTTP server on each service's `RESTATE_HANDLER_PORT` but does not register itself with the Restate Admin API. Registration must happen after the service is Ready and after Restate is reachable. We implement this as a per-service Kubernetes Job created by a Helm `post-install,post-upgrade` hook, which: (a) waits for the service's restate handler endpoint to respond on `/discover`; (b) calls `POST {restate-admin}/deployments` with the service's in-cluster URL and `force: true` for idempotency. The Job is auto-deleted on success via `helm.sh/hook-delete-policy: hook-succeeded,before-hook-creation`. The canary overlay disables this Job so canary pods never register handlers.

**Probes via Spring Boot Actuator (Java) and a custom `/health` route (Node).** Java services add `spring-boot-starter-actuator` and expose `/actuator/health/liveness` + `/actuator/health/readiness`. Node services add a 5-line `/health` route. K8s probes hit those.

## Repo layout (new + modified files)

```
deploy/
├── images/
│   └── build-and-load.sh                  # NEW — build all 5 images + kind load
├── kafka/topics/                          # NEW — 5 KafkaTopic CRDs
│   ├── orders.events.yaml
│   ├── payments.events.yaml
│   ├── inventory.events.yaml
│   ├── notifications.events.yaml
│   └── audit.events.yaml
├── helm/
│   ├── service-chart/                     # NEW — shared parameterized chart
│   │   ├── Chart.yaml
│   │   ├── values.yaml                    # defaults
│   │   └── templates/
│   │       ├── _helpers.tpl
│   │       ├── serviceaccount.yaml
│   │       ├── configmap.yaml
│   │       ├── deployment.yaml
│   │       ├── service.yaml
│   │       └── restate-register-job.yaml  # post-install Helm hook
│   └── values/
│       ├── audit-service.yaml
│       ├── payment-service.yaml
│       ├── inventory-service.yaml
│       ├── notification-service.yaml
│       ├── order-service.yaml
│       └── canary-overlay.yaml            # consumed by Plan 1.4
├── routing/                               # NEW — raw YAML, kubectl-applied
│   ├── destination-rules/
│   │   ├── audit-service.yaml             # subsets: stable, canary
│   │   ├── payment-service.yaml
│   │   ├── inventory-service.yaml
│   │   ├── notification-service.yaml
│   │   └── order-service.yaml
│   ├── virtual-services/                  # default rule only; canary rule added by canary-ctl in 1.4
│   │   ├── audit-service.yaml
│   │   ├── payment-service.yaml
│   │   ├── inventory-service.yaml
│   │   ├── notification-service.yaml
│   │   └── order-service.yaml
│   └── ingress/
│       ├── gateway.yaml                   # one Gateway for the cluster
│       └── edge-virtualservice.yaml       # /api/orders → order-service
└── services/
    ├── deploy.sh                          # NEW — orchestrates topics + helm + routing
    └── undeploy.sh                        # NEW — inverse

services/
├── audit-service/
│   └── Dockerfile                         # NEW
├── payment-service/
│   └── Dockerfile                         # NEW
├── inventory-service/
│   └── Dockerfile                         # NEW
├── order-service/
│   └── Dockerfile                         # NEW
└── notification-service/
    └── Dockerfile                         # NEW

tests/services/
└── deploy.bats                            # NEW — smoke

# Modified
services/audit-service/build.gradle.kts                         # + actuator
services/payment-service/build.gradle.kts                       # + actuator
services/inventory-service/build.gradle.kts                     # + actuator
services/audit-service/src/main/resources/application.yml       # expose actuator endpoints
services/payment-service/src/main/resources/application.yml
services/inventory-service/src/main/resources/application.yml
services/order-service/src/http.ts                              # GET /health
services/notification-service/src/http.ts                       # GET /health
gradle/libs.versions.toml                                       # + actuator alias
Makefile                                                        # 5 new targets
README.md                                                       # 1.3.b section
```

## Helm chart contract

The chart `deploy/helm/service-chart/` accepts these top-level values. Defaults are in `values.yaml`; per-service files override; `canary-overlay.yaml` overrides further.

| key | purpose | default | per-svc override |
|---|---|---|---|
| `serviceName` | k8s name (e.g., `audit-service`) | (required) | yes |
| `image.repository` | image namespace | `canary-release-mgmt` | no |
| `image.tag` | image tag | `dev` | only via overlay |
| `image.pullPolicy` | pull policy | `IfNotPresent` | no |
| `version` | `stable` or `canary`; drives label + Deployment name | `stable` | overlay sets `canary` |
| `replicas` | replica count | `1` | yes |
| `ports.http` | container HTTP port | (required) | yes |
| `ports.restateHandler` | container Restate handler port | (required) | yes |
| `env.KAFKA_BOOTSTRAP_SERVERS` | Kafka URL | `my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092` | rarely |
| `env.RESTATE_INGRESS_URL` | Restate ingress URL (handler-to-handler calls) | `http://restate.restate.svc.cluster.local:8080` | rarely |
| `env.RESTATE_ADMIN_URL` | Restate admin URL (used by registration Job) | `http://restate.restate.svc.cluster.local:9070` | rarely |
| `env.KAFKA_CONSUMERS_ENABLED` | feature flag (1.2 lib reads this) | `"true"` | overlay sets `"false"` |
| `env.RESTATE_REGISTER_HANDLERS` | feature flag (1.2 lib reads this) | `"true"` | overlay sets `"false"` |
| `extraEnv` | service-specific env (e.g., order-service downstream URLs) | `{}` | yes |
| `probes.liveness.path` | HTTP probe path | `/health` | yes (Java overrides to `/actuator/health/liveness`) |
| `probes.readiness.path` | HTTP probe path | `/health` | yes (Java overrides to `/actuator/health/readiness`) |
| `resources.requests.{cpu,memory}` | resource requests | `100m / 128Mi` (Node), `200m / 256Mi` (Java) | yes |
| `resources.limits.{cpu,memory}` | resource limits | `500m / 256Mi` (Node), `1000m / 512Mi` (Java) | yes |
| `restate.registerEndpoint` | enable post-install registration Job | `true` | overlay sets `false` |

All resources land in the `services` namespace. The namespace itself is created (and labeled `istio-injection: enabled`) by `deploy/services/deploy.sh` before any `helm install` runs — keeping it out of every chart release avoids the "5 releases own the same namespace" race and lets us set the istio-injection label deterministically.

The `Service` selects `app: <serviceName>` (both versions). The `Deployment` is named `<serviceName>-<version>` (e.g., `audit-service-stable`) and labels include `version`. The selector on the Deployment matches `app + version` so canary and stable Deployments do not collide.

The `Service` exposes two named ports: `http` (mapped to `ports.http`) and `restate` (mapped to `ports.restateHandler`). The Restate Admin registration Job uses the `restate` port via the in-cluster Service URL.

## Per-service values

| service | http | restate | probes | extraEnv |
|---|---|---|---|---|
| `audit-service` | 8083 | 9083 | `/actuator/health/{liveness,readiness}` | — |
| `payment-service` | 8081 | 9081 | `/actuator/health/{liveness,readiness}` | — |
| `inventory-service` | 8082 | 9082 | `/actuator/health/{liveness,readiness}` | — |
| `order-service` | 3001 | 9084 | `/health` | `INVENTORY_URL=http://inventory-service.services.svc.cluster.local:8082`, `PAYMENT_URL=http://payment-service.services.svc.cluster.local:8081`, `NOTIFICATION_URL=http://notification-service.services.svc.cluster.local:3002` |
| `notification-service` | 3002 | 9085 | `/health` | — |

(Ports preserved verbatim from each service's 1.3.a `application.yml` / `config.ts` so service code does not change.)

## Canary overlay (`deploy/helm/values/canary-overlay.yaml`)

Hard-codes the canary contract per the Phase 1 spec at lines 173–176, 226, 230:

```yaml
version: canary
replicas: 1
env:
  KAFKA_CONSUMERS_ENABLED: "false"
  RESTATE_REGISTER_HANDLERS: "false"
restate:
  registerEndpoint: false
```

Plan 1.4's `canary-ctl deploy-canary <svc> <tag>` will run roughly:

```bash
helm upgrade --install <svc>-canary deploy/helm/service-chart \
  -f deploy/helm/values/<svc>.yaml \
  -f deploy/helm/values/canary-overlay.yaml \
  --set image.tag=<tag> \
  -n services
```

We do **not** install the canary release in 1.3.b — only check the file in.

## Routing config (raw YAML, `deploy/routing/`)

### `destination-rules/<svc>.yaml`

```yaml
apiVersion: networking.istio.io/v1
kind: DestinationRule
metadata:
  name: <svc>
  namespace: services
spec:
  host: <svc>.services.svc.cluster.local
  subsets:
    - name: stable
      labels:
        version: stable
    - name: canary
      labels:
        version: canary
```

Both subsets are pre-defined. Until Plan 1.4 deploys a canary Deployment with `version: canary` labels, the canary subset is simply empty (no endpoints). This is fine — Istio tolerates empty subsets; only the default VS rule is in effect, so no traffic is sent to canary.

### `virtual-services/<svc>.yaml`

```yaml
apiVersion: networking.istio.io/v1
kind: VirtualService
metadata:
  name: <svc>
  namespace: services
spec:
  hosts:
    - <svc>.services.svc.cluster.local
  http:
    - name: default
      route:
        - destination:
            host: <svc>.services.svc.cluster.local
            subset: stable
```

Default-only. Plan 1.4's `canary-ctl` inserts a header-match rule at index 0 via JSON-merge patch.

### `ingress/gateway.yaml`

```yaml
apiVersion: networking.istio.io/v1
kind: Gateway
metadata:
  name: edge-gateway
  namespace: services
spec:
  selector:
    istio: ingressgateway
  servers:
    - port: { number: 80, name: http, protocol: HTTP }
      hosts: ["*"]
```

### `ingress/edge-virtualservice.yaml`

```yaml
apiVersion: networking.istio.io/v1
kind: VirtualService
metadata:
  name: edge
  namespace: services
spec:
  hosts: ["*"]
  gateways: [edge-gateway]
  http:
    - match:
        - uri: { prefix: /api/orders }
      route:
        - destination:
            host: order-service.services.svc.cluster.local
            port: { number: 3001 }
```

Phase 1 only needs to expose the order-service entry point externally; downstream services are reached via in-cluster DNS.

## KafkaTopic CRDs (`deploy/kafka/topics/`)

Five files, one per topic:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: <topic-name>     # e.g., orders.events
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  partitions: 1
  replicas: 1
```

Single-partition, single-replica. The 1.1 Kafka cluster is single-broker and `default.replication.factor: 1` is already set. Phase 2 may revisit partitioning when adding consumer canary strategies.

Topic names match the 1.3.a service code:
- `orders.events`
- `payments.events`
- `inventory.events`
- `notifications.events`
- `audit.events`

## Restate handler registration

Per-service Helm post-install/post-upgrade hook Job (`templates/restate-register-job.yaml`). Pseudocode:

```yaml
{{- if .Values.restate.registerEndpoint }}
apiVersion: batch/v1
kind: Job
metadata:
  name: register-restate-{{ .Values.serviceName }}-{{ .Release.Revision }}
  annotations:
    helm.sh/hook: post-install,post-upgrade
    helm.sh/hook-delete-policy: hook-succeeded,before-hook-creation
spec:
  backoffLimit: 6
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: register
          image: curlimages/curl:8.10.1
          command:
            - sh
            - -c
            - |
              set -eu
              # 1. wait for service's restate handler endpoint to be reachable
              until curl -sf -o /dev/null \
                http://{{ .Values.serviceName }}.services.svc.cluster.local:{{ .Values.ports.restateHandler }}/discover; do
                echo "waiting for {{ .Values.serviceName }} restate handler..."; sleep 2;
              done
              # 2. register with restate admin (idempotent via force:true)
              curl -sf -X POST {{ .Values.env.RESTATE_ADMIN_URL }}/deployments \
                -H 'content-type: application/json' \
                -d '{"uri":"http://{{ .Values.serviceName }}.services.svc.cluster.local:{{ .Values.ports.restateHandler }}","force":true}'
{{- end }}
```

Why a Job rather than service self-registration:
- Service self-registration would require the service to know its own in-cluster DNS name (or use the Downward API) and call admin synchronously at startup, blocking pod readiness on a separate cluster service. A Job decouples this cleanly.
- Helm hooks give us proper deploy ordering (Job runs only after the Deployment's resources exist) and lifecycle cleanup (`helm uninstall` removes the Job; success-deletion keeps the cluster tidy).
- The `force: true` Restate admin contract makes this idempotent — re-registering an existing URI replaces the registration. Safe across `helm upgrade`.

Why canary skips: `canary-overlay.yaml` sets `restate.registerEndpoint: false` → Job is not rendered → canary pods are never registered. Combined with `RESTATE_REGISTER_HANDLERS=false` (which causes 1.3.a's `RestateEndpointConfig` to skip starting the handler endpoint server entirely), this gives two layers of isolation matching the Phase 1 spec error-handling section E.

## Image build (`deploy/images/build-and-load.sh`)

Single bash script, two subcommands:

- `./build-and-load.sh build` — for each of the 5 services, run `docker build -f services/<svc>/Dockerfile -t canary-release-mgmt/<svc>:dev .` from the repo root.
- `./build-and-load.sh load` — for each of the 5 services, run `kind load docker-image canary-release-mgmt/<svc>:dev --name "$KIND_CLUSTER_NAME"`.
- `./build-and-load.sh all` — build then load.

### Java Dockerfile (per service)

Multi-stage. Build stage uses `eclipse-temurin:25-jdk` (matches local toolchain), copies the gradle wrapper + relevant subprojects, runs `./gradlew :services:<svc>:bootJar --no-daemon`. Runtime stage uses `eclipse-temurin:25-jre` and copies the resulting fat jar.

```dockerfile
# syntax=docker/dockerfile:1
FROM eclipse-temurin:25-jdk AS builder
WORKDIR /repo
COPY gradlew settings.gradle.kts build.gradle.kts /repo/
COPY gradle /repo/gradle
COPY platform/lib-java /repo/platform/lib-java
COPY platform/restate-defs-java /repo/platform/restate-defs-java
COPY services/<svc> /repo/services/<svc>
RUN ./gradlew :services:<svc>:bootJar --no-daemon

FROM eclipse-temurin:25-jre
COPY --from=builder /repo/services/<svc>/build/libs/*.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

### Node Dockerfile (per service)

Multi-stage. Build stage uses `node:25-alpine` + pnpm, installs workspace deps, builds `lib-node`, `restate-defs-node`, and the service, then `pnpm deploy --prod` flattens workspace deps into a self-contained directory. Runtime stage copies that directory.

```dockerfile
# syntax=docker/dockerfile:1
FROM node:25-alpine AS builder
WORKDIR /repo
RUN npm install -g pnpm@9.12.0
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json /repo/
COPY platform/lib-node/package.json /repo/platform/lib-node/
COPY platform/restate-defs-node/package.json /repo/platform/restate-defs-node/
COPY services/<svc>/package.json /repo/services/<svc>/
RUN pnpm install --frozen-lockfile
COPY platform/lib-node /repo/platform/lib-node
COPY platform/restate-defs-node /repo/platform/restate-defs-node
COPY services/<svc> /repo/services/<svc>
RUN pnpm --filter @canary/lib-node --filter @canary/restate-defs-node --filter @canary/<svc> build
RUN pnpm deploy --filter @canary/<svc> --prod /deploy

FROM node:25-alpine
WORKDIR /app
COPY --from=builder /deploy /app
ENTRYPOINT ["node", "dist/index.js"]
```

Build context for both is the **repo root** (so the Dockerfile can pull in sibling `platform/` modules). Each service's Dockerfile lives in `services/<svc>/Dockerfile` — five separate files, each with the `<svc>` placeholder above replaced by the concrete service name (e.g., `audit-service`, `order-service`). Node services additionally use `@canary/<svc>` as the pnpm filter target, since the package names in `package.json` follow that convention.

## Probes

### Java (`spring-boot-starter-actuator`)

Add the dependency to `gradle/libs.versions.toml` and to each Java service's `build.gradle.kts`. Configure in each `application.yml`:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info
  endpoint:
    health:
      probes:
        enabled: true
      show-details: never
```

Spring Boot Actuator then serves:
- `/actuator/health/liveness` — process is up
- `/actuator/health/readiness` — process is ready to handle traffic (default: app context refreshed)

These run on the same HTTP port as the service (no extra port). K8s probes use them with default thresholds: `initialDelaySeconds: 20` (liveness) / `10` (readiness), `periodSeconds: 10` / `5`, `failureThreshold: 3`.

### Node

Add a 5-line route to each service's `setupHttp` (`services/order-service/src/http.ts`, `services/notification-service/src/http.ts`):

```typescript
app.get("/health", (_req, res) => res.json({ ok: true }));
```

Single path used for both liveness and readiness probes. Same K8s probe thresholds.

## Deploy ordering (`make deploy-services` → `deploy/services/deploy.sh`)

```bash
set -euo pipefail
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:?must be set}"

# 1. Create services namespace with istio-injection label (idempotent)
kubectl get namespace services >/dev/null 2>&1 || kubectl create namespace services
kubectl label namespace services istio-injection=enabled --overwrite

# 2. Apply KafkaTopic CRDs and wait for Strimzi user-operator to create topics
kubectl apply -f deploy/kafka/topics/
kubectl wait --for=condition=Ready --timeout=60s -n kafka kafkatopics --all

# 3. Helm install all 5 services (workloads only)
SERVICES=(audit-service payment-service inventory-service notification-service order-service)
for svc in "${SERVICES[@]}"; do
  helm upgrade --install "$svc" deploy/helm/service-chart \
    -f deploy/helm/values/"$svc".yaml \
    -n services
done

# 4. Wait for stable Deployments to be Available
kubectl wait --for=condition=Available --timeout=180s -n services deployment --all

# 5. Apply routing config (DR + default-only VS per service)
kubectl apply -f deploy/routing/destination-rules/
kubectl apply -f deploy/routing/virtual-services/

# 6. Apply edge ingress
kubectl apply -f deploy/routing/ingress/

echo "==> deploy-services complete"
```

Helm post-install hook (`restate-register-job.yaml`) handles Restate registration per service after step 2; no extra registration step is needed in the script.

`undeploy.sh` is the inverse: delete edge → delete routing → `helm uninstall` each service → delete topics. The `services` namespace is left in place for fast re-deploy.

## Smoke verification (`tests/services/deploy.bats`)

Five test functions:

1. `@test "all 5 KafkaTopics are Ready"` — `kubectl get -n kafka kafkatopic <name> -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'` for each.
2. `@test "all 5 Deployments are Available"` — `kubectl get -n services deployment <svc>-stable -o jsonpath='{.status.conditions[?(@.type=="Available")].status}'`.
3. `@test "all 5 Services have endpoints"` — `kubectl get -n services endpoints <svc> -o jsonpath='{.subsets[0].addresses[*].ip}' | wc -w` ≥ 1.
4. `@test "Restate Admin reports 5 deployments registered"` — `curl -sf http://localhost:9070/deployments | jq '.deployments | length'` == 5. (kind exposes Restate admin on host port 9070 per 1.1's port mapping.)
5. `@test "POST /api/orders via Istio returns 200"` — `curl -sf -X POST -H 'content-type: application/json' -d '{"userId":"u1","sku":"sku-1","quantity":1,"amount":100}' http://localhost:8080/api/orders` returns 200 with a JSON body containing an `id`.

Run with `make smoke-services`. Prerequisites (the user runs in order): `make up` → `make build-services` → `make build-images` → `make load-images` → `make deploy-services`.

## Make targets

New targets in the existing `Makefile`:

| target | what it does |
|---|---|
| `build-images` | `bash deploy/images/build-and-load.sh build` — build all 5 docker images |
| `load-images` | `bash deploy/images/build-and-load.sh load` — `kind load docker-image` all 5 |
| `images` | build-images + load-images (sequential) |
| `deploy-services` | `bash deploy/services/deploy.sh` — KafkaTopics + Helm install all 5 + routing + ingress |
| `undeploy-services` | `bash deploy/services/undeploy.sh` — inverse |
| `smoke-services` | `bats tests/services/deploy.bats` |

Existing targets (`up`, `down`, `status`, `verify`, `build-services`, etc.) untouched.

The README's "operator workflow" section maps to the Phase 1 spec's `make up && make build && make load && make deploy && make e2e` sequence as: `make up && make build-services && make build-images && make load-images && make deploy-services && make smoke-services` (e2e is Plan 1.5).

## Data flow at runtime (1.3.b substrate, no canary)

For a no-header request `POST /api/orders`:

```
client (host:8080)
  → Istio Ingress Gateway (NodePort 30080 → containerPort 80)
  → edge VirtualService matches /api/orders prefix
  → routes to order-service.services.svc:3001
  → order-service VirtualService default rule → subset: stable
  → order-service Service round-robins to a Ready stable pod
  → order-service handler logic:
    ├─→ HTTP fan-out via axios:
    │     ├─→ inventory-service.services.svc:8082 (POST /reservations)
    │     ├─→ payment-service.services.svc:8081 (POST /charges)
    │     └─→ notification-service.services.svc:3002 (POST /notifications)
    │   each downstream → its VS default rule → its stable pod
    │   each downstream calls audit-service via Restate Ingress (HTTP fan-out is for order-service only; per Plan 1.3.a option β)
    └─→ Kafka producer publishes to orders.events (consumed by stable consumers)
```

For Restate handler invocations from a stable service:

```
service A
  → Restate Ingress at restate.restate.svc:8080
  → Restate routes to registered handler URL
  → http://<service-B>.services.svc:<restate-handler-port>
  → service B's Restate handler executes
```

Plan 1.4 will add the `x-canary: true` header path on top of this substrate.

## Error handling and edge cases

**A. Restate registration timing.** The post-install Job's `until curl /discover` loop polls until the service is up, so the Job tolerates the service's own startup latency. If the service never starts (e.g., misconfigured), `backoffLimit: 6` causes the Job to fail and `helm install` exits non-zero. Acceptable for a dev cluster.

**B. KafkaTopic readiness.** `kubectl wait --for=condition=Ready ... kafkatopics --all` blocks deploy until Strimzi's user-operator has created topics. If the user-operator isn't healthy, this surfaces immediately rather than letting consumers crash-loop on missing topics.

**C. Deployment rollout failures.** `kubectl wait --for=condition=Available --timeout=180s` blocks deploy until each stable Deployment has at least one Ready pod. If a service crash-loops, the script fails with a clear error.

**D. Empty canary subset.** `DestinationRule` defines both subsets up front, but the `canary` subset has no endpoints in 1.3.b (no canary Deployment exists). The default-only `VirtualService` never routes to canary, so this is harmless. Plan 1.4 fills it by deploying a `version: canary` Deployment.

**E. Single Kafka broker, no replication.** Topic `replicas: 1` and `default.replication.factor: 1` are intentional for single-broker dev. Production guidance is Phase 5.

**F. Image rebuild + redeploy.** `imagePullPolicy: IfNotPresent` + tag `:dev` means rebuilding an image with the same tag and re-loading into kind requires a Deployment restart to pick up the new image. The user can `kubectl rollout restart deploy/<svc>-stable -n services`. Re-running `make deploy-services` does not restart pods if the chart values haven't changed.

**G. Service-to-Service plaintext.** Istio is in PERMISSIVE mTLS by default (Phase 1.1's install). Service-to-service traffic gets sidecar proxying but not enforced mTLS. Tightening is Phase 4.

## Testing strategy

Three layers:

1. **No new unit tests for service code.** 1.3.a's unit tests already cover business logic. The only code change in 1.3.b is adding `/health` routes (Node) and actuator config (Java); these are integration concerns covered by the smoke test.
2. **No new library tests.** 1.2's library tests stand.
3. **Bats smoke test (`tests/services/deploy.bats`)** — verifies the deployed substrate works end to end after `make deploy-services`. Five assertions per the smoke verification section above. Runs in roughly 60 seconds against the running cluster.

## Open questions and assumptions

- **Spring Boot Actuator on Spring Boot 4.0.4.** Assume actuator's `health/liveness` and `health/readiness` paths and behavior are stable across Spring Boot 3 → 4. Implementation will verify; unlikely to break.
- **Restate Admin's `force: true` on `POST /deployments`.** Assume Restate 1.6.2 accepts this contract for idempotent re-registration. Implementation will verify against the running Restate.
- **`pnpm deploy --prod` flattens workspace deps correctly.** Assume the resulting directory has resolvable `@canary/lib-node` and `@canary/restate-defs-node` modules. Implementation will verify by `node dist/index.js` inside the built image.
- **Kind image cache invalidation.** Assume `kind load docker-image <ref>` overwrites an existing same-tag image in the cache. If not, the user can `kind delete cluster && make up` to start clean.

## Deliverable summary

After 1.3.b is merged, running `make up && make build-services && make images && make deploy-services && make smoke-services` from a clean kind cluster will produce: five service Deployments running on the cluster, all five Kafka topics provisioned, all five services registered with Restate Admin, Istio routing 100% of traffic to stable subsets, and a smoke test verifying that `POST /api/orders` succeeds end to end through the Istio Ingress Gateway. The `canary-overlay.yaml` values file is checked in but not applied — Plan 1.4's `canary-ctl` will use it. No canary Deployments exist; no VirtualService has a canary header rule. The substrate is ready for Plan 1.4 to add canary lifecycle management.
