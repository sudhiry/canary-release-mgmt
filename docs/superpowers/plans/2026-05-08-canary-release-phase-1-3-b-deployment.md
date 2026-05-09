# Phase 1.3.b — Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the five 1.3.a domain services to the local kind cluster behind Istio routing, with stable-only traffic, leaving the substrate ready for Plan 1.4 (canary-ctl) to layer canary mechanics on top.

**Architecture:** One shared Helm chart renders per-service `Deployment + Service + ServiceAccount + ConfigMap` plus a post-install Job that registers the service's Restate handlers with the Restate Admin API. Routing config (`DestinationRule` + default-only `VirtualService`) lives as raw YAML under `deploy/routing/` so Plan 1.4's canary-ctl can mutate the VS without conflicting with Helm release ownership. Five `KafkaTopic` CRDs provision the topics consumed by the services. Multi-stage Dockerfiles produce images tagged `canary-release-mgmt/<svc>:dev`, loaded into the kind image cache. A bats smoke test verifies `POST /api/orders` works end to end through Istio.

**Tech Stack:** Helm v3, Istio 1.29, Strimzi 0.45 (KafkaTopic CRDs), Restate 1.6.2 Admin API, Spring Boot 4.0.4 + Actuator, Express 4 + supertest, kind 0.24, Docker, bats, bash, Make.

---

## File structure

### New files (created by this plan)

```
deploy/
├── images/
│   └── build-and-load.sh                      # bash script, builds + loads all 5 images
├── kafka/topics/
│   ├── audit.events.yaml                      # Strimzi KafkaTopic CRD
│   ├── inventory.events.yaml
│   ├── notifications.events.yaml
│   ├── orders.events.yaml
│   └── payments.events.yaml
├── helm/
│   ├── service-chart/
│   │   ├── Chart.yaml
│   │   ├── values.yaml                        # defaults
│   │   └── templates/
│   │       ├── _helpers.tpl
│   │       ├── serviceaccount.yaml
│   │       ├── configmap.yaml
│   │       ├── deployment.yaml
│   │       ├── service.yaml
│   │       └── restate-register-job.yaml
│   └── values/
│       ├── audit-service.yaml
│       ├── canary-overlay.yaml
│       ├── inventory-service.yaml
│       ├── notification-service.yaml
│       ├── order-service.yaml
│       └── payment-service.yaml
├── routing/
│   ├── destination-rules/
│   │   ├── audit-service.yaml
│   │   ├── inventory-service.yaml
│   │   ├── notification-service.yaml
│   │   ├── order-service.yaml
│   │   └── payment-service.yaml
│   ├── virtual-services/
│   │   ├── audit-service.yaml                 # default rule only
│   │   ├── inventory-service.yaml
│   │   ├── notification-service.yaml
│   │   ├── order-service.yaml
│   │   └── payment-service.yaml
│   └── ingress/
│       ├── gateway.yaml                       # Istio Gateway
│       └── edge-virtualservice.yaml           # /api/orders → order-service
└── services/
    ├── deploy.sh                              # orchestrates topics + helm + routing
    └── undeploy.sh                            # inverse

services/audit-service/Dockerfile              # multi-stage Java
services/payment-service/Dockerfile
services/inventory-service/Dockerfile
services/order-service/Dockerfile              # multi-stage Node
services/notification-service/Dockerfile

tests/services/deploy.bats                     # 5-assertion smoke test
```

### Modified files

```
gradle/libs.versions.toml                                # + actuator alias
services/audit-service/build.gradle.kts                  # + actuator dependency
services/payment-service/build.gradle.kts                # + actuator dependency
services/inventory-service/build.gradle.kts              # + actuator dependency
services/audit-service/src/main/resources/application.yml    # expose actuator endpoints
services/payment-service/src/main/resources/application.yml
services/inventory-service/src/main/resources/application.yml
services/order-service/src/http.ts                       # GET /health
services/order-service/src/__tests__/http.test.ts        # health route test
services/notification-service/src/http.ts                # GET /health
services/notification-service/src/__tests__/http.test.ts # health route test
Makefile                                                  # 6 new targets
README.md                                                 # 1.3.b section + run instructions
```

---

## Task 1: Add Spring Boot Actuator dependency to libs catalog and the 3 Java services

**Files:**
- Modify: `gradle/libs.versions.toml`
- Modify: `services/audit-service/build.gradle.kts`
- Modify: `services/payment-service/build.gradle.kts`
- Modify: `services/inventory-service/build.gradle.kts`

- [ ] **Step 1: Add actuator alias to the version catalog**

In `gradle/libs.versions.toml`, under `[libraries]`, add this line right after `spring-boot-starter-test`:

```toml
spring-boot-starter-actuator   = { module = "org.springframework.boot:spring-boot-starter-actuator" }
```

(`springBoot` version is managed transitively via the `spring-dependency-management` plugin, so no `version.ref` is needed — same as the other `spring-boot-starter-*` libraries.)

- [ ] **Step 2: Add the dependency to all 3 Java services**

In each of these three files, add `implementation(libs.spring.boot.starter.actuator)` to the existing `dependencies { ... }` block, immediately after `implementation(libs.spring.boot.starter.web)`:

- `services/audit-service/build.gradle.kts`
- `services/payment-service/build.gradle.kts`
- `services/inventory-service/build.gradle.kts`

Resulting block (example for audit-service; the other two follow the same pattern):

```kotlin
dependencies {
    implementation(project(":platform:lib-java"))
    implementation(project(":platform:restate-defs-java"))
    implementation(libs.spring.boot.starter.web)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.spring.kafka)
    implementation(libs.restate.sdk.api)
    implementation(libs.restate.sdk.common)
    implementation(libs.restate.sdk.http.vertx)
    implementation(libs.vertx.core)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}
```

- [ ] **Step 3: Verify the dependency resolves**

Run: `./gradlew :services:audit-service:dependencies --configuration runtimeClasspath | grep actuator`

Expected: at least one line containing `org.springframework.boot:spring-boot-starter-actuator`.

- [ ] **Step 4: Build all 3 services to confirm nothing breaks**

Run: `./gradlew :services:audit-service:test :services:payment-service:test :services:inventory-service:test --quiet`

Expected: BUILD SUCCESSFUL with the same number of tests as before (no behavior change yet).

- [ ] **Step 5: Commit**

```bash
git add gradle/libs.versions.toml services/audit-service/build.gradle.kts services/payment-service/build.gradle.kts services/inventory-service/build.gradle.kts
git commit -m "feat(services): add spring-boot-starter-actuator dependency to 3 Java services"
```

---

## Task 2: Configure Actuator health probes in the 3 Java services

**Files:**
- Modify: `services/audit-service/src/main/resources/application.yml`
- Modify: `services/payment-service/src/main/resources/application.yml`
- Modify: `services/inventory-service/src/main/resources/application.yml`

- [ ] **Step 1: Add management config to all 3 application.yml files**

In each of the three files, append the following block at the end (after the existing `app:` block):

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

This exposes `/actuator/health/liveness` and `/actuator/health/readiness` on the same HTTP port as the service. `show-details: never` keeps the probe response opaque to outside callers (no internal details leaked); k8s only needs the HTTP status code.

- [ ] **Step 2: Run a smoke check by booting one service in test mode**

Run: `./gradlew :services:audit-service:bootRun --quiet &`

Wait roughly 10 seconds, then:

```bash
curl -sf http://localhost:8083/actuator/health/liveness
curl -sf http://localhost:8083/actuator/health/readiness
```

Expected: each returns `{"status":"UP"}`. If liveness returns but readiness 503s, that's a Spring Boot transient state — wait 5s and retry. If both consistently fail, check logs and verify the YAML indentation.

Stop the bootRun: `kill %1` (or `pkill -f audit-service`).

- [ ] **Step 3: Run all Java tests**

Run: `./gradlew :services:audit-service:test :services:payment-service:test :services:inventory-service:test --quiet`

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add services/audit-service/src/main/resources/application.yml services/payment-service/src/main/resources/application.yml services/inventory-service/src/main/resources/application.yml
git commit -m "feat(services): expose actuator health/liveness and health/readiness endpoints"
```

---

## Task 3: Add /health route to order-service

**Files:**
- Modify: `services/order-service/src/http.ts`
- Modify: `services/order-service/src/__tests__/http.test.ts`

- [ ] **Step 1: Write the failing test**

In `services/order-service/src/__tests__/http.test.ts`, add this test inside the existing `describe("HTTP routes", ...)` block, alongside the other route tests. Use the existing `mockAxios(null)` helper (defined at the top of the file) for consistency with the other tests that don't exercise the clients:

```typescript
  it("GET /health returns 200 with {ok: true}", async () => {
    const app = setupHttp({
      clients: {
        inventory: mockAxios(null),
        payment: mockAxios(null),
        notification: mockAxios(null),
      },
    });
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `pnpm --filter @canary/order-service test`

Expected: the new test fails with a 404 or similar (the route doesn't exist yet).

- [ ] **Step 3: Add the /health route to setupHttp**

In `services/order-service/src/http.ts`, inside the `setupHttp` function, register `/health` **BEFORE** `app.use(xCanaryMiddleware);` (immediately after `app.use(express.json());`). This keeps the k8s probe path infrastructure-transparent — it does not pass through the canary header propagation middleware (probes carry no `x-canary` header and shouldn't trigger ALS context creation):

```typescript
  app.use(express.json());
  app.get("/health", (_req, res) => res.json({ ok: true }));
  app.use(xCanaryMiddleware);
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `pnpm --filter @canary/order-service test`

Expected: all tests pass, including the new `/health` test.

- [ ] **Step 5: Commit**

```bash
git add services/order-service/src/http.ts services/order-service/src/__tests__/http.test.ts
git commit -m "feat(order-service): add GET /health route for k8s probes"
```

---

## Task 4: Add /health route to notification-service

**Files:**
- Modify: `services/notification-service/src/http.ts`
- Modify: `services/notification-service/src/__tests__/http.test.ts`

- [ ] **Step 1: Write the failing test**

In `services/notification-service/src/__tests__/http.test.ts`, add this test alongside the other route tests in the existing `describe` block. Use the same mock pattern that other tests in that file use for unused clients (inspect the file first; if a `mockAxios()` helper exists, prefer it for consistency):

```typescript
  it("GET /health returns 200 with {ok: true}", async () => {
    const app = setupHttp({
      ingressClient: { post: () => Promise.reject() } as unknown as AxiosInstance,
    });
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });
```

If the file has a `mockAxios()` helper (similar to order-service's), substitute `ingressClient: mockAxios(null)` for the inline `Promise.reject()` form.

- [ ] **Step 2: Run the test to confirm it fails**

Run: `pnpm --filter @canary/notification-service test`

Expected: the new test fails.

- [ ] **Step 3: Add the /health route**

In `services/notification-service/src/http.ts`, inside `setupHttp`, register `/health` **BEFORE** `app.use(xCanaryMiddleware);` (immediately after `app.use(express.json());`). This keeps the k8s probe path infrastructure-transparent — it does not pass through the canary header propagation middleware:

```typescript
  app.use(express.json());
  app.get("/health", (_req, res) => res.json({ ok: true }));
  app.use(xCanaryMiddleware);
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `pnpm --filter @canary/notification-service test`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/notification-service/src/http.ts services/notification-service/src/__tests__/http.test.ts
git commit -m "feat(notification-service): add GET /health route for k8s probes"
```

---

## Task 5: Verify make verify still passes (full project regression)

**Files:** none changed; just running the full test suite.

- [ ] **Step 1: Run the project-wide verify**

Run from repo root: `make verify`

Expected: BUILD SUCCESSFUL (Java) and all vitest suites pass (Node). Approximately 118+ tests, all green.

- [ ] **Step 2: If anything fails**

Stop and report the failure with the relevant test name + first 20 lines of the error output. Do not proceed to Task 6 until this passes.

- [ ] **Step 3: No commit (no changes)**

This task is a verification gate, not a code change.

---

## Task 6: Create the 5 Strimzi KafkaTopic CRDs

**Files:**
- Create: `deploy/kafka/topics/audit.events.yaml`
- Create: `deploy/kafka/topics/inventory.events.yaml`
- Create: `deploy/kafka/topics/notifications.events.yaml`
- Create: `deploy/kafka/topics/orders.events.yaml`
- Create: `deploy/kafka/topics/payments.events.yaml`

- [ ] **Step 1: Create the topics directory and write all 5 files**

For each topic name in the list `audit.events`, `inventory.events`, `notifications.events`, `orders.events`, `payments.events`, write a file at `deploy/kafka/topics/<topic-name>.yaml` with this content (substitute `<topic-name>`):

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: <topic-name>
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  partitions: 1
  replicas: 1
```

- [ ] **Step 2: Validate each file with kubectl client-side dry-run**

For each file, run:

```bash
for f in deploy/kafka/topics/*.yaml; do
  kubectl apply --dry-run=client -f "$f" >/dev/null && echo "OK: $f" || { echo "FAIL: $f"; exit 1; }
done
```

Expected: 5 lines `OK: deploy/kafka/topics/<topic>.yaml`. (Client-side dry-run only validates YAML schema, not the CRD itself; the CRD validation happens at apply time when Strimzi is installed in the cluster.)

- [ ] **Step 3: Commit**

```bash
git add deploy/kafka/topics/
git commit -m "feat(kafka): add 5 KafkaTopic CRDs (orders/payments/inventory/notifications/audit events)"
```

---

## Task 7: Helm chart skeleton (Chart.yaml, values.yaml, _helpers.tpl)

**Files:**
- Create: `deploy/helm/service-chart/Chart.yaml`
- Create: `deploy/helm/service-chart/values.yaml`
- Create: `deploy/helm/service-chart/templates/_helpers.tpl`

- [ ] **Step 1: Write Chart.yaml**

Create `deploy/helm/service-chart/Chart.yaml`:

```yaml
apiVersion: v2
name: service-chart
description: Shared chart for all 5 canary-release-mgmt services (stable + canary)
type: application
version: 0.1.0
appVersion: "0.1.0"
```

- [ ] **Step 2: Write values.yaml (defaults)**

Create `deploy/helm/service-chart/values.yaml`:

```yaml
# Required per release; no default
serviceName: ""

image:
  repository: canary-release-mgmt
  tag: dev
  pullPolicy: IfNotPresent

# stable | canary; canary overlay sets this to "canary"
version: stable

replicas: 1

# Container ports — overridden per service
ports:
  http: 8080
  restateHandler: 9080

# Shared env vars — base; per-service files extend, canary overlay overrides
env:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
  RESTATE_INGRESS_URL: http://restate.restate.svc.cluster.local:8080
  RESTATE_ADMIN_URL: http://restate.restate.svc.cluster.local:9070
  KAFKA_CONSUMERS_ENABLED: "true"
  RESTATE_REGISTER_HANDLERS: "true"

# Service-specific extra env (e.g., order-service downstream URLs)
extraEnv: {}

# Probe paths — Java services override to /actuator/health/{liveness,readiness}
probes:
  liveness:
    path: /health
  readiness:
    path: /health

# Resource defaults (Node profile); Java services override to higher
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 256Mi

# Whether to render the post-install Restate-registration Job (canary skips)
restate:
  registerEndpoint: true
```

- [ ] **Step 3: Write _helpers.tpl**

Create `deploy/helm/service-chart/templates/_helpers.tpl`:

```yaml
{{/*
Common labels applied to every resource managed by this chart.
*/}}
{{- define "service-chart.labels" -}}
app: {{ .Values.serviceName }}
version: {{ .Values.version | default "stable" }}
managed-by: helm
{{- end -}}

{{/*
Selector labels (subset of common labels) — used by Deployment.spec.selector
and matched by the headless Service via app:.
*/}}
{{- define "service-chart.selectorLabels" -}}
app: {{ .Values.serviceName }}
version: {{ .Values.version | default "stable" }}
{{- end -}}

{{/*
Resource name = serviceName + version (e.g., audit-service-stable, audit-service-canary).
Used for Deployment name; Service name is plain serviceName.
*/}}
{{- define "service-chart.resourceName" -}}
{{ .Values.serviceName }}-{{ .Values.version | default "stable" }}
{{- end -}}
```

- [ ] **Step 4: Validate the chart with helm lint**

Run from repo root: `helm lint deploy/helm/service-chart --set serviceName=audit-service`

Expected: `1 chart(s) linted, 0 chart(s) failed`. (Lint with a dummy serviceName to satisfy the required-but-not-yet-validated value.)

- [ ] **Step 5: Commit**

```bash
git add deploy/helm/service-chart/
git commit -m "feat(helm): scaffold service-chart with Chart.yaml + values.yaml + _helpers.tpl"
```

---

## Task 8: ServiceAccount and ConfigMap templates

**Files:**
- Create: `deploy/helm/service-chart/templates/serviceaccount.yaml`
- Create: `deploy/helm/service-chart/templates/configmap.yaml`

- [ ] **Step 1: Write serviceaccount.yaml**

Create `deploy/helm/service-chart/templates/serviceaccount.yaml`:

```yaml
{{- if eq (.Values.version | default "stable") "stable" -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ .Values.serviceName }}
  namespace: {{ .Release.Namespace }}
  labels:
    app: {{ .Values.serviceName }}
    managed-by: helm
{{- end }}
```

The `ServiceAccount` is shared across stable and canary — both Deployments run as `serviceAccountName: <serviceName>`. We gate this template on `version == stable` so only the stable release owns the SA; canary skips creation to avoid Helm ownership conflicts. Labels intentionally omit `version:` since this resource is shared.

- [ ] **Step 2: Write configmap.yaml**

Create `deploy/helm/service-chart/templates/configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "service-chart.resourceName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "service-chart.labels" . | nindent 4 }}
data:
{{- range $k, $v := .Values.env }}
  {{ $k }}: {{ $v | quote }}
{{- end }}
{{- range $k, $v := .Values.extraEnv }}
  {{ $k }}: {{ $v | quote }}
{{- end }}
```

The ConfigMap is named per-version (`audit-service-stable` vs `audit-service-canary`) so the canary overlay can produce a separate ConfigMap with overridden values (e.g., `KAFKA_CONSUMERS_ENABLED=false`).

- [ ] **Step 3: Render with helm template to confirm output is well-formed**

Run: `helm template test deploy/helm/service-chart --set serviceName=audit-service --set ports.http=8083 --set ports.restateHandler=9083 | head -50`

Expected: see a `ServiceAccount` named `audit-service` and a `ConfigMap` named `audit-service-stable` containing the default env vars (`KAFKA_BOOTSTRAP_SERVERS`, etc.).

- [ ] **Step 4: Commit**

```bash
git add deploy/helm/service-chart/templates/serviceaccount.yaml deploy/helm/service-chart/templates/configmap.yaml
git commit -m "feat(helm): add ServiceAccount and ConfigMap templates to service-chart"
```

---

## Task 9: Deployment template

**Files:**
- Create: `deploy/helm/service-chart/templates/deployment.yaml`

- [ ] **Step 1: Write deployment.yaml**

Create `deploy/helm/service-chart/templates/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "service-chart.resourceName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "service-chart.labels" . | nindent 4 }}
spec:
  replicas: {{ .Values.replicas }}
  selector:
    matchLabels:
      {{- include "service-chart.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "service-chart.labels" . | nindent 8 }}
    spec:
      serviceAccountName: {{ .Values.serviceName }}
      containers:
        - name: {{ .Values.serviceName }}
          image: "{{ .Values.image.repository }}/{{ .Values.serviceName }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          ports:
            - name: http
              containerPort: {{ .Values.ports.http }}
            - name: restate
              containerPort: {{ .Values.ports.restateHandler }}
          envFrom:
            - configMapRef:
                name: {{ include "service-chart.resourceName" . }}
          livenessProbe:
            httpGet:
              path: {{ .Values.probes.liveness.path }}
              port: http
            initialDelaySeconds: 20
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: {{ .Values.probes.readiness.path }}
              port: http
            initialDelaySeconds: 10
            periodSeconds: 5
            failureThreshold: 3
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
```

Key points:
- The Deployment is named `<serviceName>-<version>` so stable and canary are separate Deployments.
- Selector includes both `app` and `version` labels — guarantees stable and canary Deployments don't fight over the same Pods.
- `envFrom: configMapRef` reads all env from the per-Deployment ConfigMap (created in Task 8).
- Probes use the named port `http` (defined in `ports`) — the path is overridable per service (Java uses `/actuator/health/...`, Node uses `/health`).

- [ ] **Step 2: Render with helm template and verify shape**

Run:
```bash
helm template test deploy/helm/service-chart \
  --set serviceName=audit-service --set ports.http=8083 --set ports.restateHandler=9083 \
  --set probes.liveness.path=/actuator/health/liveness \
  --set probes.readiness.path=/actuator/health/readiness \
  -s templates/deployment.yaml
```

Expected output should include:
- `name: audit-service-stable`
- `selector.matchLabels.app: audit-service` and `version: stable`
- `image: "canary-release-mgmt/audit-service:dev"`
- `livenessProbe.httpGet.path: /actuator/health/liveness`

- [ ] **Step 3: Render the canary variant to confirm version flips**

Run:
```bash
helm template test deploy/helm/service-chart \
  --set serviceName=audit-service --set version=canary \
  --set ports.http=8083 --set ports.restateHandler=9083 \
  -s templates/deployment.yaml | grep -E "(name:|version:)" | head -10
```

Expected: `name: audit-service-canary`, `version: canary` labels.

- [ ] **Step 4: Commit**

```bash
git add deploy/helm/service-chart/templates/deployment.yaml
git commit -m "feat(helm): add Deployment template (stable/canary toggled by .Values.version)"
```

---

## Task 10: Service template

**Files:**
- Create: `deploy/helm/service-chart/templates/service.yaml`

- [ ] **Step 1: Write service.yaml**

Create `deploy/helm/service-chart/templates/service.yaml`:

```yaml
{{- if eq (.Values.version | default "stable") "stable" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ .Values.serviceName }}
  namespace: {{ .Release.Namespace }}
  labels:
    app: {{ .Values.serviceName }}
spec:
  type: ClusterIP
  selector:
    app: {{ .Values.serviceName }}
  ports:
    - name: http
      port: {{ .Values.ports.http }}
      targetPort: http
    - name: restate
      port: {{ .Values.ports.restateHandler }}
      targetPort: restate
{{- end }}
```

Two critical points:
1. The selector is **just `app: <serviceName>`** (no `version`) — so the Service spans both stable and canary Pods. Istio's DestinationRule + VirtualService is what splits traffic by version subset.
2. The whole template is gated on `version == stable` — only the stable release owns this Service. The canary release shares it and must not try to create a duplicate.

- [ ] **Step 2: Render and verify**

Run:
```bash
helm template test deploy/helm/service-chart \
  --set serviceName=order-service --set ports.http=3001 --set ports.restateHandler=9084 \
  -s templates/service.yaml
```

Expected output:
- `name: order-service` (no version suffix)
- `selector: app: order-service` only
- two ports `http` (3001) and `restate` (9084)

- [ ] **Step 3: Commit**

```bash
git add deploy/helm/service-chart/templates/service.yaml
git commit -m "feat(helm): add Service template (selects both stable and canary by app: label)"
```

---

## Task 11: Restate-register Job template (Helm post-install hook)

**Files:**
- Create: `deploy/helm/service-chart/templates/restate-register-job.yaml`

- [ ] **Step 1: Write restate-register-job.yaml**

Create `deploy/helm/service-chart/templates/restate-register-job.yaml`:

```yaml
{{- if .Values.restate.registerEndpoint }}
apiVersion: batch/v1
kind: Job
metadata:
  name: register-restate-{{ .Values.serviceName }}
  namespace: {{ .Release.Namespace }}
  labels:
    app: {{ .Values.serviceName }}
    component: restate-register
  annotations:
    helm.sh/hook: post-install,post-upgrade
    helm.sh/hook-weight: "10"
    helm.sh/hook-delete-policy: hook-succeeded,before-hook-creation
spec:
  backoffLimit: 6
  template:
    metadata:
      labels:
        app: {{ .Values.serviceName }}
        component: restate-register
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
              SVC="{{ .Values.serviceName }}.{{ .Release.Namespace }}.svc.cluster.local"
              RESTATE_PORT="{{ .Values.ports.restateHandler }}"
              ADMIN="{{ .Values.env.RESTATE_ADMIN_URL }}"
              echo "Waiting for ${SVC}:${RESTATE_PORT}/discover ..."
              # Restate SDK serves /discover (the OpenAPI-like discovery doc) once the handler endpoint is listening
              until curl -sf -o /dev/null "http://${SVC}:${RESTATE_PORT}/discover"; do
                sleep 2
              done
              echo "Registering ${SVC}:${RESTATE_PORT} with Restate Admin at ${ADMIN}"
              curl -sf -X POST "${ADMIN}/deployments" \
                -H 'content-type: application/json' \
                -d "{\"uri\":\"http://${SVC}:${RESTATE_PORT}\",\"force\":true}" \
                && echo "Registered."
{{- end }}
```

Notes:
- Job is gated by `.Values.restate.registerEndpoint` — canary overlay sets this to `false`, so canary releases skip registration.
- `helm.sh/hook: post-install,post-upgrade` runs the Job after every install/upgrade.
- `helm.sh/hook-delete-policy: hook-succeeded,before-hook-creation` deletes the Job on success and also before next creation (idempotent across upgrades).
- `curl /discover` is the Restate SDK's discovery endpoint (returns 200 once the handler endpoint server is listening). It's the most reliable readiness signal that doesn't require adding a separate readiness route inside the service code.
- `force: true` on `POST /deployments` causes Restate to overwrite an existing same-URI registration — makes the Job safe to re-run.

- [ ] **Step 2: Render and verify shape**

Run:
```bash
helm template test deploy/helm/service-chart \
  --set serviceName=audit-service --set ports.http=8083 --set ports.restateHandler=9083 \
  -s templates/restate-register-job.yaml
```

Expected: a `batch/v1 Job` named `register-restate-audit-service` with the correct annotations and a script containing `audit-service.default.svc.cluster.local` (the `default` placeholder is from `--set` not specifying namespace; in real install it'll be `services`).

- [ ] **Step 3: Render with registerEndpoint=false to confirm Job is skipped**

Run:
```bash
helm template test deploy/helm/service-chart \
  --set serviceName=audit-service --set ports.http=8083 --set ports.restateHandler=9083 \
  --set restate.registerEndpoint=false \
  -s templates/restate-register-job.yaml
```

Expected: empty output (the `{{- if ... }}` block produces nothing).

- [ ] **Step 4: Commit**

```bash
git add deploy/helm/service-chart/templates/restate-register-job.yaml
git commit -m "feat(helm): add post-install Helm hook Job that registers handlers with Restate Admin"
```

---

## Task 12: Per-service values files (5)

**Files:**
- Create: `deploy/helm/values/audit-service.yaml`
- Create: `deploy/helm/values/payment-service.yaml`
- Create: `deploy/helm/values/inventory-service.yaml`
- Create: `deploy/helm/values/order-service.yaml`
- Create: `deploy/helm/values/notification-service.yaml`

- [ ] **Step 1: audit-service**

Create `deploy/helm/values/audit-service.yaml`:

```yaml
serviceName: audit-service
ports:
  http: 8083
  restateHandler: 9083
probes:
  liveness:
    path: /actuator/health/liveness
  readiness:
    path: /actuator/health/readiness
resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 512Mi
```

- [ ] **Step 2: payment-service**

Create `deploy/helm/values/payment-service.yaml`:

```yaml
serviceName: payment-service
ports:
  http: 8081
  restateHandler: 9081
probes:
  liveness:
    path: /actuator/health/liveness
  readiness:
    path: /actuator/health/readiness
resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 512Mi
```

- [ ] **Step 3: inventory-service**

Create `deploy/helm/values/inventory-service.yaml`:

```yaml
serviceName: inventory-service
ports:
  http: 8082
  restateHandler: 9082
probes:
  liveness:
    path: /actuator/health/liveness
  readiness:
    path: /actuator/health/readiness
resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 512Mi
```

- [ ] **Step 4: order-service**

Create `deploy/helm/values/order-service.yaml`:

```yaml
serviceName: order-service
ports:
  http: 3001
  restateHandler: 9084
extraEnv:
  INVENTORY_URL: http://inventory-service.services.svc.cluster.local:8082
  PAYMENT_URL: http://payment-service.services.svc.cluster.local:8081
  NOTIFICATION_URL: http://notification-service.services.svc.cluster.local:3002
probes:
  liveness:
    path: /health
  readiness:
    path: /health
```

(Resource defaults from `values.yaml` are appropriate for Node services; no override.)

- [ ] **Step 5: notification-service**

Create `deploy/helm/values/notification-service.yaml`:

```yaml
serviceName: notification-service
ports:
  http: 3002
  restateHandler: 9085
probes:
  liveness:
    path: /health
  readiness:
    path: /health
```

- [ ] **Step 6: Render each release to verify**

Run for each service:

```bash
for svc in audit-service payment-service inventory-service order-service notification-service; do
  echo "=== $svc ==="
  helm template "$svc" deploy/helm/service-chart -f "deploy/helm/values/$svc.yaml" \
    | grep -E "^(kind|  name|        path):" | head -20
done
```

Expected: each output shows `Deployment`, `Service`, `ServiceAccount`, `ConfigMap`, `Job` named with the right service name; probe paths matching the values file.

- [ ] **Step 7: Commit**

```bash
git add deploy/helm/values/audit-service.yaml deploy/helm/values/payment-service.yaml deploy/helm/values/inventory-service.yaml deploy/helm/values/order-service.yaml deploy/helm/values/notification-service.yaml
git commit -m "feat(helm): add per-service values files for all 5 services"
```

---

## Task 13: Canary overlay values file

**Files:**
- Create: `deploy/helm/values/canary-overlay.yaml`

- [ ] **Step 1: Write canary-overlay.yaml**

Create `deploy/helm/values/canary-overlay.yaml`:

```yaml
# Canary overlay — applied IN ADDITION to a per-service values file by
# Plan 1.4's canary-ctl. Hard-codes the canary contract from the Phase 1 spec:
#   - canary pods do not consume Kafka (no partition stealing)
#   - canary pods do not register Restate handlers (only stable owns the registry)
#   - canary pods carry the version: canary label so Istio's DestinationRule
#     subset routing can target them
#
# Plan 1.4 runs roughly:
#   helm upgrade --install <svc>-canary deploy/helm/service-chart \
#     -f deploy/helm/values/<svc>.yaml \
#     -f deploy/helm/values/canary-overlay.yaml \
#     --set image.tag=<canary-tag> -n services

version: canary
replicas: 1
env:
  KAFKA_CONSUMERS_ENABLED: "false"
  RESTATE_REGISTER_HANDLERS: "false"
restate:
  registerEndpoint: false
```

Note: this overlay is checked in but NOT applied in 1.3.b. Plan 1.4's canary-ctl will use it.

- [ ] **Step 2: Render a canary release dry-run to verify the overlay works**

Run:
```bash
helm template audit-service-canary deploy/helm/service-chart \
  -f deploy/helm/values/audit-service.yaml \
  -f deploy/helm/values/canary-overlay.yaml \
  --set image.tag=canary-001 \
  | grep -E "(name:|version:|KAFKA_CONSUMERS_ENABLED|RESTATE_REGISTER_HANDLERS)" \
  | head -20
```

Expected: contains `version: canary`, `KAFKA_CONSUMERS_ENABLED: "false"`, `RESTATE_REGISTER_HANDLERS: "false"`.

Verify the canary release also skips Service, ServiceAccount, and Job (since stable owns them):

```bash
for kind in Job "ServiceAccount" "Service$"; do
  count=$(helm template audit-service-canary deploy/helm/service-chart \
    -f deploy/helm/values/audit-service.yaml \
    -f deploy/helm/values/canary-overlay.yaml \
    | grep -cE "^kind: ${kind}")
  echo "$kind: $count"
done
```

Expected output:
```
Job: 0
ServiceAccount: 0
Service$: 0
```

(All three should be `0` for canary — the canary release contains only Deployment + ConfigMap.)

- [ ] **Step 3: Commit**

```bash
git add deploy/helm/values/canary-overlay.yaml
git commit -m "feat(helm): add canary-overlay values file (consumed by Plan 1.4 canary-ctl)"
```

---

## Task 14: DestinationRule files (5)

**Files:**
- Create: `deploy/routing/destination-rules/audit-service.yaml`
- Create: `deploy/routing/destination-rules/payment-service.yaml`
- Create: `deploy/routing/destination-rules/inventory-service.yaml`
- Create: `deploy/routing/destination-rules/order-service.yaml`
- Create: `deploy/routing/destination-rules/notification-service.yaml`

- [ ] **Step 1: Write all 5 DestinationRule files**

For each service in the list `audit-service`, `payment-service`, `inventory-service`, `order-service`, `notification-service`, write `deploy/routing/destination-rules/<svc>.yaml` with this content (substitute `<svc>`):

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

Both subsets are pre-defined; the canary subset's endpoint set is empty in 1.3.b (no canary Deployment exists) but Istio tolerates this fine.

- [ ] **Step 2: Validate all 5 files with kubectl client-side dry-run**

```bash
for f in deploy/routing/destination-rules/*.yaml; do
  kubectl apply --dry-run=client -f "$f" >/dev/null && echo "OK: $f" || { echo "FAIL: $f"; exit 1; }
done
```

Expected: 5 lines `OK:`. (CRD validation requires the cluster, but YAML schema is checked here.)

- [ ] **Step 3: Commit**

```bash
git add deploy/routing/destination-rules/
git commit -m "feat(routing): add 5 DestinationRules (stable + canary subsets per service)"
```

---

## Task 15: Default-only VirtualService files (5)

**Files:**
- Create: `deploy/routing/virtual-services/audit-service.yaml`
- Create: `deploy/routing/virtual-services/payment-service.yaml`
- Create: `deploy/routing/virtual-services/inventory-service.yaml`
- Create: `deploy/routing/virtual-services/order-service.yaml`
- Create: `deploy/routing/virtual-services/notification-service.yaml`

- [ ] **Step 1: Write all 5 VirtualService files**

For each service, write `deploy/routing/virtual-services/<svc>.yaml`:

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

This is the default-only rule — 100% of traffic to `subset: stable`. Plan 1.4's `canary-ctl` will JSON-merge-patch a header-match rule at index 0 in front of `default`.

- [ ] **Step 2: Validate**

```bash
for f in deploy/routing/virtual-services/*.yaml; do
  kubectl apply --dry-run=client -f "$f" >/dev/null && echo "OK: $f" || { echo "FAIL: $f"; exit 1; }
done
```

Expected: 5 lines `OK:`.

- [ ] **Step 3: Commit**

```bash
git add deploy/routing/virtual-services/
git commit -m "feat(routing): add 5 default-only VirtualServices (canary header rule deferred to 1.4)"
```

---

## Task 16: Edge Gateway and edge VirtualService

**Files:**
- Create: `deploy/routing/ingress/gateway.yaml`
- Create: `deploy/routing/ingress/edge-virtualservice.yaml`

- [ ] **Step 1: Write gateway.yaml**

Create `deploy/routing/ingress/gateway.yaml`:

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
    - port:
        number: 80
        name: http
        protocol: HTTP
      hosts:
        - "*"
```

- [ ] **Step 2: Write edge-virtualservice.yaml**

Create `deploy/routing/ingress/edge-virtualservice.yaml`:

```yaml
apiVersion: networking.istio.io/v1
kind: VirtualService
metadata:
  name: edge
  namespace: services
spec:
  hosts:
    - "*"
  gateways:
    - edge-gateway
  http:
    - match:
        - uri:
            prefix: /api/orders
      route:
        - destination:
            host: order-service.services.svc.cluster.local
            port:
              number: 3001
```

- [ ] **Step 3: Validate**

```bash
kubectl apply --dry-run=client -f deploy/routing/ingress/gateway.yaml
kubectl apply --dry-run=client -f deploy/routing/ingress/edge-virtualservice.yaml
```

Expected: each prints `gateway.networking.istio.io/edge-gateway created (dry run)` / `virtualservice.networking.istio.io/edge created (dry run)`.

- [ ] **Step 4: Commit**

```bash
git add deploy/routing/ingress/
git commit -m "feat(routing): add edge Gateway and VirtualService routing /api/orders to order-service"
```

---

## Task 17: Java Dockerfiles (3)

**Files:**
- Create: `services/audit-service/Dockerfile`
- Create: `services/payment-service/Dockerfile`
- Create: `services/inventory-service/Dockerfile`

- [ ] **Step 1: Write all 3 Dockerfiles**

For each Java service in `audit-service`, `payment-service`, `inventory-service`, write `services/<svc>/Dockerfile` (substitute `<svc>`):

```dockerfile
# syntax=docker/dockerfile:1
# Multi-stage Java build for <svc>.
# Build context is the repo root: docker build -f services/<svc>/Dockerfile -t canary-release-mgmt/<svc>:dev .

FROM eclipse-temurin:25-jdk AS builder
WORKDIR /repo
# Gradle wrapper + root build files
COPY gradlew settings.gradle.kts build.gradle.kts /repo/
COPY gradle /repo/gradle
# Shared platform modules (siblings of the service)
COPY platform/lib-java /repo/platform/lib-java
COPY platform/restate-defs-java /repo/platform/restate-defs-java
# This service
COPY services/<svc> /repo/services/<svc>
# Build the bootJar; --no-daemon avoids a daemon hanging in CI
RUN ./gradlew :services:<svc>:bootJar --no-daemon

FROM eclipse-temurin:25-jre
COPY --from=builder /repo/services/<svc>/build/libs/*.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

Each file is essentially identical except for the `<svc>` substitution; this is intentional — per-service Dockerfiles mirror per-service code modules and keep the build context simple.

- [ ] **Step 2: Build one image to verify the pattern works**

Run from repo root:
```bash
docker build -f services/audit-service/Dockerfile -t canary-release-mgmt/audit-service:dev .
```

Expected: build succeeds in 2-5 minutes (cold). Final line: `Successfully tagged canary-release-mgmt/audit-service:dev`. Image size ~250MB. If gradle plugin downloads stall, the cache rebuilds on next try.

If the build fails with "ENTRYPOINT cannot find /app.jar", inspect with:
```bash
docker run --rm canary-release-mgmt/audit-service:dev ls -la /
```
and verify `/app.jar` exists.

- [ ] **Step 3: Verify the image runs (smoke)**

```bash
docker run --rm -d --name audit-smoke -p 18083:8083 canary-release-mgmt/audit-service:dev
sleep 15
curl -sf http://localhost:18083/actuator/health/liveness
docker rm -f audit-smoke
```

Expected: `{"status":"UP"}`. The container will print errors about Kafka/Restate connection failures (no broker reachable from host); that's fine — actuator liveness is independent.

- [ ] **Step 4: Commit**

```bash
git add services/audit-service/Dockerfile services/payment-service/Dockerfile services/inventory-service/Dockerfile
git commit -m "feat(services): add multi-stage Java Dockerfiles for audit, payment, inventory"
```

---

## Task 18: Node Dockerfiles (2)

**Files:**
- Create: `services/order-service/Dockerfile`
- Create: `services/notification-service/Dockerfile`

- [ ] **Step 1: Write both Dockerfiles**

For each Node service in `order-service`, `notification-service`, write `services/<svc>/Dockerfile` (substitute `<svc>`):

```dockerfile
# syntax=docker/dockerfile:1
# Multi-stage Node build for <svc>.
# Build context is the repo root: docker build -f services/<svc>/Dockerfile -t canary-release-mgmt/<svc>:dev .

FROM node:25-alpine AS builder
WORKDIR /repo
RUN npm install -g pnpm@9.12.0
# Workspace manifests (so pnpm resolves the workspace graph)
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json /repo/
COPY platform/lib-node/package.json /repo/platform/lib-node/
COPY platform/restate-defs-node/package.json /repo/platform/restate-defs-node/
COPY services/<svc>/package.json /repo/services/<svc>/
# Install workspace deps (uses pnpm's content-addressable store; one big layer)
RUN pnpm install --frozen-lockfile
# Source for the workspace deps + this service
COPY platform/lib-node /repo/platform/lib-node
COPY platform/restate-defs-node /repo/platform/restate-defs-node
COPY services/<svc> /repo/services/<svc>
# Build the workspace deps that this service consumes, then the service itself
RUN pnpm --filter @canary/lib-node --filter @canary/restate-defs-node --filter @canary/<svc> build
# Flatten workspace deps into a self-contained app dir
RUN pnpm deploy --filter @canary/<svc> --prod /deploy

FROM node:25-alpine
WORKDIR /app
COPY --from=builder /deploy /app
ENTRYPOINT ["node", "dist/index.js"]
```

- [ ] **Step 2: Build one Node image to verify**

Run from repo root:
```bash
docker build -f services/order-service/Dockerfile -t canary-release-mgmt/order-service:dev .
```

Expected: build succeeds. Final line: `Successfully tagged canary-release-mgmt/order-service:dev`. Image size ~150MB.

- [ ] **Step 3: Verify the image runs (smoke)**

```bash
docker run --rm -d --name order-smoke -p 13001:3001 -e KAFKA_CONSUMERS_ENABLED=false -e RESTATE_REGISTER_HANDLERS=false canary-release-mgmt/order-service:dev
sleep 5
curl -sf http://localhost:13001/health
docker rm -f order-smoke
```

Expected: `{"ok":true}`.

- [ ] **Step 4: Commit**

```bash
git add services/order-service/Dockerfile services/notification-service/Dockerfile
git commit -m "feat(services): add multi-stage Node Dockerfiles for order and notification"
```

---

## Task 19: build-and-load.sh

**Files:**
- Create: `deploy/images/build-and-load.sh`

- [ ] **Step 1: Write the script**

Create `deploy/images/build-and-load.sh`:

```bash
#!/usr/bin/env bash
# Build all 5 service images and (optionally) load them into the kind cluster's image cache.
# Usage:
#   build-and-load.sh build          # docker build only
#   build-and-load.sh load           # kind load only (assumes images exist)
#   build-and-load.sh all            # build + load

set -euo pipefail

SERVICES=(audit-service payment-service inventory-service order-service notification-service)
REPO="canary-release-mgmt"
TAG="dev"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

build_one() {
  local svc="$1"
  echo "==> Building image: ${REPO}/${svc}:${TAG}"
  docker build -f "${REPO_ROOT}/services/${svc}/Dockerfile" -t "${REPO}/${svc}:${TAG}" "${REPO_ROOT}"
}

load_one() {
  local svc="$1"
  : "${KIND_CLUSTER_NAME:?KIND_CLUSTER_NAME must be set (export from Makefile)}"
  echo "==> Loading into kind cluster '${KIND_CLUSTER_NAME}': ${REPO}/${svc}:${TAG}"
  kind load docker-image "${REPO}/${svc}:${TAG}" --name "${KIND_CLUSTER_NAME}"
}

cmd="${1:-all}"

case "$cmd" in
  build)
    for svc in "${SERVICES[@]}"; do build_one "$svc"; done
    ;;
  load)
    for svc in "${SERVICES[@]}"; do load_one "$svc"; done
    ;;
  all)
    for svc in "${SERVICES[@]}"; do build_one "$svc"; done
    for svc in "${SERVICES[@]}"; do load_one "$svc"; done
    ;;
  *)
    echo "Usage: $0 {build|load|all}" >&2
    exit 2
    ;;
esac

echo "==> $cmd complete"
```

- [ ] **Step 2: Make executable**

```bash
chmod +x deploy/images/build-and-load.sh
```

- [ ] **Step 3: Run a build of all 5 to verify**

Run from repo root:
```bash
deploy/images/build-and-load.sh build
```

Expected: 5 successful docker builds, one per service. Total time 5-15 minutes cold (much faster on subsequent runs due to layer caching).

If any service fails, fix the Dockerfile in question (Task 17 or 18) and re-run; the script doesn't abort other services on failure but it does exit with the failing service's status.

- [ ] **Step 4: Verify images exist**

```bash
docker images | grep canary-release-mgmt
```

Expected: 5 lines, one per service, all tagged `dev`.

- [ ] **Step 5: Commit**

```bash
git add deploy/images/build-and-load.sh
git commit -m "feat(images): add build-and-load.sh script for all 5 service images"
```

---

## Task 20: deploy.sh and undeploy.sh

**Files:**
- Create: `deploy/services/deploy.sh`
- Create: `deploy/services/undeploy.sh`

- [ ] **Step 1: Write deploy.sh**

Create `deploy/services/deploy.sh`:

```bash
#!/usr/bin/env bash
# Deploy the 5 domain services + KafkaTopics + Istio routing to the kind cluster.
# Idempotent: re-running upgrades existing releases.

set -euo pipefail

SERVICES=(audit-service payment-service inventory-service notification-service order-service)
NAMESPACE="services"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> 1. Create services namespace with istio-injection label"
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"
kubectl label namespace "$NAMESPACE" istio-injection=enabled --overwrite

echo "==> 2. Apply KafkaTopic CRDs"
kubectl apply -f "${REPO_ROOT}/deploy/kafka/topics/"
echo "    waiting for KafkaTopics to be Ready..."
kubectl wait --for=condition=Ready --timeout=60s -n kafka kafkatopics --all

echo "==> 3. Helm install/upgrade all 5 services"
for svc in "${SERVICES[@]}"; do
  echo "    --- $svc ---"
  helm upgrade --install "$svc" "${REPO_ROOT}/deploy/helm/service-chart" \
    -f "${REPO_ROOT}/deploy/helm/values/${svc}.yaml" \
    -n "$NAMESPACE" \
    --wait --timeout 3m
done

echo "==> 4. Wait for stable Deployments to be Available"
kubectl wait --for=condition=Available --timeout=180s -n "$NAMESPACE" deployment --all

echo "==> 5. Apply routing config (DestinationRules + default-only VirtualServices)"
kubectl apply -f "${REPO_ROOT}/deploy/routing/destination-rules/"
kubectl apply -f "${REPO_ROOT}/deploy/routing/virtual-services/"

echo "==> 6. Apply edge ingress (Gateway + edge VirtualService)"
kubectl apply -f "${REPO_ROOT}/deploy/routing/ingress/"

echo "==> deploy-services complete"
```

- [ ] **Step 2: Write undeploy.sh**

Create `deploy/services/undeploy.sh`:

```bash
#!/usr/bin/env bash
# Inverse of deploy.sh: removes routing, helm releases, and topics.
# Leaves the services namespace in place for fast re-deploy.

set -euo pipefail

SERVICES=(audit-service payment-service inventory-service notification-service order-service)
NAMESPACE="services"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "==> 1. Delete edge ingress"
kubectl delete --ignore-not-found -f "${REPO_ROOT}/deploy/routing/ingress/"

echo "==> 2. Delete routing config"
kubectl delete --ignore-not-found -f "${REPO_ROOT}/deploy/routing/virtual-services/"
kubectl delete --ignore-not-found -f "${REPO_ROOT}/deploy/routing/destination-rules/"

echo "==> 3. Helm uninstall all 5 services"
for svc in "${SERVICES[@]}"; do
  if helm status "$svc" -n "$NAMESPACE" >/dev/null 2>&1; then
    helm uninstall "$svc" -n "$NAMESPACE"
  fi
done

echo "==> 4. Delete KafkaTopics"
kubectl delete --ignore-not-found -f "${REPO_ROOT}/deploy/kafka/topics/"

echo "==> undeploy-services complete (services namespace preserved)"
```

- [ ] **Step 3: Make both executable**

```bash
chmod +x deploy/services/deploy.sh deploy/services/undeploy.sh
```

- [ ] **Step 4: No live invocation in this task**

Validation happens in Task 23's end-to-end smoke. (We could try a dry-run here but the deploy depends on `make up` having been run, which has prerequisites outside this task's scope.)

- [ ] **Step 5: Commit**

```bash
git add deploy/services/deploy.sh deploy/services/undeploy.sh
git commit -m "feat(deploy): add deploy.sh and undeploy.sh orchestration scripts"
```

---

## Task 21: Add Make targets

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add new targets**

In `Makefile`, replace the `.PHONY` line and add the new targets. The full updated `.PHONY` line and the new target block follow.

Replace:
```makefile
.PHONY: help up down status smoke-infra dashboards dashboards-stop dashboards-status verify build-services clean
```

with:
```makefile
.PHONY: help up down status smoke-infra dashboards dashboards-stop dashboards-status verify build-services build-images load-images images deploy-services undeploy-services smoke-services clean
```

After the existing `build-services:` target block (and before `clean:`), insert:

```makefile
build-images: ## Build all 5 service docker images
	@bash deploy/images/build-and-load.sh build

load-images: ## Load all 5 service images into kind
	@bash deploy/images/build-and-load.sh load

images: build-images load-images ## Build then load all 5 images

deploy-services: ## Apply KafkaTopics + Helm install all 5 + Istio routing
	@bash deploy/services/deploy.sh

undeploy-services: ## Remove routing, Helm releases, and KafkaTopics
	@bash deploy/services/undeploy.sh

smoke-services: ## Run service deployment smoke tests
	@bats tests/services/deploy.bats
```

- [ ] **Step 2: Verify make help shows the new targets**

```bash
make help
```

Expected: lines for `build-images`, `load-images`, `images`, `deploy-services`, `undeploy-services`, `smoke-services` appear in alphabetical order.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "feat(make): add build-images, load-images, deploy-services, smoke-services targets"
```

---

## Task 22: Bats smoke test

**Files:**
- Create: `tests/services/deploy.bats`

- [ ] **Step 1: Write the smoke test**

Create `tests/services/deploy.bats`:

```bash
#!/usr/bin/env bats
# Smoke test for Plan 1.3.b deployment.
# Prerequisites (the user runs in order):
#   make up                  # 1.1 cluster + Istio + Strimzi + Restate
#   make build-services      # 1.3.a compile (Java + Node)
#   make build-images        # 1.3.b docker build
#   make load-images         # 1.3.b kind load
#   make deploy-services     # 1.3.b helm install + routing

SERVICES="audit-service payment-service inventory-service notification-service order-service"
TOPICS="audit.events inventory.events notifications.events orders.events payments.events"

@test "all 5 KafkaTopics are Ready" {
  for topic in $TOPICS; do
    run kubectl get -n kafka kafkatopic "$topic" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
    [ "$status" -eq 0 ]
    [ "$output" = "True" ] || { echo "topic $topic not Ready: $output" >&3; false; }
  done
}

@test "all 5 stable Deployments are Available" {
  for svc in $SERVICES; do
    run kubectl get -n services deployment "${svc}-stable" -o jsonpath='{.status.conditions[?(@.type=="Available")].status}'
    [ "$status" -eq 0 ]
    [ "$output" = "True" ] || { echo "deployment ${svc}-stable not Available: $output" >&3; false; }
  done
}

@test "all 5 Services have at least one endpoint" {
  for svc in $SERVICES; do
    run bash -c "kubectl get -n services endpoints '$svc' -o jsonpath='{.subsets[0].addresses[*].ip}' | wc -w"
    [ "$status" -eq 0 ]
    [ "$output" -ge 1 ] || { echo "service $svc has no endpoints" >&3; false; }
  done
}

@test "Restate Admin reports 5 deployments registered" {
  # kind exposes Restate admin on host port 9070 (1.1's NodePort mapping)
  run bash -c "curl -sf http://localhost:9070/deployments | jq '.deployments | length'"
  [ "$status" -eq 0 ]
  [ "$output" = "5" ] || { echo "expected 5 Restate deployments, got: $output" >&3; false; }
}

@test "POST /api/orders via Istio Ingress returns 2xx with an order id" {
  # kind exposes Istio ingress on host port 8080 (1.1's NodePort mapping for ingressgateway)
  run bash -c "curl -sf -X POST -H 'content-type: application/json' \
    -d '{\"userId\":\"u1\",\"sku\":\"sku-1\",\"quantity\":1,\"amount\":100}' \
    http://localhost:8080/api/orders | jq -r '.id'"
  [ "$status" -eq 0 ]
  [ -n "$output" ] && [ "$output" != "null" ] || { echo "no order id in response: $output" >&3; false; }
}
```

The test reads stderr (`>&3`) so failure messages bubble up cleanly even when `bats` is otherwise silent.

- [ ] **Step 2: Make the bats file findable by the runner**

Bats discovers files via `bats <path>`; no shebang execution needed. Ensure the file has a trailing newline.

- [ ] **Step 3: No live invocation in this task**

The bats test depends on a fully deployed cluster, which Task 23 provides.

- [ ] **Step 4: Commit**

```bash
git add tests/services/deploy.bats
git commit -m "test(services): bats smoke test verifying topics, pods, Restate registration, and order POST"
```

---

## Task 23: README updates and end-to-end smoke

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a "1.3.b — Deployment" section to README.md**

In `README.md`, after any existing "1.3.a" section (or at the bottom of the existing content if no other 1.3.x section exists), append:

```markdown
## Plan 1.3.b — Deployment to kind

After Plan 1.3.a (services compile + tests green), Plan 1.3.b deploys all five services to the kind cluster behind Istio routing. Stable-only traffic; canary lifecycle is Plan 1.4.

### Quickstart

```bash
make up                  # bootstrap cluster + Istio + Kafka + Restate (1.1)
make build-services      # compile Java + Node (1.3.a)
make build-images        # docker build all 5 service images
make load-images         # kind load image cache
make deploy-services     # KafkaTopics + Helm install + Istio routing
make smoke-services      # bats smoke test
```

### What this ships

- 5 multi-stage Dockerfiles (`services/<svc>/Dockerfile`)
- 5 Strimzi `KafkaTopic` CRDs (`deploy/kafka/topics/`)
- One shared Helm chart (`deploy/helm/service-chart/`) parameterized by per-service values files (`deploy/helm/values/<svc>.yaml`)
- `canary-overlay.yaml` values file (used by Plan 1.4 canary-ctl; checked in but not applied)
- 5 `DestinationRule` + 5 default-only `VirtualService` files (`deploy/routing/`)
- Istio `Gateway` + edge `VirtualService` (`/api/orders` → order-service)
- Per-service Helm post-install Job that registers handlers with Restate Admin

### Service inventory

| Service | Stack | HTTP port | Restate port | Probes |
|---|---|---|---|---|
| audit-service | Java + Spring Boot | 8083 | 9083 | /actuator/health/{liveness,readiness} |
| payment-service | Java + Spring Boot | 8081 | 9081 | /actuator/health/{liveness,readiness} |
| inventory-service | Java + Spring Boot | 8082 | 9082 | /actuator/health/{liveness,readiness} |
| order-service | TypeScript + Node | 3001 | 9084 | /health |
| notification-service | TypeScript + Node | 3002 | 9085 | /health |

### Verifying

```bash
make smoke-services      # 5 bats assertions, ~60s
kubectl get -n services pods,svc,deploy
kubectl get -n services destinationrules,virtualservices
helm list -n services    # should show 5 releases, all deployed
curl -s http://localhost:9070/deployments | jq '.deployments | length'  # 5
curl -s -X POST -H 'content-type: application/json' \
  -d '{"userId":"u1","sku":"sku-1","quantity":1,"amount":100}' \
  http://localhost:8080/api/orders
```

### Tearing down (without destroying the cluster)

```bash
make undeploy-services
```
```

- [ ] **Step 2: Run the full end-to-end deploy + smoke**

This step requires a clean kind cluster.

```bash
# From repo root
make down 2>/dev/null || true   # ensure clean state
make up
make build-services
make build-images
make load-images
make deploy-services
make smoke-services
```

Expected:
- `make up`: cluster + addons come up (~3-5 min cold)
- `make build-services`: gradle + pnpm compile (~30 s)
- `make build-images`: docker build all 5 (~5-15 min cold)
- `make load-images`: kind load (~30 s)
- `make deploy-services`: ~3-5 min (KafkaTopics 30 s, Helm releases 30 s each, deployments 60-120 s, routing instant)
- `make smoke-services`: 5/5 tests pass

If any step fails, stop and report which step + the first 30 lines of error output. Common failures:
- `make up` fails: pre-existing kind cluster of the same name; `kind delete cluster --name canary-release-mgmt` and retry.
- `make build-images` fails on first build: gradle or pnpm registry timeout; retry.
- `make deploy-services` hangs at `kubectl wait deployment`: image not loaded, or `imagePullPolicy: IfNotPresent` not honored. Inspect with `kubectl describe -n services pod -l app=<svc>`.
- `make smoke-services` fails on Restate registration count: registration Job may have hit the 6-retry backoff limit. Inspect with `kubectl logs -n services job/register-restate-<svc>` and `kubectl get jobs -n services`.
- `make smoke-services` fails on `POST /api/orders`: edge VirtualService may not be applied or order-service may be returning 5xx. Inspect with `curl -v http://localhost:8080/api/orders ...` and `kubectl logs -n services deploy/order-service-stable`.

- [ ] **Step 3: Commit the README update**

```bash
git add README.md
git commit -m "docs(readme): add Plan 1.3.b deployment section + run instructions"
```

- [ ] **Step 4: Final verification**

Run `git log --oneline phase-1.3.b-deployment ^main` (or just `git log --oneline -25`) and confirm there are roughly 17-23 commits since main, all on the `phase-1.3.b-deployment` branch, all green.

Then announce: "Plan 1.3.b implementation complete. Ready to merge."

---

## Self-review checklist (controller runs after all tasks)

This section is for the agent driving execution. After Task 23 completes:

1. **Spec coverage:** Walk each numbered section of `docs/superpowers/specs/2026-05-08-canary-release-phase-1-3-b-deployment-design.md` and confirm a task delivers it.
   - §3 Helm chart contract → Tasks 7–11
   - §4 Canary overlay → Task 13
   - §5 Per-service values → Task 12
   - §6 Routing (DR + VS + ingress) → Tasks 14–16
   - §7 KafkaTopics → Task 6
   - §8 Image build → Tasks 17–19
   - §9 Probes → Tasks 1–4
   - §10 Deploy ordering → Task 20
   - §11 Smoke verification → Task 22
   - §12 Make targets → Task 21
   - §13 README update → Task 23
2. **All checkboxes ticked across all 23 tasks.**
3. **`make smoke-services`** passes 5/5 in Task 23 step 2.
4. **No uncommitted changes** in the worktree.

If all four hold, hand off to `superpowers:finishing-a-development-branch` for merge.
