# Phase 1.1 — Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a working local Kubernetes substrate on a developer laptop with kind + Istio + Strimzi Kafka + Restate, fronted by a `make up` / `make down` workflow and verified by an infrastructure smoke test.

**Architecture:** A single kind cluster bootstrapped by a Makefile that calls a small set of focused shell scripts and applies versioned manifests. Istio is installed in default profile. Kafka runs in KRaft mode via Strimzi. Restate runs as a single-replica StatefulSet. Each piece has its own smoke check; an aggregate `make smoke-infra` proves the substrate is healthy end-to-end.

**Tech Stack:**
- kind 0.23.x (local Kubernetes)
- Istio 1.22.x (service mesh, default profile)
- Strimzi 0.43.x (Kafka operator, KRaft mode)
- Restate 1.1.x (durable workflow engine)
- bash + GNU make (orchestration)
- bats-core (shell test framework for smoke tests)

**Spec reference:** `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`

---

## Prerequisites

The plan assumes the developer machine has these tools installed. If not, install with Homebrew on macOS:

```bash
brew install kind kubectl istioctl helm jq bats-core
# Docker Desktop must be running (kind needs the Docker daemon)
```

The kind cluster will use Docker Desktop's daemon. Allocate at least 6 GB RAM and 4 CPUs to Docker Desktop.

---

## File Structure

```
canary-release-mgmt/
├── Makefile                                    # top-level orchestration
├── README.md                                   # project intro + quickstart
├── deploy/
│   └── kind/
│       ├── cluster-config.yaml                 # kind cluster declaration
│       ├── bootstrap.sh                        # called by `make up`; orchestrates substeps
│       ├── teardown.sh                         # called by `make down`
│       ├── status.sh                           # prints pod state across namespaces
│       ├── istio/
│       │   ├── iop-config.yaml                 # IstioOperator config (default profile + NodePort)
│       │   └── install.sh                      # `istioctl install -f` invocation
│       ├── kafka/
│       │   ├── strimzi-operator-install.sh     # downloads + applies Strimzi operator
│       │   └── kafka-cluster.yaml              # Kafka CR (KRaft, 1 broker)
│       ├── restate/
│       │   ├── namespace.yaml
│       │   ├── statefulset.yaml
│       │   ├── service.yaml
│       │   └── install.sh
│       └── observability/
│           └── install.sh                      # applies Istio addons (Prometheus, Grafana, Kiali, Jaeger)
└── tests/
    └── infra/
        ├── smoke.bats                          # bats-core test file
        └── helpers.bash                        # shared assertions (wait_for_pod_ready, etc.)
```

**Why one Makefile, multiple shell scripts:** keeps Makefile shell-portable (no embedded multiline shell) and makes each piece independently runnable for debugging.

**Why bats-core:** the smoke checks are shell-native (kubectl + jq). bats gives us assertion structure, parallel-friendly test functions, and CI-clean output without dragging in Python or TypeScript at this layer.

---

## Task 1: Repo top-level scaffolding

**Files:**
- Create: `README.md`
- Create: `Makefile`

- [ ] **Step 1: Write `README.md`**

```markdown
# canary-release-mgmt

A reference architecture for canary release management across HTTP, Kafka,
and Restate.dev in a polyglot microservice system.

This repository is built in phases. See `docs/superpowers/specs/` for design
specs and `docs/superpowers/plans/` for implementation plans.

## Phase 1 — Substrate + HTTP canary

Quickstart:

    make up         # bootstrap kind + Istio + Kafka + Restate
    make smoke-infra # verify all infra is Ready
    make down       # tear down

Full design: `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`
```

- [ ] **Step 2: Write top-level `Makefile`**

```makefile
.PHONY: help up down status smoke-infra clean

# Versions (pinned for reproducibility)
KIND_CLUSTER_NAME := canary-release-mgmt
ISTIO_VERSION    := 1.22.3
STRIMZI_VERSION  := 0.43.0
RESTATE_VERSION  := 1.1.5

export KIND_CLUSTER_NAME ISTIO_VERSION STRIMZI_VERSION RESTATE_VERSION

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

up: ## Bootstrap kind cluster + Istio + Kafka + Restate
	@bash deploy/kind/bootstrap.sh

down: ## Delete the kind cluster
	@bash deploy/kind/teardown.sh

status: ## Show pod state across infra namespaces
	@bash deploy/kind/status.sh

smoke-infra: ## Run infrastructure smoke tests
	@bats tests/infra/smoke.bats

clean: down ## Alias for down
```

- [ ] **Step 3: Verify `make help` works**

Run: `make help`
Expected: prints the help table with `up`, `down`, `status`, `smoke-infra`, `clean` targets.

- [ ] **Step 4: Commit**

```bash
git add README.md Makefile
git commit -m "feat(scaffold): add top-level README and Makefile shell"
```

---

## Task 2: kind cluster config + bootstrap shell skeleton

**Files:**
- Create: `deploy/kind/cluster-config.yaml`
- Create: `deploy/kind/bootstrap.sh`
- Create: `deploy/kind/teardown.sh`
- Create: `deploy/kind/status.sh`
- Create: `tests/infra/helpers.bash`
- Create: `tests/infra/smoke.bats`

- [ ] **Step 1: Write `tests/infra/helpers.bash` with shared assertions**

```bash
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
```

- [ ] **Step 2: Write `tests/infra/smoke.bats` with the cluster-running test only (more added in later tasks)**

```bash
#!/usr/bin/env bats
# tests/infra/smoke.bats

load helpers
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-canary-release-mgmt}"

@test "kind cluster is running" {
  assert_kind_cluster_running
}
```

- [ ] **Step 3: Run smoke test — confirm it fails (no cluster yet)**

Run: `bats tests/infra/smoke.bats`
Expected: FAIL — `kubectl cluster-info` returns non-zero because no cluster exists.

- [ ] **Step 4: Write `deploy/kind/cluster-config.yaml`**

```yaml
# deploy/kind/cluster-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: canary-release-mgmt
nodes:
  - role: control-plane
    extraPortMappings:
      # Istio ingress gateway HTTP
      - containerPort: 30080
        hostPort: 8080
        protocol: TCP
      # Restate ingress (for invoking handlers from outside the cluster, dev only)
      - containerPort: 30090
        hostPort: 9070
        protocol: TCP
```

- [ ] **Step 5: Write `deploy/kind/bootstrap.sh` (cluster-only for now; later tasks append to it)**

```bash
#!/usr/bin/env bash
# deploy/kind/bootstrap.sh
# Bootstraps the local cluster. Idempotent: re-running on an existing cluster is a no-op for cluster creation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${KIND_CLUSTER_NAME:?KIND_CLUSTER_NAME must be set}"

echo "==> Creating kind cluster: ${KIND_CLUSTER_NAME}"
if kind get clusters | grep -qx "${KIND_CLUSTER_NAME}"; then
  echo "    cluster already exists; skipping create"
else
  kind create cluster --config "${SCRIPT_DIR}/cluster-config.yaml" --wait 120s
fi

kubectl config use-context "kind-${KIND_CLUSTER_NAME}"
kubectl cluster-info

echo "==> Cluster ready"
```

- [ ] **Step 6: Write `deploy/kind/teardown.sh`**

```bash
#!/usr/bin/env bash
# deploy/kind/teardown.sh

set -euo pipefail
: "${KIND_CLUSTER_NAME:?KIND_CLUSTER_NAME must be set}"

echo "==> Deleting kind cluster: ${KIND_CLUSTER_NAME}"
if kind get clusters | grep -qx "${KIND_CLUSTER_NAME}"; then
  kind delete cluster --name "${KIND_CLUSTER_NAME}"
else
  echo "    cluster does not exist; nothing to do"
fi
```

- [ ] **Step 7: Write `deploy/kind/status.sh`**

```bash
#!/usr/bin/env bash
# deploy/kind/status.sh

set -euo pipefail

for ns in istio-system kafka restate default; do
  echo "==> Namespace: ${ns}"
  kubectl get pods -n "${ns}" 2>/dev/null || echo "    namespace not present"
  echo
done
```

- [ ] **Step 8: Make scripts executable**

Run: `chmod +x deploy/kind/bootstrap.sh deploy/kind/teardown.sh deploy/kind/status.sh`

- [ ] **Step 9: Run `make up` (cluster only at this point)**

Run: `make up`
Expected: kind cluster created; final line "Cluster ready".

- [ ] **Step 10: Run smoke test — confirm it passes**

Run: `KIND_CLUSTER_NAME=canary-release-mgmt bats tests/infra/smoke.bats`
Expected: PASS — `kind cluster is running`.

- [ ] **Step 11: Commit**

```bash
git add deploy/kind tests/infra
git commit -m "feat(infra): kind cluster bootstrap, teardown, status, and smoke skeleton"
```

---

## Task 3: Install Istio

**Files:**
- Create: `deploy/kind/istio/iop-config.yaml`
- Create: `deploy/kind/istio/install.sh`
- Modify: `deploy/kind/bootstrap.sh` (append Istio install step)
- Modify: `tests/infra/smoke.bats` (add Istio assertion)

- [ ] **Step 1: Add failing Istio assertion to `tests/infra/smoke.bats`**

Append:

```bash
@test "istiod is Ready" {
  wait_for_pod_ready istio-system "app=istiod" 60
}

@test "istio-ingressgateway is Ready" {
  wait_for_pod_ready istio-system "app=istio-ingressgateway" 60
}
```

- [ ] **Step 2: Run smoke test — confirm Istio assertions fail**

Run: `bats tests/infra/smoke.bats`
Expected: cluster test passes; both Istio tests FAIL — namespace `istio-system` does not exist.

- [ ] **Step 3: Write `deploy/kind/istio/iop-config.yaml`**

```yaml
# deploy/kind/istio/iop-config.yaml
# IstioOperator config: default profile + NodePort ingress gateway with stable nodePorts.
apiVersion: install.istio.io/v1alpha1
kind: IstioOperator
metadata:
  name: canary-release-mgmt
spec:
  profile: default
  components:
    ingressGateways:
      - name: istio-ingressgateway
        enabled: true
        k8s:
          service:
            type: NodePort
            ports:
              - name: status-port
                port: 15021
                targetPort: 15021
                nodePort: 30021
              - name: http2
                port: 80
                targetPort: 8080
                nodePort: 30080
              - name: https
                port: 443
                targetPort: 8443
                nodePort: 30443
```

- [ ] **Step 4: Write `deploy/kind/istio/install.sh`**

```bash
#!/usr/bin/env bash
# deploy/kind/istio/install.sh
# Installs Istio with the IstioOperator config in iop-config.yaml.

set -euo pipefail
: "${ISTIO_VERSION:?ISTIO_VERSION must be set}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Installing Istio ${ISTIO_VERSION}"

# Idempotent: only install if istiod doesn't already exist.
if kubectl -n istio-system get deployment istiod >/dev/null 2>&1; then
  echo "    istiod already present; skipping install"
else
  istioctl install -f "${SCRIPT_DIR}/iop-config.yaml" -y
fi

# Label the default namespace for sidecar injection (services will land here in Plan 1.3).
kubectl label namespace default istio-injection=enabled --overwrite

echo "==> Istio installed"
```

- [ ] **Step 5: Make script executable and append to `bootstrap.sh`**

Run: `chmod +x deploy/kind/istio/install.sh`

Append to `deploy/kind/bootstrap.sh` after the cluster creation block:

```bash
echo "==> Installing Istio"
bash "${SCRIPT_DIR}/istio/install.sh"
```

- [ ] **Step 6: Run `make up` again to install Istio**

Run: `make up`
Expected: Istio install output; istiod and istio-ingressgateway pods come Ready.

- [ ] **Step 7: Run smoke test — confirm Istio assertions pass**

Run: `bats tests/infra/smoke.bats`
Expected: all 3 tests PASS.

- [ ] **Step 8: Commit**

```bash
git add deploy/kind/istio deploy/kind/bootstrap.sh tests/infra/smoke.bats
git commit -m "feat(infra): install Istio default profile with NodePort ingress"
```

---

## Task 4: Install Strimzi operator and Kafka cluster

**Files:**
- Create: `deploy/kind/kafka/strimzi-operator-install.sh`
- Create: `deploy/kind/kafka/kafka-cluster.yaml`
- Modify: `deploy/kind/bootstrap.sh` (append Kafka install step)
- Modify: `tests/infra/smoke.bats` (add Kafka assertions)

- [ ] **Step 1: Add failing Kafka assertions to `tests/infra/smoke.bats`**

Append:

```bash
@test "strimzi cluster operator is Ready" {
  wait_for_pod_ready kafka "name=strimzi-cluster-operator" 120
}

@test "kafka cluster reports Ready" {
  run kubectl -n kafka get kafka my-cluster \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
  [ "$status" -eq 0 ]
  [ "$output" = "True" ]
}
```

- [ ] **Step 2: Run smoke test — confirm Kafka assertions fail**

Run: `bats tests/infra/smoke.bats`
Expected: cluster + Istio tests pass; Kafka tests FAIL.

- [ ] **Step 3: Write `deploy/kind/kafka/strimzi-operator-install.sh`**

```bash
#!/usr/bin/env bash
# deploy/kind/kafka/strimzi-operator-install.sh
# Installs the Strimzi cluster operator into the kafka namespace.

set -euo pipefail
: "${STRIMZI_VERSION:?STRIMZI_VERSION must be set}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Installing Strimzi ${STRIMZI_VERSION}"

kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -

# Idempotent: only install if cluster operator absent.
if kubectl -n kafka get deployment strimzi-cluster-operator >/dev/null 2>&1; then
  echo "    strimzi cluster operator already present; skipping"
else
  curl -sL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
    | sed 's/namespace: .*/namespace: kafka/' \
    | kubectl -n kafka apply -f -
fi

# Wait for operator to be Ready before applying the Kafka CR.
kubectl -n kafka rollout status deployment/strimzi-cluster-operator --timeout=180s

echo "==> Applying Kafka cluster CR"
kubectl -n kafka apply -f "${SCRIPT_DIR}/kafka-cluster.yaml"

# Wait for the Kafka cluster to be Ready.
kubectl -n kafka wait --for=condition=Ready --timeout=300s kafka/my-cluster

echo "==> Kafka cluster Ready"
```

- [ ] **Step 4: Write `deploy/kind/kafka/kafka-cluster.yaml`**

```yaml
# deploy/kind/kafka/kafka-cluster.yaml
# Single-broker KRaft Kafka for local development.

apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: kafka-pool
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  replicas: 1
  roles:
    - controller
    - broker
  storage:
    type: ephemeral
  resources:
    requests:
      memory: 512Mi
      cpu: 250m
    limits:
      memory: 1Gi
      cpu: 1000m
---
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
  namespace: kafka
  annotations:
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: 3.7.1
    metadataVersion: 3.7-IV4
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

- [ ] **Step 5: Make script executable and append to `bootstrap.sh`**

Run: `chmod +x deploy/kind/kafka/strimzi-operator-install.sh`

Append to `deploy/kind/bootstrap.sh`:

```bash
echo "==> Installing Strimzi + Kafka"
bash "${SCRIPT_DIR}/kafka/strimzi-operator-install.sh"
```

- [ ] **Step 6: Run `make up` to install Kafka**

Run: `make up`
Expected: operator deployed, Kafka CR applied, cluster reaches Ready (may take 1–3 minutes).

- [ ] **Step 7: Run smoke test — confirm Kafka assertions pass**

Run: `bats tests/infra/smoke.bats`
Expected: all 5 tests PASS.

- [ ] **Step 8: Commit**

```bash
git add deploy/kind/kafka deploy/kind/bootstrap.sh tests/infra/smoke.bats
git commit -m "feat(infra): install Strimzi operator and 1-broker KRaft Kafka cluster"
```

---

## Task 5: Install Restate

**Files:**
- Create: `deploy/kind/restate/namespace.yaml`
- Create: `deploy/kind/restate/statefulset.yaml`
- Create: `deploy/kind/restate/service.yaml`
- Create: `deploy/kind/restate/install.sh`
- Modify: `deploy/kind/bootstrap.sh` (append Restate install step)
- Modify: `tests/infra/smoke.bats` (add Restate assertion)

- [ ] **Step 1: Add failing Restate assertion to `tests/infra/smoke.bats`**

Append:

```bash
@test "restate server is Ready" {
  wait_for_pod_ready restate "app=restate" 120
}

@test "restate admin endpoint responds" {
  run kubectl -n restate exec restate-0 -- \
    curl -sf -o /dev/null -w '%{http_code}' http://localhost:9070/health
  [ "$status" -eq 0 ]
  [ "$output" = "200" ]
}
```

- [ ] **Step 2: Run smoke test — confirm Restate assertions fail**

Run: `bats tests/infra/smoke.bats`
Expected: existing tests pass; both Restate tests FAIL.

- [ ] **Step 3: Write `deploy/kind/restate/namespace.yaml`**

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: restate
  labels:
    istio-injection: disabled
```

- [ ] **Step 4: Write `deploy/kind/restate/statefulset.yaml`**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: restate
  namespace: restate
spec:
  serviceName: restate-headless
  replicas: 1
  selector:
    matchLabels:
      app: restate
  template:
    metadata:
      labels:
        app: restate
    spec:
      containers:
        - name: restate
          image: docker.io/restatedev/restate:1.1.5
          imagePullPolicy: IfNotPresent
          args:
            - --node-name
            - restate-0
          env:
            - name: RESTATE_TRACING_ENDPOINT
              value: ""
            - name: RUST_LOG
              value: info
          ports:
            - name: ingress
              containerPort: 8080
            - name: admin
              containerPort: 9070
            - name: node
              containerPort: 5122
          readinessProbe:
            httpGet:
              path: /health
              port: 9070
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 6
          livenessProbe:
            httpGet:
              path: /health
              port: 9070
            initialDelaySeconds: 30
            periodSeconds: 10
          resources:
            requests:
              memory: 256Mi
              cpu: 200m
            limits:
              memory: 1Gi
              cpu: 1000m
          volumeMounts:
            - name: data
              mountPath: /restate-data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
```

- [ ] **Step 5: Write `deploy/kind/restate/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: restate-headless
  namespace: restate
spec:
  clusterIP: None
  selector:
    app: restate
  ports:
    - name: node
      port: 5122
      targetPort: 5122
---
apiVersion: v1
kind: Service
metadata:
  name: restate
  namespace: restate
spec:
  type: ClusterIP
  selector:
    app: restate
  ports:
    - name: ingress
      port: 8080
      targetPort: 8080
    - name: admin
      port: 9070
      targetPort: 9070
---
# NodePort exposing the admin endpoint to the host (mapped to host port 9070 by kind config).
apiVersion: v1
kind: Service
metadata:
  name: restate-admin-nodeport
  namespace: restate
spec:
  type: NodePort
  selector:
    app: restate
  ports:
    - name: admin
      port: 9070
      targetPort: 9070
      nodePort: 30090
```

- [ ] **Step 6: Write `deploy/kind/restate/install.sh`**

```bash
#!/usr/bin/env bash
# deploy/kind/restate/install.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Installing Restate"

kubectl apply -f "${SCRIPT_DIR}/namespace.yaml"
kubectl apply -f "${SCRIPT_DIR}/service.yaml"
kubectl apply -f "${SCRIPT_DIR}/statefulset.yaml"

# Wait for the StatefulSet to be Ready (rollout status reports completion).
kubectl -n restate rollout status statefulset/restate --timeout=180s

echo "==> Restate Ready"
```

- [ ] **Step 7: Make script executable and append to `bootstrap.sh`**

Run: `chmod +x deploy/kind/restate/install.sh`

Append to `deploy/kind/bootstrap.sh`:

```bash
echo "==> Installing Restate"
bash "${SCRIPT_DIR}/restate/install.sh"

echo "==> Bootstrap complete"
```

- [ ] **Step 8: Run `make up` to install Restate**

Run: `make up`
Expected: Restate StatefulSet rolled out; pod Ready.

- [ ] **Step 9: Run smoke test — confirm Restate assertions pass**

Run: `bats tests/infra/smoke.bats`
Expected: all 7 tests PASS.

- [ ] **Step 10: Commit**

```bash
git add deploy/kind/restate deploy/kind/bootstrap.sh tests/infra/smoke.bats
git commit -m "feat(infra): install Restate as single-replica StatefulSet"
```

---

## Task 6: Install Istio observability addons (Prometheus, Grafana, Kiali, Jaeger)

The Phase 1 design calls for Prometheus, Grafana, Kiali, and Jaeger (or Tempo) as the substrate observability stack. Phase 1.5 e2e scenarios (S3, S4, S8, S9) verify behavior via Jaeger traces and Kiali graphs, so the addons must be live by the end of Plan 1.1. Polish — dashboards, SLOs, alerting — is deferred to Phase 5.

**Files:**
- Create: `deploy/kind/observability/install.sh`
- Modify: `deploy/kind/bootstrap.sh` (append observability install step)
- Modify: `tests/infra/smoke.bats` (add observability assertions)

- [ ] **Step 1: Add failing observability assertions to `tests/infra/smoke.bats`**

Append:

```bash
@test "prometheus is Ready" {
  wait_for_pod_ready istio-system "app=prometheus" 120
}

@test "grafana is Ready" {
  wait_for_pod_ready istio-system "app=grafana" 120
}

@test "kiali is Ready" {
  wait_for_pod_ready istio-system "app=kiali" 120
}

@test "jaeger is Ready" {
  wait_for_pod_ready istio-system "app=jaeger" 120
}
```

- [ ] **Step 2: Run smoke test — confirm observability assertions fail**

Run: `bats tests/infra/smoke.bats`
Expected: prior tests pass; 4 observability tests FAIL.

- [ ] **Step 3: Write `deploy/kind/observability/install.sh`**

This script applies the official Istio addon manifests from the matching release. They are single-file dev-grade deployments — suitable for local kind, not production. Production observability is Phase 5.

```bash
#!/usr/bin/env bash
# deploy/kind/observability/install.sh
# Installs the Istio observability addons (Prometheus, Grafana, Kiali, Jaeger)
# from the matching Istio release. These are dev-grade single-replica deployments.

set -euo pipefail
: "${ISTIO_VERSION:?ISTIO_VERSION must be set}"

BASE_URL="https://raw.githubusercontent.com/istio/istio/${ISTIO_VERSION}/samples/addons"

echo "==> Installing Istio observability addons (${ISTIO_VERSION})"

for addon in prometheus grafana kiali jaeger; do
  echo "    applying ${addon}"
  kubectl apply -f "${BASE_URL}/${addon}.yaml"
done

# Wait for each addon Deployment to be Ready before returning.
for addon in prometheus grafana kiali jaeger; do
  kubectl -n istio-system rollout status "deployment/${addon}" --timeout=180s
done

echo "==> Observability addons Ready"
```

- [ ] **Step 4: Make script executable and append to `bootstrap.sh`**

Run: `chmod +x deploy/kind/observability/install.sh`

Append to `deploy/kind/bootstrap.sh` (BEFORE the final `echo "==> Bootstrap complete"`):

```bash
echo "==> Installing observability addons"
bash "${SCRIPT_DIR}/observability/install.sh"
```

- [ ] **Step 5: Run `make up` to install observability addons**

Run: `make up`
Expected: each addon manifest applied; Deployments roll out cleanly.

- [ ] **Step 6: Run smoke test — confirm observability assertions pass**

Run: `bats tests/infra/smoke.bats`
Expected: all 11 tests PASS (cluster + 2 Istio + 2 Kafka + 2 Restate + 4 observability).

- [ ] **Step 7: Commit**

```bash
git add deploy/kind/observability deploy/kind/bootstrap.sh tests/infra/smoke.bats
git commit -m "feat(infra): install Istio observability addons (Prometheus, Grafana, Kiali, Jaeger)"
```

---

## Task 7: End-to-end fresh-bootstrap verification

**Files:** (no new files; this task verifies idempotence and a clean cycle)

- [ ] **Step 1: Tear down everything**

Run: `make down`
Expected: kind cluster deleted.

- [ ] **Step 2: Bootstrap from a clean state**

Run: `make up`
Expected: cluster created → Istio installed → Kafka cluster Ready → Restate Ready → observability addons Ready. Total runtime should be 4–8 minutes on a typical developer laptop.

- [ ] **Step 3: Run the full smoke test suite**

Run: `make smoke-infra`
Expected: all 11 bats tests PASS.

- [ ] **Step 4: Verify idempotent re-run of `make up`**

Run: `make up`
Expected: every step reports "already present; skipping" or equivalent; no errors; smoke still passes.

Run: `make smoke-infra`
Expected: all 11 PASS.

- [ ] **Step 5: Run `make status` and visually inspect**

Run: `make status`
Expected: pods listed for `istio-system` (istiod, ingressgateway, prometheus, grafana, kiali, jaeger), `kafka`, `restate`. The `default` namespace shows no pods (it will host services in Plan 1.3).

- [ ] **Step 6: No commit**

This task is verification only; no files changed.

---

## Task 8: Document operator workflow in README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Append a "Plan 1.1 status" section to `README.md`**

Append:

```markdown

## Plan 1.1 — Foundation (complete)

Local infrastructure is in place. The following commands are operational:

| Command            | What it does                                                       |
| ------------------ | ------------------------------------------------------------------ |
| `make up`          | Bootstrap kind + Istio + Strimzi/Kafka + Restate + observability   |
| `make down`        | Delete the kind cluster                                            |
| `make status`      | Show pod state across `istio-system`, `kafka`, `restate`           |
| `make smoke-infra` | Run bats infrastructure smoke tests (11 assertions)                |
| `make help`        | List available targets                                             |

The Kafka cluster is reachable inside the mesh at
`my-cluster-kafka-bootstrap.kafka:9092`. Restate's admin API is reachable
inside the mesh at `restate.restate:9070` and from the host on
`http://localhost:9070`. Istio's ingress gateway is reachable from the host
on `http://localhost:8080`.

Observability dashboards (port-forward to access):
- Kiali:      `kubectl -n istio-system port-forward svc/kiali 20001:20001` → http://localhost:20001
- Grafana:    `kubectl -n istio-system port-forward svc/grafana 3000:3000` → http://localhost:3000
- Prometheus: `kubectl -n istio-system port-forward svc/prometheus 9090:9090` → http://localhost:9090
- Jaeger:     `kubectl -n istio-system port-forward svc/tracing 16686:80` → http://localhost:16686

Next phase: 1.2 (shared platform libraries for `x-canary` propagation).
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: record Plan 1.1 operator workflow in README"
```

---

## Self-review checklist (run after task definitions)

After every task is mapped, the plan author runs this checklist:

- **Spec coverage.** Plan 1.1 implements the spec's "Cluster", "Image flow" (local-only), "Observability" (substrate install only — polish is Phase 5), and infrastructure substrate sections. Service mesh, Kafka, Restate, and Istio observability addons (Prometheus, Grafana, Kiali, Jaeger) are all present in stable-only mode. The `lib-java`/`lib-node` libraries, services, Helm chart, `canary-ctl`, and e2e scenarios are explicitly deferred to Plans 1.2–1.5.
- **Placeholders.** None. All file contents are concrete; all commands are exact.
- **Type/name consistency.** `KIND_CLUSTER_NAME=canary-release-mgmt` is used everywhere. Restate StatefulSet name is `restate`, pod name is `restate-0`, services are `restate`, `restate-headless`, `restate-admin-nodeport`. Kafka cluster CR is `my-cluster` in namespace `kafka`. Observability addon Deployments are `prometheus`, `grafana`, `kiali`, `jaeger` in `istio-system`. Smoke test selectors match the workload labels.
- **TDD discipline.** Every smoke assertion is added to bats *before* the implementation step, run to confirm it fails, then implementation is run, then re-run to confirm pass. Infra-flavored TDD adapted from unit-test TDD.
- **Frequent commits.** Seven commits across Tasks 1–6 + 8. Each commit produces a working state of the substrate.

---

## Done when

- `make down && make up && make smoke-infra` runs cleanly on a fresh Docker Desktop in 4–8 minutes.
- All 11 bats smoke tests pass.
- All commits in this task list are present in the working branch.
- README documents the operator workflow for the foundation layer.
