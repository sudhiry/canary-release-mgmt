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
