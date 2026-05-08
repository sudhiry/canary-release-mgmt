.PHONY: help up down status smoke-infra dashboards dashboards-stop dashboards-status clean

# Versions (pinned for reproducibility)
KIND_CLUSTER_NAME := canary-release-mgmt
ISTIO_VERSION    := 1.29.2
STRIMZI_VERSION  := 0.45.2
RESTATE_VERSION  := 1.6.2

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

dashboards: ## Open port-forwards to Kiali/Grafana/Prometheus/Jaeger (background)
	@bash deploy/kind/dashboards.sh start

dashboards-stop: ## Stop all dashboard port-forwards
	@bash deploy/kind/dashboards.sh stop

dashboards-status: ## Show which dashboard port-forwards are running
	@bash deploy/kind/dashboards.sh status

clean: down ## Alias for down
