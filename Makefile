.PHONY: help up down status smoke-infra clean

# Versions (pinned for reproducibility)
KIND_CLUSTER_NAME := canary-release-mgmt
ISTIO_VERSION    := 1.29.2
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
