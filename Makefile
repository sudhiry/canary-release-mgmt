.PHONY: help up down status smoke-infra dashboards dashboards-stop dashboards-status verify build-services build-images load-images images deploy-services undeploy-services smoke-services clean e2e ci-local

# Versions (pinned for reproducibility)
KIND_CLUSTER_NAME := canary-release-mgmt
ISTIO_VERSION    := 1.29.2
STRIMZI_VERSION  := 0.45.2
RESTATE_VERSION  := 1.6.2

export KIND_CLUSTER_NAME ISTIO_VERSION STRIMZI_VERSION RESTATE_VERSION

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
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

verify: ## Run all unit/library/service tests (Java + Node)
	@echo "==> Java"
	@./gradlew test --quiet
	@echo "==> Node"
	@pnpm -r --filter "@canary/*" run test

build-services: ## Compile all 5 service binaries (Java bootJars + Node tsc dist)
	@echo "==> Java services"
	@./gradlew :services:audit-service:bootJar :services:payment-service:bootJar :services:inventory-service:bootJar --quiet
	@echo "==> Node services (with workspace deps in topo order)"
	@pnpm --filter '@canary/order-service...' --filter '@canary/notification-service...' build

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

clean: down ## Alias for down

.PHONY: canary-deploy canary-rollback canary-status canary-reconcile smoke-canary

canary-deploy: ## Deploy a canary: SVC=<service> TAG=<image-tag>
	@if [ -z "$(SVC)" ] || [ -z "$(TAG)" ]; then \
	  echo "usage: make canary-deploy SVC=<service> TAG=<tag>" >&2; exit 2; \
	fi
	@node tools/canary-ctl/bin/canary-ctl deploy-canary $(SVC) $(TAG)

canary-rollback: ## Rollback a canary: SVC=<service>
	@if [ -z "$(SVC)" ]; then echo "usage: make canary-rollback SVC=<service>" >&2; exit 2; fi
	@node tools/canary-ctl/bin/canary-ctl rollback $(SVC)

canary-status: ## Show canary status: SVC=<service>
	@if [ -z "$(SVC)" ]; then echo "usage: make canary-status SVC=<service>" >&2; exit 2; fi
	@node tools/canary-ctl/bin/canary-ctl status $(SVC)

canary-reconcile: ## Reconcile canary state for SVC=<service>
	@if [ -z "$(SVC)" ]; then echo "usage: make canary-reconcile SVC=<service>" >&2; exit 2; fi
	@node tools/canary-ctl/bin/canary-ctl reconcile $(SVC)

smoke-canary: ## Run canary-ctl bats smoke tests (requires deployed substrate)
	@pnpm --filter @canary/canary-ctl build >/dev/null
	@pnpm --filter @canary/traffic-cli build >/dev/null
	@bats tests/canary/canary-ctl.bats

e2e: ## Run e2e scenarios (use SCENARIO=<name> to run a single file)
	@pnpm --filter @canary/e2e build >/dev/null
	@if [ -n "$(SCENARIO)" ]; then \
		E2E_SCENARIOS=1 pnpm --filter @canary/e2e exec vitest run $(SCENARIO); \
	else \
		E2E_SCENARIOS=1 pnpm --filter @canary/e2e test; \
	fi

ci-local: ## Run fast e2e subset (S1 in 1.5.a; S1+S2+S5+S8+S9+S12 in 1.5.b)
	@pnpm --filter @canary/e2e build >/dev/null
	@E2E_SCENARIOS=1 pnpm --filter @canary/e2e exec vitest run s1
