# htcondordb Grafana datasource plugin: build, E2E, and demo (video) targets.
# Requires Go, Node/npm, and Docker (with the compose plugin).

COMPOSE    := docker compose -f e2e/docker-compose.e2e.yaml
DEMO_VIDEO := e2e/htcondordb-grafana-demo.webm

.PHONY: help build test e2e-up e2e-down e2e demo clean

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

node_modules: package-lock.json ## Install frontend deps (npm ci)
	npm ci

playwright-browser: node_modules ## Install the Playwright chromium browser
	npx playwright install chromium

build: node_modules ## Build the plugin into dist/ (frontend + linux backend binaries)
	./e2e/build-dist.sh

test: ## Backend unit tests (standalone module)
	GOWORK=off go test ./pkg/...

e2e-up: build ## Start the E2E stack (htcondordb + Grafana + the plugin)
	$(COMPOSE) up -d --build --wait

e2e-down: ## Stop and remove the E2E stack
	$(COMPOSE) down -v

e2e: e2e-up playwright-browser ## Run the Playwright E2E suite against the stack
	npx playwright test

demo: e2e-up playwright-browser ## Record the install-and-use walkthrough video
	npx playwright test -c playwright.demo.config.ts
	@cp "$$(find e2e/demo-output -path '*install-and-use*' -name video.webm | head -1)" "$(DEMO_VIDEO)"
	@echo "Demo video: $(CURDIR)/$(DEMO_VIDEO)"

clean: e2e-down ## Tear the stack down and remove build/test artifacts
	rm -rf dist e2e/demo-output test-results playwright-report playwright "$(DEMO_VIDEO)"
