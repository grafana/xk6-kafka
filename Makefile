# Developer entry point. Run `make` (or `make help`) to list targets.
#
# Lint targets come from the grafana/k6-ci template: they read the pinned
# k6-ci commit from the CI workflow (single source) and produce the effective
# .golangci.yml locally so local lint matches CI. See grafana/k6-ci/README.md.
#
# Lint config overrides: WORKFLOW, LINT_BASE, LINT_FINAL, LINT_PATCH.
# Produces two gitignored files at the repo root:
#   .golangci-base.yml  cached download from grafana/k6-ci (re-fetched only
#                       when WORKFLOW changes)
#   .golangci.yml       effective config = base + LINT_PATCH (if present)

MODULE := github.com/grafana/xk6-kafka

WORKFLOW   ?= .github/workflows/k6-ci.yml
K6_CI_REF  := $(shell grep -oE 'grafana/k6-ci/[^@[:space:]]+@[A-Za-z0-9._/-]+' $(WORKFLOW) | head -n1 | cut -d@ -f2)
BASE_URL   := https://raw.githubusercontent.com/grafana/k6-ci/$(K6_CI_REF)/.golangci.yml

LINT_BASE  ?= .golangci-base.yml
LINT_FINAL ?= .golangci.yml
LINT_PATCH ?= .golangci.patch

.DEFAULT_GOAL := help

.PHONY: help
help: ## Print this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build a k6 binary with this extension (xk6 build)
	CGO_ENABLED=0 xk6 build --with $(MODULE)=.

.PHONY: test
test: ## Run the unit tests (go test)
	go test ./...

.PHONY: it
it: ## Run the integration tests (xk6 test)
	xk6 test

$(LINT_BASE): $(WORKFLOW)
	curl -fsSL $(BASE_URL) -o $@

$(LINT_FINAL): $(LINT_BASE) $(wildcard $(LINT_PATCH))
	cp $(LINT_BASE) $@
	@if [ -f $(LINT_PATCH) ]; then \
	  echo "Applying $(LINT_PATCH)"; \
	  git apply $(LINT_PATCH); \
	fi

.PHONY: lint
lint: $(LINT_FINAL) ## Run golangci-lint with the pinned k6-ci config
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$$(head -n1 $(LINT_BASE) | tr -d '# ') \
	  run --config=$(LINT_FINAL) ./...

.PHONY: update-lint-patch
update-lint-patch: $(LINT_BASE) ## Record local .golangci.yml edits as a patch over the k6-ci base
	@if [ ! -f $(LINT_FINAL) ]; then \
	  echo "Run 'make lint' first to materialize $(LINT_FINAL), edit it, then re-run."; \
	  exit 1; \
	fi
	-diff -u --label a/.golangci.yml --label b/.golangci.yml $(LINT_BASE) $(LINT_FINAL) > $(LINT_PATCH)

.PHONY: clean-lint
clean-lint: ## Remove the generated lint config files
	rm -f $(LINT_BASE) $(LINT_FINAL)
