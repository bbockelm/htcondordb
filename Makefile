# htcondordb build.
#
# The module depends on in-development sibling checkouts via go.mod `replace`
# directives, so builds run with the module-graph flags those need. `go build`
# is itself incremental (build cache), so the phony targets just invoke it.

BIN_DIR ?= bin

# Sibling modules are private and resolved directly (replaces point at local
# checkouts); GOWORK=off keeps a stray workspace file from overriding them.
GOENV := GOWORK=off GOFLAGS=-mod=mod \
         GOPRIVATE=github.com/bbockelm,github.com/PelicanPlatform \
         GOPROXY=direct
GO    ?= go

.PHONY: all build daemon cli test vet tidy clean

all: build

build: daemon cli ## Build both binaries into $(BIN_DIR)

daemon: ## Build the htcondordb daemon
	$(GOENV) $(GO) build -o $(BIN_DIR)/htcondordb ./cmd/htcondordb

cli: ## Build the htcondordb-cli shell/loader
	$(GOENV) $(GO) build -o $(BIN_DIR)/htcondordb-cli ./cmd/htcondordb-cli

test: ## Run the test suite
	$(GOENV) $(GO) test ./...

vet: ## Static checks
	$(GOENV) $(GO) vet ./...

tidy: ## Reconcile go.mod / go.sum
	$(GOENV) $(GO) mod tidy

clean: ## Remove built binaries
	rm -rf $(BIN_DIR)
