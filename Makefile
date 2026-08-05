# htcondordb build.
#
# The module depends on in-development sibling checkouts via go.mod `replace`
# directives, so builds run with the module-graph flags those need. `go build`
# is itself incremental (build cache), so the phony targets just invoke it.

BIN_DIR ?= bin

# Version stamped into both binaries' -version flag (main.version); a plain
# `go build` without this leaves it "dev".
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

# Sibling modules are private and resolved directly (replaces point at local
# checkouts); GOWORK=off keeps a stray workspace file from overriding them.
GOENV := GOWORK=off GOFLAGS=-mod=mod \
         GOPRIVATE=github.com/bbockelm,github.com/PelicanPlatform \
         GOPROXY=direct
PYTHON ?= python3
GO    ?= go

# Shared-library suffix for the ./capi client (dlopen'd by the Python driver in python/).
# macOS wants .dylib; every other platform we build on wants .so.
LIB_EXT ?= $(if $(filter Darwin,$(shell uname -s)),dylib,so)
LIB     := $(BIN_DIR)/libhtcondordb_client.$(LIB_EXT)

.PHONY: all build daemon cli lib archive python-test test vet tidy clean version

all: build

build: daemon cli ## Build both binaries into $(BIN_DIR)

daemon: ## Build the htcondordb daemon
	$(GOENV) $(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/htcondordb ./cmd/htcondordb

cli: ## Build the htcondordb-cli shell/loader
	$(GOENV) $(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/htcondordb-cli ./cmd/htcondordb-cli

lib: ## Build the C client as a shared library (for the Python driver / dlopen callers)
	$(GOENV) $(GO) build -buildmode=c-shared -ldflags '$(LDFLAGS)' -o $(LIB) ./capi

archive: ## Build the C client as a static archive (for C/C++ callers to link)
	$(GOENV) $(GO) build -buildmode=c-archive -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/libhtcondordb_client.a ./capi

python-test: lib ## Run the Python driver's test suite against the freshly built library
	HTCONDORDB_LIBRARY=$(abspath $(LIB)) $(PYTHON) -m pytest python/tests -v

version: ## Print the version that would be stamped
	@echo $(VERSION)

test: ## Run the test suite
	$(GOENV) $(GO) test ./...

vet: ## Static checks
	$(GOENV) $(GO) vet ./...

tidy: ## Reconcile go.mod / go.sum
	$(GOENV) $(GO) mod tidy

clean: ## Remove built binaries
	rm -rf $(BIN_DIR)
