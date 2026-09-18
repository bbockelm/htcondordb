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

# Architecture for the containerized manylinux build: amd64 or arm64.
WHEEL_ARCH ?= amd64
GO    ?= go

# Shared-library suffix for the ./capi client (dlopen'd by the Python driver in python/).
# macOS wants .dylib; every other platform we build on wants .so.
LIB_EXT ?= $(if $(filter Darwin,$(shell uname -s)),dylib,so)
LIB     := $(BIN_DIR)/libhtcondordb_client.$(LIB_EXT)

.PHONY: all build daemon cli lib lib-check-linux archive python-test wheel wheel-macos wheel-linux wheel-validate wheel-clean test vet tidy clean version rpm rpm-build exporters

all: build

build: daemon cli ## Build both binaries into $(BIN_DIR)

daemon: ## Build the htcondordb daemon
	$(GOENV) $(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/htcondordb ./cmd/htcondordb

cli: ## Build the htcondordb-cli shell/loader
	$(GOENV) $(GO) build -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/htcondordb-cli ./cmd/htcondordb-cli

lib: ## Build the C client as a shared library (for the Python driver / dlopen callers)
	$(GOENV) $(GO) build -buildmode=c-shared -ldflags '$(LDFLAGS)' -o $(LIB) ./capi

lib-check-linux: ## Compile the C client for Linux in a container (catches macOS-only cgo)
	# cgo headers differ: glibc does not pull stdint.h in via stdlib.h the way macOS does, so a
	# missing include in a capi preamble builds clean on a Mac and fails only in CI. This is the
	# same check without the round trip. Needs docker; the module cache is shared read-only.
	docker run --rm -v "$(CURDIR)":/src:ro -v "$$(go env GOMODCACHE)":/gomod:ro -w /src \
		-e GOWORK=off -e GOFLAGS=-mod=mod -e GOPROXY=off -e GOMODCACHE=/gomod \
		golang:1.27 go build -buildmode=c-shared -o /tmp/libhtcondordb_client.so ./capi

archive: ## Build the C client as a static archive (for C/C++ callers to link)
	$(GOENV) $(GO) build -buildmode=c-archive -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/libhtcondordb_client.a ./capi

python-test: lib ## Run the Python driver's test suite against the freshly built library
	HTCONDORDB_LIBRARY=$(abspath $(LIB)) $(PYTHON) -m pytest python/tests -v

wheel: lib ## Build a platform wheel for THIS host (bundles the shared library)
	rm -rf python/htcondordb/_lib python/build python/*.egg-info
	mkdir -p python/htcondordb/_lib
	cp $(LIB) python/htcondordb/_lib/
	cd python && $(PYTHON) -m build --wheel --no-isolation
	@echo
	@ls -la python/dist/*.whl

wheel-macos: ## Build the universal2 macOS wheel (both architectures in one binary)
	rm -rf python/dist python/build python/htcondordb/_lib
	mkdir -p python/htcondordb/_lib
	$(GOENV) $(GO) build -buildvcs=false -buildmode=c-shared -o /tmp/hcdb-arm64.dylib ./capi
	$(GOENV) CGO_ENABLED=1 GOARCH=amd64 CC="clang -arch x86_64" \
		$(GO) build -buildvcs=false -buildmode=c-shared -o /tmp/hcdb-amd64.dylib ./capi
	lipo -create /tmp/hcdb-arm64.dylib /tmp/hcdb-amd64.dylib \
		-output python/htcondordb/_lib/libhtcondordb_client.dylib
	cd python && _PYTHON_HOST_PLATFORM=macosx-11.0-universal2 $(PYTHON) -m build --wheel --no-isolation
	@ls -la python/dist/*.whl

wheel-linux: ## Build a manylinux_2_28 wheel in a container (the shipping artifact)
	./python/build-manylinux.sh $(WHEEL_ARCH)

wheel-validate: ## Install the built Linux wheel in a clean container and run real SQL through it
	$(GOENV) $(GO) build -o python/dist/htcondordb-linux ./cmd/htcondordb 2>/dev/null || \
		docker run --rm --platform linux/$(WHEEL_ARCH) -v "$(CURDIR)":/src \
			-v "$$(go env GOMODCACHE)":/gomodcache:ro -w /src \
			-e GOMODCACHE=/gomodcache -e GOCACHE=/tmp/gocache -e GOFLAGS=-mod=mod \
			-e GOWORK=off -e GOPROXY=off quay.io/pypa/manylinux_2_28_x86_64 bash -eu -c \
			'curl -sSL https://go.dev/dl/go1.25.7.linux-$(WHEEL_ARCH).tar.gz | tar -C /usr/local -xz; \
			 PATH=/usr/local/go/bin:$$PATH go build -o /src/python/dist/htcondordb-linux ./cmd/htcondordb; \
			 chmod 0755 /src/python/dist/htcondordb-linux'
	docker run --rm --platform linux/$(WHEEL_ARCH) -v "$(CURDIR)":/src \
		python:3.12-slim bash /src/python/validate-wheel-in-container.sh

wheel-clean: ## Remove wheel build artifacts, including the staged library
	rm -rf python/dist python/build python/*.egg-info python/htcondordb/_lib

version: ## Print the version that would be stamped
	@echo $(VERSION)

test: ## Run the test suite
	$(GOENV) $(GO) test ./...

vet: ## Static checks
	$(GOENV) $(GO) vet ./...

tidy: ## Reconcile go.mod / go.sum (both modules)
	$(GOENV) $(GO) mod tidy
	# grafana/ is a separate module that requires this one (via a
	# ../ replace) plus the same classad/cedar/golang-htcondor
	# versions. Bumping a dependency here leaves it behind, and its
	# CI job runs with -mod=readonly, so the drift surfaces there as
	# "updates to go.mod needed, disabled by -mod=readonly" rather
	# than anywhere near the change that caused it. Tidying both
	# together is what keeps them in step.
	cd grafana && $(GOENV) $(GO) mod tidy

clean: ## Remove built binaries
	rm -rf $(BIN_DIR)

# --- RPM packaging -------------------------------------------------------
#
# The package carries the exporter launchers as well as the daemon: they are
# resolved by basename on PATH, else next to os.Executable(), so a package
# with the daemon alone installs cleanly and then silently skips every
# configured exporter.

NFPM     ?= nfpm
PKG_ARCH ?= $(shell go env GOARCH)
PKG_DIR  ?= dist
# Where the payload is assembled for nfpm. Deliberately NOT derived from
# PKG_DIR and deliberately not overridable: nfpm globs contents.src literally
# (it expands environment variables in scalar fields but not there), so this
# path is spelled out in packaging/nfpm.yaml and the two must agree. Tying it
# to PKG_DIR means overriding the output directory stages the binaries
# somewhere nfpm never looks.
PKG_STAGE := packaging/staging
PKG_MAINTAINER ?= Brian Bockelman <bbockelman@morgridge.org>
PKG_VENDOR     ?= HTCondor / PATh
EXPORTERS := kafkasync opensearchsync archivedropbox

rpm-build: ## Cross-build every packaged binary into $(PKG_STAGE) for PKG_ARCH
	@rm -rf $(PKG_STAGE) && mkdir -p $(PKG_STAGE)
	@echo "Building htcondordb $(VERSION) for linux/$(PKG_ARCH)..."
	@GOOS=linux GOARCH=$(PKG_ARCH) CGO_ENABLED=0 $(GOENV) $(GO) build -trimpath \
		-ldflags '-s -w $(LDFLAGS)' -o $(PKG_STAGE)/ ./cmd/htcondordb ./cmd/htcondordb-cli
	@for exp in $(EXPORTERS); do \
		echo "Building $$exp..."; \
		(cd $$exp && GOOS=linux GOARCH=$(PKG_ARCH) CGO_ENABLED=0 GOWORK=off $(GO) build -trimpath \
			-ldflags '-s -w' -o $(CURDIR)/$(PKG_STAGE)/ ./cmd/$$exp) || exit 1; \
	done
	@ls -1 $(PKG_STAGE)

rpm: rpm-build ## Build an RPM for linux/$(PKG_ARCH) (PKG_ARCH=amd64|arm64)
	@command -v $(NFPM) >/dev/null 2>&1 || { \
		echo "nfpm not found. Build the pinned version:"; \
		echo "  (cd .github/tools && GOWORK=off go build -o \"$$HOME/go/bin/nfpm\" github.com/goreleaser/nfpm/v2/cmd/nfpm)"; \
		echo "or point NFPM at one you already have: make rpm NFPM=/path/to/nfpm"; \
		exit 1; }
	# nfpm writes to --target as a FILE unless the path is an existing
	# directory, so this has to exist before it runs.
	@mkdir -p $(PKG_DIR)
	@set -e; \
	set -- $$(packaging/rpm-version.sh "$(VERSION)"); \
	PKG_VERSION=$$1; PKG_RELEASE=$$2; \
	echo "Packaging htcondordb $$PKG_VERSION-$$PKG_RELEASE ($(PKG_ARCH))..."; \
	PKG_VERSION=$$PKG_VERSION PKG_RELEASE=$$PKG_RELEASE PKG_ARCH=$(PKG_ARCH) \
	PKG_MAINTAINER="$(PKG_MAINTAINER)" PKG_VENDOR="$(PKG_VENDOR)" \
		$(NFPM) package --config packaging/nfpm.yaml --packager rpm --target $(PKG_DIR)
	@ls -1 $(PKG_DIR)/*.rpm
