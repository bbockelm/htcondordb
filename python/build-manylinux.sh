#!/usr/bin/env bash
# Build a manylinux wheel for the Python driver, in a container.
#
# The wheel bundles libhtcondordb_client, which cgo links against the build host's glibc --
# so building on a modern distro produces a wheel that will not install on the EL8/EL9
# machines this is actually for. Building inside manylinux_2_28 sets that floor at glibc
# 2.28 (EL8), which covers everything current.
#
# Go module sources come from the host's module cache, mounted read-only, so the container
# needs no credentials for the private sibling modules and no network for dependencies.
#
# Usage: build-manylinux.sh [amd64|arm64]
set -euo pipefail

ARCH="${1:-amd64}"
case "$ARCH" in
amd64) IMAGE_ARCH="x86_64"; GOARCH="amd64" ;;
arm64) IMAGE_ARCH="aarch64"; GOARCH="arm64" ;;
*) echo "unknown arch: $ARCH (want amd64 or arm64)" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GOMODCACHE="$(cd "$REPO_ROOT" && go env GOMODCACHE)"
GO_VERSION="${GO_VERSION:-1.25.7}"
IMAGE="quay.io/pypa/manylinux_2_28_${IMAGE_ARCH}"

if [ ! -d "$GOMODCACHE" ]; then
	echo "Go module cache not found at $GOMODCACHE; run a build on the host first" >&2
	exit 1
fi

echo "==> building ${ARCH} wheel in ${IMAGE}"

# The module cache is mounted read-only, so give Go a writable copy of the bits it insists
# on writing (the build cache) inside the container.
docker run --rm \
	--platform "linux/${ARCH}" \
	-v "$REPO_ROOT":/src \
	-v "$GOMODCACHE":/gomodcache:ro \
	-w /src \
	-e GOARCH="$GOARCH" \
	-e GOMODCACHE=/gomodcache \
	-e GOCACHE=/tmp/gocache \
	-e GOFLAGS=-mod=mod \
	-e GOWORK=off \
	-e GOPROXY=off \
	-e GO_VERSION="$GO_VERSION" \
	"$IMAGE" \
	bash -eu -o pipefail -c '
		curl -sSL "https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz" | tar -C /usr/local -xz
		export PATH=/usr/local/go/bin:$PATH
		go version

		# The shared library, built inside the container so it links this glibc.
		mkdir -p /tmp/out
		go build -buildmode=c-shared -o /tmp/out/libhtcondordb_client.so ./capi

		# Stage it into the package and build the wheel. Work in a copy so the mounted
		# source tree is left untouched by root-owned build artifacts.
		cp -r /src/python /tmp/pkg
		rm -rf /tmp/pkg/dist /tmp/pkg/build /tmp/pkg/htcondordb/_lib
		mkdir -p /tmp/pkg/htcondordb/_lib
		cp /tmp/out/libhtcondordb_client.so /tmp/pkg/htcondordb/_lib/

		PY=/opt/python/cp312-cp312/bin/python
		"$PY" -m pip install -q --upgrade build setuptools wheel auditwheel
		cd /tmp/pkg && "$PY" -m build --wheel --no-isolation

		# auditwheel checks the external library dependencies and retags to the real
		# glibc floor. The Go runtime pulls in only libc/libpthread/libdl, so there is
		# normally nothing to bundle -- the value here is the tag being honest.
		"$PY" -m auditwheel repair --plat "manylinux_2_28_'"${IMAGE_ARCH}"'" -w /tmp/repaired /tmp/pkg/dist/*.whl \
			|| { echo "auditwheel repair failed; the unrepaired wheel is below"; cp /tmp/pkg/dist/*.whl /tmp/repaired/ 2>/dev/null || true; }

		mkdir -p /src/python/dist
		cp /tmp/repaired/*.whl /src/python/dist/
		chmod 0644 /src/python/dist/*.whl
	'

echo
ls -la "$REPO_ROOT"/python/dist/*.whl
