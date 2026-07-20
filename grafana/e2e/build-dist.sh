#!/usr/bin/env bash
# Build the plugin into grafana/dist so the compose Grafana can load it:
# the frontend module.js plus the backend binary for the container's arch(es).
set -euo pipefail
cd "$(dirname "$0")/.."   # grafana/

npm run build   # frontend -> dist/module.js (+ plugin.json, img)

# Linux backend binaries (Grafana container is linux; build amd64 + arm64 so the
# same dist works on either host arch).
for arch in amd64 arm64; do
  GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH="$arch" \
    go build -trimpath -o "dist/gpx_htcondordb_linux_${arch}" ./pkg
done
echo "built dist/: $(ls dist | tr '\n' ' ')"
