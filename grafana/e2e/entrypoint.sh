#!/usr/bin/env bash
# Start htcondordb with a permissive (anonymous read + write) config and load the
# sample dataset, then serve in the foreground.
set -euo pipefail

export CONDOR_CONFIG=/etc/htcondordb/condor_config
mkdir -p /etc/htcondordb /var/lib/htcondordb
cat > "$CONDOR_CONFIG" <<CFG
ALLOW_READ = *
ALLOW_WRITE = *
ALLOW_DAEMON = *
# NEVER so every peer is anonymous: cross-container clients (the plugin, the
# loader) cannot FS-authenticate, and a client that offers auth methods the server
# won't run would otherwise error instead of falling back to anonymous. Anonymous
# still writes here because ALLOW_WRITE = * grants it -- fine for a throwaway E2E DB.
SEC_DEFAULT_AUTHENTICATION = NEVER
HTCONDORDB_DIR = /var/lib/htcondordb
# Expose the Prometheus /metrics endpoint (materialized-view gauges + storage
# stats) so a Prometheus server can scrape it.
HTCONDORDB_METRICS_ADDRESS = :9631
CFG

htcondordb -listen 0.0.0.0:9630 &
srv=$!

# Wait for the command socket to accept connections.
for _ in $(seq 1 60); do
  (echo > /dev/tcp/127.0.0.1/9630) 2>/dev/null && break
  sleep 0.5
done
sleep 1

# Load sample data: one statement per non-comment, non-blank line.
while IFS= read -r stmt; do
  case "$stmt" in ''|'#'*|'--'*) continue ;; esac
  htcondordb-cli -addr 127.0.0.1:9630 -e "$stmt" >/dev/null || echo "load failed: $stmt" >&2
done < /etc/htcondordb/sample-data.sql
echo "htcondordb: sample data loaded; serving on :9630"

wait "$srv"
