#!/bin/sh
# Refresh the IDTOKEN in .env and restart Grafana.
#
# Needed hourly against daemons running cedar older than v0.7.2, which reject a
# token more than an hour past issuance (see README.md, "The one-hour trap").
#
#   ./refresh-token.sh ap40.chtc.wisc.edu
set -e
host="${1:?usage: $0 <ap-hostname>}"
cd "$(dirname "$0")"

[ -f .env ] || { echo "no .env here; copy env.example first" >&2; exit 1; }

tok=$(ssh "$host" condor_token_fetch -lifetime 86400 | tr -d '\n\r')
# A failed ssh still exits 0 through the pipe on some shells, and an error
# message is not a JWT -- check the shape before overwriting a working token.
case "$tok" in
	eyJ*.*.*) ;;
	*) echo "did not get a token back from $host: $tok" >&2; exit 1 ;;
esac

addr=$(grep '^HTCONDORDB_ADDR=' .env)
printf '%s\nHTCONDORDB_TOKEN=%s\n' "$addr" "$tok" > .env
docker compose up -d --force-recreate >/dev/null
printf 'waiting for grafana'
i=0
while [ $i -lt 40 ]; do
	curl -sf -o /dev/null http://localhost:3000/api/health && break
	printf '.'; sleep 1; i=$((i+1))
done
echo
curl -s http://localhost:3000/api/datasources/uid/htcondordb-local/health
echo
