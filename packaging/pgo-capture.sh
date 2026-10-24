#!/usr/bin/env bash
# pgo-capture.sh -- capture CPU profiles from running htcondordb daemons and merge
# them into the profile-guided optimization (PGO) profile the daemon is built with.
#
#   packaging/pgo-capture.sh [-s SECONDS] [-n COUNT] [-o OUT] [-k DIR] [-a FILE]... URL...
#
#   URL        a daemon's metrics/pprof base address, e.g. http://ap1.example.org:9630
#              (HTCONDORDB_METRICS_ADDRESS with HTCONDORDB_ENABLE_PPROF = true)
#   -s SECONDS length of each CPU profile (default 60)
#   -n COUNT   profiles to take from each URL, back to back (default 1)
#   -o OUT     merged output (default cmd/htcondordb/default.pgo)
#   -k DIR     keep the raw profiles in DIR (default: a temporary dir, removed on exit)
#   -a FILE    also merge FILE, a profile saved earlier (repeatable); with -a and no
#              URL, only merges
#
# `go build` picks up cmd/htcondordb/default.pgo automatically (the default -pgo=auto
# applies a main package's default.pgo to that package), so every build path --
# `make`, release.yml, the RPM, the Dockerfiles -- uses whatever this writes. The
# other main packages (htcondordb-cli, history-import, changefeed-toysink) carry no
# profile and are built without PGO.
#
# The checked-in profile is synthetic: it was recorded from a generated workload in
# a container, not from production. Replace it with one captured from production
# daemons. A profile should cover the work the daemon actually does, so capture it
# from a busy mirror (linux/amd64, the main deployment target) across several
# windows and merge them, for example:
#
#   # steady-state tailing plus normal query load, three minutes
#   packaging/pgo-capture.sh -n 3 -k prof/steady http://ap1:9630
#   # during a busy dashboard period
#   packaging/pgo-capture.sh -k prof/busy http://ap1:9630
#   # while a reconcile runs (start `.resync jobs` in htcondordb-cli first)
#   packaging/pgo-capture.sh -k prof/resync http://ap1:9630
#   # merge all of them into the checked-in profile
#   packaging/pgo-capture.sh $(printf -- '-a %s ' prof/*/*.pprof)
#
# Each profile weighs in proportion to the CPU time it recorded, so the merged
# profile follows where the daemon spends its CPU. Refresh it when a classad minor
# version is adopted, or quarterly; a stale profile still builds (the compiler
# ignores samples that no longer match the source) but optimizes less. Keep the
# output under about 1 MB: `go tool pprof -proto` already drops everything but
# the samples and their call stacks.
#
# Needs curl and a Go toolchain (for `go tool pprof`).
set -euo pipefail

seconds=60
count=1
out=cmd/htcondordb/default.pgo
keep=""
extra=()

usage() { sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

while getopts "s:n:o:k:a:h" opt; do
	case "$opt" in
	s) seconds=$OPTARG ;;
	n) count=$OPTARG ;;
	o) out=$OPTARG ;;
	k) keep=$OPTARG ;;
	a) extra+=("$OPTARG") ;;
	*) usage ;;
	esac
done
shift $((OPTIND - 1))
[ $# -gt 0 ] || [ ${#extra[@]} -gt 0 ] || usage

if [ -n "$keep" ]; then
	dir=$keep
	mkdir -p "$dir"
else
	dir=$(mktemp -d)
	trap 'rm -rf "$dir"' EXIT
fi

profiles=()
for url in "$@"; do
	url=${url%/}
	host=${url#*://}
	host=${host//[^A-Za-z0-9.-]/_}
	for i in $(seq 1 "$count"); do
		f="$dir/${host}_$(date -u +%Y%m%dT%H%M%SZ)_$i.pprof"
		echo "capturing ${seconds}s CPU profile from $url ($i/$count)" >&2
		curl -fsS --max-time $((seconds + 60)) -o "$f" \
			"$url/debug/pprof/profile?seconds=$seconds"
		profiles+=("$f")
	done
done
profiles+=(${extra[@]+"${extra[@]}"})

tmp="$out.tmp.$$"
go tool pprof -proto "${profiles[@]}" >"$tmp"
mv "$tmp" "$out"
echo "wrote $out ($(wc -c <"$out" | tr -d ' ') bytes) from ${#profiles[@]} profile(s)" >&2
