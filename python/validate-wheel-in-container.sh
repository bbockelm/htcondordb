#!/usr/bin/env bash
# Validates a built Linux wheel end to end, INSIDE a container, on a different distro from
# the one it was built on -- which is the property that matters: the wheel is built in
# manylinux (AlmaLinux 8) and has to work on whatever the user actually runs.
#
# Installs the wheel into a clean venv from nothing but the wheel, starts a real daemon,
# and runs real SQL through it. No HTCONDORDB_LIBRARY, no make lib.
#
# Not run directly -- see `make wheel-validate`, which supplies the container.
set -eu
echo "== distro =="; cat /etc/os-release | head -2
python -m venv /tmp/v && . /tmp/v/bin/activate
pip install -q /src/python/dist/*.whl
python - <<'PY'
import htcondordb
print("== import ok, version", htcondordb.__version__)
print("== bundled lib:", htcondordb.library_path())
PY
# Real daemon, real query.
useradd -u 1000 -m hcdb 2>/dev/null || true
R=/tmp/db; mkdir -p $R/log $R/spool $R/db
cat > $R/condor_config <<EOF
LOG = $R/log
SPOOL = $R/spool
HTCONDORDB_DIR = $R/db
HTCONDORDB_ADDRESS_FILE = $R/address
UID_DOMAIN = localhost
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_DEFAULT_ENCRYPTION = OPTIONAL
SEC_DEFAULT_INTEGRITY = OPTIONAL
ALLOW_READ = *
ALLOW_WRITE = *
ALLOW_DAEMON = *
CONDOR_IDS = 1000.1000
EOF
chown -R 1000:1000 $R
CONDOR_CONFIG=$R/condor_config setpriv --reuid=1000 --regid=1000 --clear-groups /src/python/dist/htcondordb-linux -listen 127.0.0.1:0 > $R/daemon.log 2>&1 &
for i in $(seq 1 40); do [ -s $R/address ] && break; sleep 0.25; done
[ -s $R/address ] || { echo "== daemon failed to start:"; cat $R/daemon.log; ls -la $R; exit 1; }
ADDR=$(cat $R/address); echo "== daemon at $ADDR"
CONDOR_CONFIG=$R/condor_config python - "$ADDR" <<'PY'
import sys, htcondordb
conn = htcondordb.connect(sys.argv[1]); cur = conn.cursor()
cur.execute("CREATE TABLE machines")
cur.execute("INSERT INTO machines (Key, Name, Cpus, Start, WithinResourceLimits, Requirements) "
            "VALUES (?, ?, ?, true, true, Start && WithinResourceLimits)", ("s1", "slot1@ep", 8))
cur.execute("SELECT Name, Cpus, Requirements FROM machines")
print("== query:", cur.fetchall())
cur.execute("SELECT COUNT(*) AS n FROM machines")
print("== aggregate:", cur.fetchone())
conn.close()
print("== END TO END OK on a clean venv from the wheel alone")
PY
