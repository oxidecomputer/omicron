#!/usr/bin/env bash
# TODO-RAINCLAUDE: populates a fresh single-node CockroachDB store with schema/crdb/dbinit.sql, then stops the node cleanly so the image ships a consistent store.

set -euo pipefail

store_dir="${1:?usage: cockroach-seed.sh STORE_DIR}"
listen_addr="127.0.0.1:26257"
http_addr="127.0.0.1:8080"
pid_file="/tmp/cockroach-seed.pid"

cockroach start-single-node \
    --insecure \
    --store="path=${store_dir},ballast-size=0" \
    --listen-addr="${listen_addr}" \
    --http-addr="${http_addr}" \
    --pid-file="${pid_file}" \
    --background

cockroach sql \
    --insecure \
    --host="${listen_addr}" \
    --file=/opt/oxide/schema/crdb/dbinit.sql

cockroach sql \
    --insecure \
    --host="${listen_addr}" \
    --file=/opt/oxide/schema/cockroach-seed-antithesis.sql

version="$(cockroach sql --insecure --host="${listen_addr}" --format=csv \
    --execute='SELECT version FROM omicron.public.db_metadata' | tail -n 1)"
echo "seeded omicron database at schema version ${version}"

cockroach node drain --insecure --host="${listen_addr}" --self
kill -TERM "$(cat "${pid_file}")"
while kill -0 "$(cat "${pid_file}")" 2>/dev/null; do
    sleep 1
done
rm -f "${pid_file}"
