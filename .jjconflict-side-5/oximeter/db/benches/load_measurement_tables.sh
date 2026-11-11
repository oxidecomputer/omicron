#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

#
# Load a measurement table backup into a ClickHouse for benchmarking.
# Crashes if the destination table already contains data.
#
# Usage: ./load_measurement_tables.sh <input_dir> <table> [port]

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <input_dir> <table> [port]" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEMA_DIR="$SCRIPT_DIR/../schema/single-node"

INPUT_DIR="$1"
TABLE="$2"
PORT="${3:-9000}"

DATABASE="oximeter"

# Initialize schema. db-init.sql is fully IF NOT EXISTS, so this is a no-op
# if the schema is already in place.
echo "Initializing database schema..."
clickhouse client --port "$PORT" --multiquery < "$SCHEMA_DIR/db-init.sql"

# Error if destination table already has data.
count=$(clickhouse client --port "$PORT" \
    --query "SELECT count() FROM $DATABASE.$TABLE")
if [[ "$count" -gt 0 ]]; then
    echo "Error: $DATABASE.$TABLE already contains data ($count rows)"
    echo "Refusing to load into a non-empty table."
    exit 1
fi

input="$INPUT_DIR/${TABLE}.native.gz"
if [[ ! -f "$input" ]]; then
    echo "No backup for table $TABLE in $INPUT_DIR"
    exit 1
fi

# Note: Use INSERT rather than RESTORE because we may not have access to the
# local ClickHouse's disk, or have backups enabled at all.
echo "Loading $DATABASE.$TABLE from $input"
gunzip -c "$input" | clickhouse client --port "$PORT" \
    --query "INSERT INTO $DATABASE.$TABLE FORMAT Native"
