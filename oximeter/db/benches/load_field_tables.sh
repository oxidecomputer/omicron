#!/bin/bash
#
# Load field table backups into a fresh ClickHouse for benchmarking.
# Crashes if the destination database already contains data.
#
# Usage: ./load_field_tables.sh <input_dir> [port]

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <input_dir> [port]" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEMA_DIR="$SCRIPT_DIR/../schema/single-node"

INPUT_DIR="$1"
PORT="${2:-9000}"

DATABASE="oximeter"

# Error if database isn't empty.
echo "Checking for existing data..."
count=$(clickhouse client --port "$PORT" \
    --query "SELECT ifNull(sum(total_rows), 0) FROM system.tables WHERE database = '$DATABASE'")

if [[ "$count" -gt 0 ]]; then
    echo "Error: $DATABASE database already contains data ($count rows)"
    echo "Refusing to initialize a non-empty database."
    exit 1
fi

# Initialize schema.
echo "Initializing database schema..."
clickhouse client --port "$PORT" --multiquery < "$SCHEMA_DIR/db-init.sql"

# Load backups.
#
# Note: Use INSERT rather than RESTORE because we may not have access to the
# remote ClickHouse's local disk, or have backups enabled at all.
for table in timeseries_schema fields_{bool,i8,i16,i32,i64,ipaddr,string,u8,u16,u32,u64,uuid}; do
    input="$INPUT_DIR/${table}.native.gz"
    if [[ ! -f "$input" ]]; then
        echo "No backup for table $table; skipping"
        continue
    fi
    echo "Loading $table"
    gunzip -c "$input" | clickhouse client --port "$PORT" \
        --query "INSERT INTO $DATABASE.$table FORMAT Native"
done
