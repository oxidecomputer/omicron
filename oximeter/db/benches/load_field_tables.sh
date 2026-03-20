#!/bin/bash
#
# Load field table backups into a fresh ClickHouse for benchmarking.
# Safety: refuses to run if the database already contains data.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEMA_DIR="$SCRIPT_DIR/../schema/single-node"

INPUT_DIR="${1:-$SCRIPT_DIR}"
PORT="${2:-9000}"

DATABASE="oximeter"

# Check if database exists and has data
echo "Checking for existing data..."
count=$(clickhouse client --port "$PORT" \
    --query "SELECT ifNull(sum(total_rows), 0) FROM system.tables WHERE database = '$DATABASE'" \
    2>/dev/null || echo "0")

if [[ "$count" -gt 0 ]]; then
    echo "Error: $DATABASE database already contains data ($count rows)"
    echo "Refusing to initialize a non-empty database."
    exit 1
fi

# Initialize schema
echo "Initializing database schema..."
clickhouse client --port "$PORT" --multiquery < "$SCHEMA_DIR/db-init.sql"

# Load backups
for table in timeseries_schema fields_{bool,i8,i16,i32,i64,ipaddr,string,u8,u16,u32,u64,uuid}; do
    input="$INPUT_DIR/${table}.native.gz"
    if [[ ! -f "$input" ]]; then
        echo "Skipping $table (no backup)"
        continue
    fi
    echo "Loading $table"
    gunzip -c "$input" | clickhouse client --port "$PORT" \
        --query "INSERT INTO $DATABASE.$table FORMAT Native"
done
