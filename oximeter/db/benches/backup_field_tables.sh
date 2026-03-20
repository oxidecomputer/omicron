#!/bin/bash
#
# Dump ClickHouse field and schema tables to disk in native format. Run against
# a test rack with realistic oximeter data. Used to capture test data for
# benchmarking.
#
# Usage: ./backup_field_tables.sh [output_dir] [port]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR}"
PORT="${2:-9000}"
DATABASE="oximeter"

TABLES=(
    timeseries_schema
    fields_bool
    fields_i8
    fields_i16
    fields_i32
    fields_i64
    fields_ipaddr
    fields_string
    fields_u8
    fields_u16
    fields_u32
    fields_u64
    fields_uuid
)

mkdir -p "$OUTPUT_DIR"

for table in "${TABLES[@]}"; do
    count=$(clickhouse client --port "$PORT" \
        --query "SELECT count() FROM $DATABASE.$table")
    if [[ "$count" -eq 0 ]]; then
        echo "Skipping $DATABASE.$table (empty)"
        continue
    fi
    output="$OUTPUT_DIR/${table}.native.gz"
    echo "Backing up $DATABASE.$table ($count rows) to $output"
    clickhouse client --port "$PORT" \
        --query "SELECT * FROM $DATABASE.$table FORMAT Native" \
        | gzip > "$output"
done
