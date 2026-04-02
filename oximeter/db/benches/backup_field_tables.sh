#!/bin/bash
#
# Dump ClickHouse field and schema tables to disk in native format. Run against
# a test rack with realistic oximeter data. Used to capture test data for
# benchmarking.
#
# Usage: ./backup_field_tables.sh <output_dir> [port]

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <output_dir> [port]" >&2
    exit 1
fi

OUTPUT_DIR="$1"
PORT="${2:-9000}"
DATABASE="oximeter"

mkdir -p "$OUTPUT_DIR"

# Back up field tables.
#
# Note: Use SELECT rather than RESTORE because we may not have access to the
# remote ClickHouse's local disk, or have backups enabled at all.
for table in timeseries_schema fields_{bool,i8,i16,i32,i64,ipaddr,string,u8,u16,u32,u64,uuid}; do
    count=$(clickhouse client --port "$PORT" \
        --query "SELECT count() FROM $DATABASE.$table")
    if [[ "$count" -eq 0 ]]; then
        echo "No rows in table $DATABASE.$table; skipping"
        continue
    fi
    output="$OUTPUT_DIR/${table}.native.gz"
    echo "Backing up $DATABASE.$table ($count rows) to $output"
    clickhouse client --port "$PORT" \
        --query "SELECT * FROM $DATABASE.$table FORMAT Native" \
        | gzip > "$output"
done
