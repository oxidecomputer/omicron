#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

#
# Dump a partial ClickHouse measurement table (time-windowed slice) to disk in
# native format. Run against a test rack with realistic oximeter data. Used to
# capture test data for benchmarking.
#
# Usage: ./backup_measurement_tables.sh <output_dir> <table> <window_start> <window_end> [port]

set -euo pipefail

if [[ $# -lt 4 ]]; then
    echo "Usage: $0 <output_dir> <table> <window_start> <window_end> [port]" >&2
    exit 1
fi

OUTPUT_DIR="$1"
TABLE="$2"
WINDOW_START="$3"
WINDOW_END="$4"
PORT="${5:-9000}"
DATABASE="oximeter"

mkdir -p "$OUTPUT_DIR"

# Back up a single measurement table. These tables can be very large, so we limit to the specified time range, and only operate on one measurement table at a time.

# Note: Use SELECT rather than RESTORE because we may not have access to the
# remote ClickHouse's local disk, or have backups enabled at all.
count=$(clickhouse client --port "$PORT" \
    --query "SELECT count() FROM $DATABASE.$TABLE WHERE timestamp >= '$WINDOW_START' AND timestamp < '$WINDOW_END'")
if [[ "$count" -eq 0 ]]; then
    echo "No rows in $DATABASE.$TABLE for window; nothing to back up"
    exit 0
fi
output="$OUTPUT_DIR/${TABLE}.native.gz"
echo "Backing up $DATABASE.$TABLE ($count rows) to $output"
clickhouse client --port "$PORT" --compression=1 \
    --query "SELECT * FROM $DATABASE.$TABLE WHERE timestamp >= '$WINDOW_START' AND timestamp < '$WINDOW_END' FORMAT Native" \
    | gzip > "$output"
