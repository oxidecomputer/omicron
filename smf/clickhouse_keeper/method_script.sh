#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

exec /opt/oxide/clickhouse_keeper/clickhouse keeper --config /opt/oxide/clickhouse_keeper/keeper_config.xml &
