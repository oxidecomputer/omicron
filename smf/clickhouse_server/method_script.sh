#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

exec /opt/oxide/clickhouse_server/clickhouse server --config /opt/oxide/clickhouse_server/config.d/replica-server-config.xml &
