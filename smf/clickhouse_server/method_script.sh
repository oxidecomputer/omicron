#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

# The clickhouse binary must be run from within the directory that contains it. 
# Otherwise, it does not automatically detect the configuration files, nor does
# it append them when necessary
cd /opt/oxide/clickhouse_server/
exec ./clickhouse server &