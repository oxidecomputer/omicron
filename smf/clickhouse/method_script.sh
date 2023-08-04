#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"
DATASTORE="$(svcprop -c -p config/store "${SMF_FMRI}")"
DATALINK="$(svcprop -c -p config/datalink "${SMF_FMRI}")"
GATEWAY="$(svcprop -c -p config/gateway "${SMF_FMRI}")"

if [[ $DATALINK == unknown ]] || [[ $GATEWAY == unknown ]]; then
    printf 'ERROR: missing datalink or gateway\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

# TODO remove when https://github.com/oxidecomputer/stlouis/issues/435 is addressed
ipadm delete-if "$DATALINK" || true
ipadm create-if -t "$DATALINK"

ipadm set-ifprop -t -p mtu=9000 -m ipv4 "$DATALINK"
ipadm set-ifprop -t -p mtu=9000 -m ipv6 "$DATALINK"

ipadm show-addr "$DATALINK/ll" || ipadm create-addr -t -T addrconf "$DATALINK/ll"
ipadm show-addr "$DATALINK/omicron6"  || ipadm create-addr -t -T static -a "$LISTEN_ADDR" "$DATALINK/omicron6"
route get -inet6 default -inet6 "$GATEWAY" || route add -inet6 default -inet6 "$GATEWAY"

# Retrieve addresses of the clickhouse nodes.
CH_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse -H \
    | head -n 2 \
    | tr '\n' ,)"

if [[ -z "$CH_ADDRS" ]]; then
    printf 'ERROR: found no addresses for other ClickHouse nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

readarray -td, nodes <<<"$CH_ADDRS,"; declare -p nodes

for i in {0..1}
do
  if ! grep -q "host.control-plane.oxide.internal" <<< "${nodes["${i}"]}"; then
    printf 'ERROR: retrieved ClickHouse address does not match the expected format\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
  fi
done

# Assign addresses to replicas
REPLICA_HOST_01="${nodes[0]}"
REPLICA_HOST_02="${nodes[1]}"

# Retrieve addresses of the keeper nodes.
K_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse-keeper -H \
    | head -n 3 \
    | tr '\n' ,)"

if [[ -z "$K_ADDRS" ]]; then
    printf 'ERROR: found no addresses for other ClickHouse Keeper nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

readarray -td, keepers <<<"$K_ADDRS,"; declare -p keepers

for i in {0..2}
do
  if ! grep -q "host.control-plane.oxide.internal" <<< "${nodes["${i}"]}"; then
    printf 'ERROR: retrieved ClickHouse Keeper address does not match the expected format\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
  fi
done

# Identify the node type this is as this will influence how the config is constructed
# TODO: There are probably much better ways to do this service discovery, but this works
# for now. The services contain the same IDs as the hostnames.
CLICKHOUSE_SVC="$(zonename | tr -dc [:digit:])"
REPLICA_IDENTIFIER_01="$( echo "${REPLICA_HOST_01}" | tr -dc [:digit:])"
REPLICA_IDENTIFIER_02="$( echo "${REPLICA_HOST_02}" | tr -dc [:digit:])"
if [[ $REPLICA_IDENTIFIER_01 == $CLICKHOUSE_SVC ]]
then
    REPLICA_DISPLAY_NAME="oximeter_cluster node 1"
    REPLICA_NUMBER="01"
elif [[ $REPLICA_IDENTIFIER_02 == $CLICKHOUSE_SVC ]]
then
    REPLICA_DISPLAY_NAME="oximeter_cluster node 2"
    REPLICA_NUMBER="02"
else
    printf 'ERROR: listen address does not match any of the identified ClickHouse addresses\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

# Setting environment variables this way is best practice, but has the downside of
# obscuring the field values to anyone ssh-ing into the zone.
export CH_LOG="${DATASTORE}/clickhouse-server.log"
export CH_ERROR_LOG="${DATASTORE}/clickhouse-server.errlog"
export CH_REPLICA_DISPLAY_NAME=${REPLICA_DISPLAY_NAME}
export CH_LISTEN_ADDR=${LISTEN_ADDR}
export CH_LISTEN_PORT=${LISTEN_PORT}
export CH_DATASTORE=${DATASTORE}
export CH_TMP_PATH="${DATASTORE}/tmp/"
export CH_USER_FILES_PATH="${DATASTORE}/user_files/"
export CH_USER_LOCAL_DIR="${DATASTORE}/access/"
export CH_FORMAT_SCHEMA_PATH="${DATASTORE}/format_schemas/"
export CH_REPLICA_NUMBER=${REPLICA_NUMBER}
export CH_REPLICA_HOST_01=${REPLICA_HOST_01}
export CH_REPLICA_HOST_02=${REPLICA_HOST_02}
export CH_KEEPER_HOST_01="${keepers[0]}"
export CH_KEEPER_HOST_02="${keepers[1]}"
export CH_KEEPER_HOST_03="${keepers[2]}"

# The clickhouse binary must be run from within the directory that contains it. 
# Otherwise, it does not automatically detect the configuration files, nor does
# it append them when necessary
cd /opt/oxide/clickhouse/  
exec ./clickhouse server &