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

# Retrieve addresses of the other clickhouse nodes, order them and assign them to be a replica node.
#
# TODO: This should probably be 3 replicas
CH_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse \
    | head -n 2 \
    | tr '\n' ,)"

if [[ -z "$CH_ADDRS" ]]; then
    printf 'ERROR: found no addresses for other ClickHouse nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

readarray -td, nodes <<<"$CH_ADDRS,"; declare -p nodes

# Assign addresses to replicas without port
REPLICA_HOST_01="$(echo "${nodes[0]}" | sed -En s/:8123//p)"
REPLICA_HOST_02="$(echo "${nodes[1]}" | sed -En s/:8123//p)"

# Retrieve addresses of the other clickhouse nodes, order them and assign them to be a keeper node.
#
# This should probably be 5 keepers?
K_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse-keeper \
    | head -n 3 \
    | tr '\n' ,)"

if [[ -z "$K_ADDRS" ]]; then
    printf 'ERROR: found no addresses for other ClickHouse Keeper nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

readarray -td, keepers <<<"$K_ADDRS,"; declare -p keepers

# Assign addresses to replicas and keeper nodes
KEEPER_HOST_01="$(echo "${keepers[0]}" | sed -En s/:9181//p)"
KEEPER_HOST_02="$(echo "${keepers[1]}" | sed -En s/:9181//p)"
KEEPER_HOST_03="$(echo "${keepers[2]}" | sed -En s/:9181//p)"

# Identify the node type this is as this will influence how the config is constructed
# TODO: There are probably much better ways to do this service discovery, but this works
# for now. The services contain the same IDs as the hostnames.
CLICKHOUSE_SVC="$(zoneadm list | tr -dc [:digit:])"
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
# obscuring the field values to anyone ssh=ing into the zone.
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
export CH_KEEPER_HOST_01=${KEEPER_HOST_01}
export CH_KEEPER_HOST_02=${KEEPER_HOST_02}
export CH_KEEPER_HOST_03=${KEEPER_HOST_03}

# The clickhouse binary must be run from within the directory that contains it. 
# Otherwise, it does not automatically detect the configuration files, nor does
# it append them when necessary
cd /opt/oxide/clickhouse/  
exec ./clickhouse server &