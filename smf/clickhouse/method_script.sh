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

# Making Keeper IDs dynamic instead of hardcoding them in the config as we may 
# want to name them something other than 01, 02, and 03 in the future. 
# Also, when a keeper node is unrecoverable the ID must be changed to something new. 
# I am not sure how we'll handle this in the future, but making these dynamic 
# seems sensible for the time being.
KEEPER_ID_01="01"
KEEPER_ID_02="02"
KEEPER_ID_03="03"

# Identify the node type this is as this will influence how the config is constructed
# TODO: There are probably much better ways to do this service discovery, but this works
# for now
CLICKHOUSE_SVC="$(zoneadm list | sed -En s/oxz_clickhouse_//p)"
REPLICA_IDENTIFIER_01="$( echo "${REPLICA_HOST_01}" | sed -En s/.host.control-plane.oxide.internal.//p)"
REPLICA_IDENTIFIER_02="$( echo "${REPLICA_HOST_02}" | sed -En s/.host.control-plane.oxide.internal.//p)"
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

# Populate corresponding template and copy to /opt/oxide/clickhouse/clickhouse/config.d/
# or /opt/oxide/clickhouse/clickhouse/keeper_config.xml
# Clickhouse recommends to have these files in `/etc/clickhouse-keeper/` and `/etc/clickhouse-server/config.d/`,
# but I'm not sure this makes sense to us since we're building the entire clickhouse binary instead of separate
# `clickhouse-server`, 'clickhouse-keeper' and 'clickhouse-client' binaries.

sed -i "s~REPLICA_DISPLAY_NAME~$REPLICA_DISPLAY_NAME~g; \
    s~LISTEN_ADDR~$LISTEN_ADDR~g; \
    s~LISTEN_PORT~$LISTEN_PORT~g; \
    s~DATASTORE~$DATASTORE~g; \
    s~REPLICA_NUMBER~$REPLICA_NUMBER~g; \
    s~REPLICA_HOST_01~$REPLICA_HOST_01~g; \
    s~REPLICA_HOST_02~$REPLICA_HOST_02~g; \
    s~KEEPER_HOST_01~$KEEPER_HOST_01~g; \
    s~KEEPER_HOST_02~$KEEPER_HOST_02~g; \
    s~KEEPER_HOST_03~$KEEPER_HOST_03~g" \
    /opt/oxide/clickhouse/config.d/config_replica.xml
    
    exec /opt/oxide/clickhouse/clickhouse server \
     --config /opt/oxide/clickhouse/config.d/config_replica.xml &
