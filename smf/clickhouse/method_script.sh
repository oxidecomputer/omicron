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

ipadm show-addr "$DATALINK/ll" || ipadm create-addr -t -T addrconf "$DATALINK/ll"
ipadm show-addr "$DATALINK/omicron6"  || ipadm create-addr -t -T static -a "$LISTEN_ADDR" "$DATALINK/omicron6"
route get -inet6 default -inet6 "$GATEWAY" || route add -inet6 default -inet6 "$GATEWAY"

# Retrieve addresses of the other clickhouse nodes, order them and assign them to be a replica or keeper node.
# In a follow up PR, keepers will be their own service.
CH_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse \
    | head -n 5 \
    | tr '\n' ,)"

if [[ -z "$CH_ADDRS" ]]; then
    printf 'ERROR: found no addresses for other ClickHouse nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

readarray -td, nodes <<<"$CH_ADDRS,"; declare -p nodes

# Assign addresses to replicas and keeper nodes
REPLICA_HOST_01=${nodes[0]}
REPLICA_HOST_02=${nodes[1]}
KEEPER_HOST_01=${nodes[2]}
KEEPER_HOST_02=${nodes[3]}
KEEPER_HOST_03=${nodes[4]}
# Making Keeper IDs dynamic instead of hardcoding them in the config as we may 
# want to name them something other than 01, 02, and 03 in the future. 
# Also, when a keeper node is unrecoverable the ID must be changed to something new. 
# I am not sure how we'll handle this in the future, but making these dynamic 
# seems sensible for the time being.
KEEPER_ID_01="01"
KEEPER_ID_02="02"
KEEPER_ID_03="03"

# Identify the node type this is as this will influence how the config is constructed
if [[ $REPLICA_HOST_01 == $LISTEN_ADDR ]]
then
    NODE_TYPE=replica
    REPLICA_DISPLAY_NAME="oximeter_cluster node 1"
    REPLICA_NUMBER="01"
elif [[ $REPLICA_HOST_02 == $LISTEN_ADDR ]]
then
    NODE_TYPE=replica
    REPLICA_DISPLAY_NAME="oximeter_cluster node 2"
    REPLICA_NUMBER="02"
elif [[ $KEEPER_HOST_01 == $LISTEN_ADDR ]]
then
    NODE_TYPE=keeper
    KEEPER_ID_CURRENT=$KEEPER_ID_01
elif [[ $KEEPER_HOST_02 == $LISTEN_ADDR ]]
then
    NODE_TYPE=keeper
    KEEPER_ID_CURRENT=$KEEPER_ID_02
elif [[ $KEEPER_HOST_03 == $LISTEN_ADDR ]]
then
    NODE_TYPE=keeper
    KEEPER_ID_CURRENT=$KEEPER_ID_03
else
    printf 'ERROR: listen address does not match any of the identified ClickHouse addresses\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

# Populate corresponding template and copy to /opt/oxide/clickhouse/clickhouse/config.d/
# or /opt/oxide/clickhouse/clickhouse/keeper_config.xml
# Clickhouse recommends to have these files in `/etc/clickhouse-keeper/` and `/etc/clickhouse-server/config.d/`,
# but I'm not sure this makes sense to us since we're building the entire clickhouse binary instead of separate
# `clickhouse-server`, 'clickhouse-keeper' and 'clickhouse-client' binaries.

if [[ $NODE_TYPE == replica ]]
then
    sed -i "s/REPLICA_DISPLAY_NAME/$REPLICA_DISPLAY_NAME/g; \
        s/LISTEN_ADDR/$LISTEN_ADDR/g; \
        s/LISTEN_PORT/$LISTEN_PORT/g; \
        s/DATASTORE/$DATASTORE/g; \
        s/REPLICA_NUMBER/$REPLICA_NUMBER/g; \
        s/REPLICA_HOST_01/$REPLICA_HOST_01/g; \
        s/REPLICA_HOST_02/$REPLICA_HOST_02/g; \
        s/KEEPER_HOST_01/$KEEPER_HOST_01/g; \
        s/KEEPER_HOST_02/$KEEPER_HOST_02/g; \
        s/KEEPER_HOST_03/$KEEPER_HOST_03/g" \
        /opt/oxide/clickhouse/clickhouse/config-replica-tpl.xml
        
        exec /opt/oxide/clickhouse/clickhouse server \
         --config /opt/oxide/clickhouse/clickhouse/config-replica-tpl.xml &
elif [[ $NODE_TYPE == keeper ]]
then
    sed -i "s/KEEPER_ID_CURRENT/$KEEPER_ID_CURRENT; \
        s/KEEPER_ID_01/$KEEPER_ID_01/g; \
        s/KEEPER_HOST_01/$KEEPER_HOST_01/g; \
        s/KEEPER_ID_02/$KEEPER_ID_02/g; \
        s/KEEPER_HOST_02/$KEEPER_HOST_02/g; \
        s/KEEPER_ID_03/$KEEPER_ID_03/g; \
        s/KEEPER_HOST_03/$KEEPER_HOST_03/g" \
        /opt/oxide/clickhouse/clickhouse/config-keeper-tpl.xml

        exec /opt/oxide/clickhouse/clickhouse keeper enable \
         --config /opt/oxide/clickhouse/clickhouse/config-keeper-tpl.xml &
else
    printf 'ERROR: node has not been assigned as a replica nor keeper\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi
