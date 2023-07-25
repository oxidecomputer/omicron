#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"
# TODO: Not sure if I need these?
# DATASTORE="$(svcprop -c -p config/store "${SMF_FMRI}")"
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
#
# This should probably be 5 keepers?
CH_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse-keeper \
    | head -n 3 \
    | tr '\n' ,)"

if [[ -z "$CH_ADDRS" ]]; then
    printf 'ERROR: found no addresses for other ClickHouse Keeper nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

readarray -td, nodes <<<"$CH_ADDRS,"; declare -p nodes

# Assign addresses to replicas and keeper nodes
KEEPER_HOST_01="$(echo "${nodes[0]}" | sed -En s/:9181//p)"
KEEPER_HOST_02="$(echo "${nodes[1]}" | sed -En s/:9181//p)"
KEEPER_HOST_03="$(echo "${nodes[2]}" | sed -En s/:9181//p)"
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
KEEPER_SVC="$(zoneadm list | sed -En s/oxz_clickhouse_keeper_//p)"
KEEPER_IDENTIFIER_01="$( echo "${KEEPER_HOST_01}" | sed -En s/.host.control-plane.oxide.internal.//p)"
KEEPER_IDENTIFIER_02="$( echo "${KEEPER_HOST_02}" | sed -En s/.host.control-plane.oxide.internal.//p)"
KEEPER_IDENTIFIER_03="$( echo "${KEEPER_HOST_03}" | sed -En s/.host.control-plane.oxide.internal.//p)"
if [[ $KEEPER_IDENTIFIER_01 == $KEEPER_SVC ]]
then
    KEEPER_ID_CURRENT=$KEEPER_ID_01
elif [[ $KEEPER_IDENTIFIER_02 == $KEEPER_SVC ]]
then
    KEEPER_ID_CURRENT=$KEEPER_ID_02
elif [[ $KEEPER_IDENTIFIER_03 == $KEEPER_SVC ]]
then
    KEEPER_ID_CURRENT=$KEEPER_ID_03
else
    printf 'ERROR: listen address does not match any of the identified ClickHouse Keeper addresses\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

# TODO: Looks like I have to create the /var/lib/clickhouse/coordination here. Clickhouse is supposed
# to do this itself, but for some reason it's not creating it

# Populate corresponding template and copy to /opt/oxide/clickhouse/clickhouse/config.d/
# or /opt/oxide/clickhouse_keeper/clickhouse/keeper_config.xml
# Clickhouse recommends to have these files in `/etc/clickhouse-keeper/` and `/etc/clickhouse-server/config.d/`,
# but I'm not sure this makes sense to us since we're building the entire clickhouse binary instead of separate
# `clickhouse-server`, 'clickhouse-keeper' and 'clickhouse-client' binaries.
sed -i "s~LISTEN_PORT~$LISTEN_PORT~g; \
    s~KEEPER_ID_CURRENT~$KEEPER_ID_CURRENT~g; \
    s~KEEPER_ID_01~$KEEPER_ID_01~g; \
    s~KEEPER_HOST_01~$KEEPER_HOST_01~g; \
    s~KEEPER_ID_02~$KEEPER_ID_02~g; \
    s~KEEPER_HOST_02~$KEEPER_HOST_02~g; \
    s~KEEPER_ID_03~$KEEPER_ID_03~g; \
    s~KEEPER_HOST_03~$KEEPER_HOST_03~g" \
    /opt/oxide/clickhouse_keeper/keeper_config.xml

exec /opt/oxide/clickhouse_keeper/clickhouse keeper enable \
 --config /opt/oxide/clickhouse_keeper/keeper_config.xml &
