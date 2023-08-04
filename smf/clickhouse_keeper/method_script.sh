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

# Retrieve addresses of all keeper nodes.
K_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse-keeper -H \
    | head -n 3 \
    | tr '\n' ,)"

if [[ -z "$K_ADDRS" ]]; then
    printf 'ERROR: found no addresses for other ClickHouse Keeper nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

readarray -td, nodes <<<"$K_ADDRS,"; declare -p nodes

for i in {0..2}
do
  if ! grep -q "host.control-plane.oxide.internal" <<< "${nodes["${i}"]}"; then
    printf 'ERROR: retrieved ClickHouse Keeper address does not match the expected format\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
  fi
done

# Assign addresses to replicas and keeper nodes
KEEPER_HOST_01="${nodes[0]}"
KEEPER_HOST_02="${nodes[1]}"
KEEPER_HOST_03="${nodes[2]}"

# Generate unique reproduceable number IDs by removing letters from KEEPER_IDENTIFIER_*
# Keeper IDs must be numbers, and they cannot be reused (i.e. when a keeper node is 
# unrecoverable the ID must be changed to something new.). 
# By trimming the hosts we can make sure all keepers will always be up to date when 
# a new keeper is spun up. Clickhouse does not allow very large numbers, so we will
# be reducing to 7 characters. This should be enough entropy given the small amount
# of keepers we have.
KEEPER_ID_01="$( echo "${KEEPER_HOST_01}" | tr -dc [:digit:] | cut -c1-7)"
KEEPER_ID_02="$( echo "${KEEPER_HOST_02}" | tr -dc [:digit:] | cut -c1-7)"
KEEPER_ID_03="$( echo "${KEEPER_HOST_03}" | tr -dc [:digit:] | cut -c1-7)"

# Identify the node type this is as this will influence how the config is constructed
# TODO: There are probably much better ways to do this service name lookup, but this works
# for now. The services contain the same IDs as the hostnames.
KEEPER_SVC="$(zonename | tr -dc [:digit:] | cut -c1-7)"
if [[ $KEEPER_ID_01 == $KEEPER_SVC ]]
then
    KEEPER_ID_CURRENT=$KEEPER_ID_01
elif [[ $KEEPER_ID_02 == $KEEPER_SVC ]]
then
    KEEPER_ID_CURRENT=$KEEPER_ID_02
elif [[ $KEEPER_ID_03 == $KEEPER_SVC ]]
then
    KEEPER_ID_CURRENT=$KEEPER_ID_03
else
    printf 'ERROR: listen address does not match any of the identified ClickHouse Keeper addresses\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

# Setting environment variables this way is best practice, but has the downside of
# obscuring the field values to anyone ssh=ing into the zone.
export CH_LOG="${DATASTORE}/clickhouse-keeper.log"
export CH_ERROR_LOG="${DATASTORE}/clickhouse-keeper.err.log"
export CH_LISTEN_ADDR=${LISTEN_ADDR}
export CH_DATASTORE=${DATASTORE}
export CH_LISTEN_PORT=${LISTEN_PORT}
export CH_KEEPER_ID_CURRENT=${KEEPER_ID_CURRENT}
export CH_LOG_STORAGE_PATH="${DATASTORE}/log"
export CH_SNAPSHOT_STORAGE_PATH="${DATASTORE}/snapshots"
export CH_KEEPER_ID_01=${KEEPER_ID_01}
export CH_KEEPER_ID_02=${KEEPER_ID_02}
export CH_KEEPER_ID_03=${KEEPER_ID_03}
export CH_KEEPER_HOST_01=${KEEPER_HOST_01}
export CH_KEEPER_HOST_02=${KEEPER_HOST_02}
export CH_KEEPER_HOST_03=${KEEPER_HOST_03}

# The clickhouse binary must be run from within the directory that contains it. 
# Otherwise, it does not automatically detect the configuration files, nor does
# it append them when necessary
cd /opt/oxide/clickhouse_keeper/
exec ./clickhouse keeper &
