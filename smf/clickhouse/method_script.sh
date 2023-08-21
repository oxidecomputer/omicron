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

# Retrieve hostnames (SRV records in internal DNS) of the clickhouse nodes.
CH_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse -H)"

if [[ -z "$CH_ADDRS" ]]; then
    printf 'ERROR: found no hostnames for other ClickHouse nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

declare -a nodes=($CH_ADDRS)

# TODO: If it is a single zone start in single node mode (check ch and keepers?), 
# else do all this dance

for i in "${nodes[@]}"
do
  if ! grep -q "host.control-plane.oxide.internal" <<< "${i}"; then
    printf 'ERROR: retrieved ClickHouse hostname does not match the expected format\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
  fi
done

if [[ "${#nodes[@]}" != 2 ]]
then
  printf "ERROR: expected 2 ClickHouse hosts, found "${#nodes[@]}" instead\n" >&2
  exit "$SMF_EXIT_ERR_CONFIG"
fi

# Assign hostnames to replicas
REPLICA_HOST_01="${nodes[0]}"
REPLICA_HOST_02="${nodes[1]}"

# Retrieve hostnames (SRV records in internal DNS) of the keeper nodes.
K_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse-keeper -H)"

if [[ -z "$K_ADDRS" ]]; then
    printf 'ERROR: found no hostnames for other ClickHouse Keeper nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

declare -a keepers=($K_ADDRS)

for i in "${keepers[@]}"
do
  if ! grep -q "host.control-plane.oxide.internal" <<< "${i}"; then
    printf 'ERROR: retrieved ClickHouse Keeper hostname does not match the expected format\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
  fi
done

if [[ "${#keepers[@]}" != 3 ]]
then
  printf "ERROR: expected 3 ClickHouse Keeper hosts, found "${#keepers[@]}" instead\n" >&2
  exit "$SMF_EXIT_ERR_CONFIG"
fi

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
    printf 'ERROR: service name does not match any of the identified ClickHouse hostnames\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

# Setting environment variables this way is best practice, but has the downside of
# obscuring the field values to anyone ssh-ing into the zone. To mitigate this,
# we will be saving them to ${DATASTORE}/config_env_vars
#
# TODO: If the node crashes, and we want to manually restart it, we would have to 
# export all of these variables before restarting the service. Is this what we want?
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

content="CH_LOG="${CH_LOG}"\n\
CH_ERROR_LOG="${CH_ERROR_LOG}"\n\
CH_REPLICA_DISPLAY_NAME="${CH_REPLICA_DISPLAY_NAME}"\n\
CH_LISTEN_ADDR="${CH_LISTEN_ADDR}"\n\
CH_LISTEN_PORT="${CH_LISTEN_PORT}"\n\
CH_DATASTORE="${CH_DATASTORE}"\n\
CH_TMP_PATH="${CH_TMP_PATH}"\n\
CH_USER_FILES_PATH="${CH_USER_FILES_PATH}"\n\
CH_USER_LOCAL_DIR="${CH_USER_LOCAL_DIR}"\n\
CH_FORMAT_SCHEMA_PATH="${CH_FORMAT_SCHEMA_PATH}"\n\
CH_REPLICA_NUMBER="${CH_REPLICA_NUMBER}"\n\
CH_REPLICA_HOST_01="${CH_REPLICA_HOST_01}"\n\
CH_REPLICA_HOST_02="${CH_REPLICA_HOST_02}"\n\
CH_KEEPER_HOST_01="${CH_KEEPER_HOST_01}"\n\
CH_KEEPER_HOST_02="${CH_KEEPER_HOST_02}"\n\
CH_KEEPER_HOST_03="${CH_KEEPER_HOST_03}""

echo $content >> "${DATASTORE}/config_env_vars"


# The clickhouse binary must be run from within the directory that contains it. 
# Otherwise, it does not automatically detect the configuration files, nor does
# it append them when necessary
cd /opt/oxide/clickhouse/  
exec ./clickhouse server &