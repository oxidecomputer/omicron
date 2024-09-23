#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"
DATASTORE="$(svcprop -c -p config/store "${SMF_FMRI}")"

# Retrieve hostnames (SRV records in internal DNS) of the clickhouse nodes.
CH_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait clickhouse-server -H)"

if [[ -z "$CH_ADDRS" ]]; then
    printf 'ERROR: found no hostnames for other ClickHouse server nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

declare -a nodes=($CH_ADDRS)

for i in "${nodes[@]}"
do
  if ! grep -q "host.control-plane.oxide.internal" <<< "${i}"; then
    printf 'ERROR: retrieved ClickHouse hostname does not match the expected format\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
  fi
done

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
# TODO(https://github.com/oxidecomputer/omicron/issues/3824): There are probably much 
# better ways to do this service discovery, but this works for now. 
# The services contain the same IDs as the hostnames.
CLICKHOUSE_SVC="$(zonename | tr -dc [:digit:])"
REPLICA_IDENTIFIER_01="$( echo "${REPLICA_HOST_01}" | tr -dc [:digit:])"
REPLICA_IDENTIFIER_02="$( echo "${REPLICA_HOST_02}" | tr -dc [:digit:])"
if [[ $REPLICA_IDENTIFIER_01 == $CLICKHOUSE_SVC ]]
then
    REPLICA_DISPLAY_NAME="oximeter_cluster node 1"
    REPLICA_NUMBER=1
elif [[ $REPLICA_IDENTIFIER_02 == $CLICKHOUSE_SVC ]]
then
    REPLICA_DISPLAY_NAME="oximeter_cluster node 2"
    REPLICA_NUMBER=2
else
    printf 'ERROR: service name does not match any of the identified ClickHouse hostnames\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

curl -X put http://[${LISTEN_ADDR}]:8888/server/config \
 -H "Content-Type: application/json" \
 -d '{
  "generation": 0,
  "settings": {
    "id": '${REPLICA_NUMBER}',
    "keepers": [
      {
        "domain_name": "'${keepers[0]}'"
      },
      {
        "domain_name": "'${keepers[1]}'"
      },
      {
        "domain_name": "'${keepers[2]}'"
      }
    ],
    "remote_servers": [
      {
        "domain_name": "'${REPLICA_HOST_01}'"
      },
      {
        "domain_name": "'${REPLICA_HOST_02}'"
      }
    ],
    "config_dir": "/opt/oxide/clickhouse_server/config.d",
    "datastore_path": "'${DATASTORE}'",
    "listen_addr": "'${LISTEN_ADDR}'"
  }
}'

# The clickhouse binary must be run from within the directory that contains it. 
# Otherwise, it does not automatically detect the configuration files, nor does
# it append them when necessary
cd /opt/oxide/clickhouse_server/

exec ./clickhouse server &