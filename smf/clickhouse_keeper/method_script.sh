#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"
DATASTORE="$(svcprop -c -p config/store "${SMF_FMRI}")"

# Retrieve hostnames (SRV records in internal DNS) of all keeper nodes.
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

# Assign hostnames to replicas and keeper nodes
KEEPER_HOST_01="${keepers[0]}"
KEEPER_HOST_02="${keepers[1]}"
KEEPER_HOST_03="${keepers[2]}"

# Generate unique reproduceable number IDs by removing letters from
# KEEPER_IDENTIFIER_* Keeper IDs must be numbers, and they cannot be reused
# (i.e. when a keeper node is unrecoverable the ID must be changed to something
# new). By trimming the hosts we can make sure all keepers will always be up to
# date when a new keeper is spun up. Clickhouse does not allow very large
# numbers, so we will be reducing to 7 characters. This should be enough
# entropy given the small amount of keepers we have.
KEEPER_ID_01="$( echo "${KEEPER_HOST_01}" | tr -dc [:digit:] | cut -c1-7)"
KEEPER_ID_02="$( echo "${KEEPER_HOST_02}" | tr -dc [:digit:] | cut -c1-7)"
KEEPER_ID_03="$( echo "${KEEPER_HOST_03}" | tr -dc [:digit:] | cut -c1-7)"

# Identify the node type this is as this will influence how the config is
# constructed
# TODO(https://github.com/oxidecomputer/omicron/issues/3824): There are
# probably much better ways to do this service name lookup, but this works for
# now. The services contain the same IDs as the hostnames.
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
    printf 'ERROR: service name does not match any of the identified ClickHouse Keeper hostnames\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

curl -X put http://[${LISTEN_ADDR}]:8888/keeper/config \
-H "Content-Type: application/json" \
-d '{
  "generation": 0,
  "settings": {
    "id": '${KEEPER_ID_CURRENT}',
    "raft_servers": [
      {
        "id": '${KEEPER_ID_01}',
        "host": {
          "domain_name": "'${KEEPER_HOST_01}'"
        }
      },
      {
        "id": '${KEEPER_ID_02}',
        "host": {
          "domain_name": "'${KEEPER_HOST_02}'"
        }
      },
      {
        "id": '${KEEPER_ID_03}',
        "host": {
          "domain_name": "'${KEEPER_HOST_03}'"
        }
      }
    ],
    "config_dir": "/opt/oxide/clickhouse_keeper",
    "datastore_path": "'${DATASTORE}'",
    "listen_addr": "'${LISTEN_ADDR}'"
  }
}'

# The clickhouse binary must be run from within the directory that contains it.
# Otherwise, it does not automatically detect the configuration files, nor does
# it append them when necessary
cd /opt/oxide/clickhouse_keeper/
exec ./clickhouse keeper &
