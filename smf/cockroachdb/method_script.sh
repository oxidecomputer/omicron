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

# We need to tell CockroachDB the DNS names or IP addresses of the other nodes
# in the cluster.  Look these up in internal DNS.  Per the recommendations in
# the CockroachDB docs, we choose at most five addresses.  Providing more
# addresses just increases the time for the cluster to stabilize.
JOIN_ADDRS="$(/opt/oxide/internal-dns-cli/bin/dnswait cockroach \
    | head -n 5 \
    | tr '\n' ,)"

if [[ -z "$JOIN_ADDRS" ]]; then
    printf 'ERROR: found no addresses for other CockroachDB nodes\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

args=(
  '--insecure'
  '--listen-addr' "[$LISTEN_ADDR]:$LISTEN_PORT"
  '--http-addr' '127.0.0.1:8080'
  '--store' "$DATASTORE"
  '--join' "$JOIN_ADDRS"
)

exec /opt/oxide/cockroachdb/bin/cockroach start "${args[@]}" &
