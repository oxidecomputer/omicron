#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
HTTP_ADDR="$(svcprop -c -p config/http_addr "${SMF_FMRI}")"
DATASTORE="$(svcprop -c -p config/store "${SMF_FMRI}")"

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
  '--listen-addr' "$LISTEN_ADDR"
  '--http-addr' "$HTTP_ADDR"
  '--store' "$DATASTORE"
  '--join' "$JOIN_ADDRS"
)

exec /opt/oxide/cockroachdb/bin/cockroach start "${args[@]}" &
