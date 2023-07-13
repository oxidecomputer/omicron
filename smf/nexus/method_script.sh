#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
DATALINK="$(svcprop -c -p config/datalink "${SMF_FMRI}")"
OPTE_INTERFACE="$(svcprop -c -p config/opte_interface "${SMF_FMRI}")"
OPTE_GATEWAY="$(svcprop -c -p config/opte_gateway "${SMF_FMRI}")"
GATEWAY="$(svcprop -c -p config/gateway "${SMF_FMRI}")"

if [[ $DATALINK == unknown ]] || [[ $GATEWAY == unknown ]]; then
    printf 'ERROR: missing datalink or gateway\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

# Set up underlay
ipadm show-addr "$DATALINK/ll" || ipadm create-addr -t -T addrconf "$DATALINK/ll"
ipadm show-addr "$DATALINK/omicron6"  || ipadm create-addr -t -T static -a "$LISTEN_ADDR" "$DATALINK/omicron6"
route get -inet6 default -inet6 "$GATEWAY" || route add -inet6 default -inet6 "$GATEWAY"

# Set up OPTE interface
if [[ "$OPTE_GATEWAY" =~ .*:.* ]]; then
    # IPv6 gateway
    echo "IPv6 OPTE gateways are not yet supported"
    exit 1
else
    # IPv4 gateway
    ipadm show-addr "$OPTE_INTERFACE/public"  || ipadm create-addr -t -T dhcp "$OPTE_INTERFACE/public"
    OPTE_IP=$(ipadm show-addr -p -o ADDR "$OPTE_INTERFACE/public" | cut -d'/' -f 1)
    route get -host "$OPTE_GATEWAY" "$OPTE_IP" -interface -ifp "$OPTE_INTERFACE" || route add -host "$OPTE_GATEWAY" "$OPTE_IP" -interface -ifp "$OPTE_INTERFACE"
    route get -inet default "$OPTE_GATEWAY" || route add -inet default "$OPTE_GATEWAY"
fi

args=(
  "--external-ip-override" "$OPTE_IP"
  "/var/svc/manifest/site/nexus/config.toml"
)

ctrun -l child -o noorphan,regent /opt/oxide/nexus/bin/nexus "${args[@]}" &
