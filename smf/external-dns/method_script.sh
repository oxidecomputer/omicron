#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

# TODO: OPTE variables to be moved to opte-nw-service
OPTE_INTERFACE="$(svcprop -c -p config/opte_interface "${SMF_FMRI}")"
OPTE_GATEWAY="$(svcprop -c -p config/opte_gateway "${SMF_FMRI}")"

HTTP_ADDR="$(svcprop -c -p config/http_address "${SMF_FMRI}")"
DNS_ADDR="$(svcprop -c -p config/dns_address "${SMF_FMRI}")"

# TODO: This should be its own service
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

# TODO: This command and args will be moved back to the start method on the manifest.xml file
args=(
  "--config-file" "/var/svc/manifest/site/external_dns/config.toml"
  "--http-address" "$HTTP_ADDR"
  "--dns-address" "$DNS_ADDR"
)

exec /opt/oxide/dns-server/bin/dns-server "${args[@]}" &