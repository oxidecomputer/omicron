#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

HTTP_ADDR="$(svcprop -c -p config/http_address "${SMF_FMRI}")"
DNS_PORT="$(svcprop -c -p config/dns_port "${SMF_FMRI}")"
OPTE_INTERFACE="$(svcprop -c -p config/opte_interface "${SMF_FMRI}")"
OPTE_IP="$(/opt/oxide/zone-network-cli/bin/zone-networking get-opte-ip -i "${OPTE_INTERFACE}")"

args=(
  "--config-file" "/var/svc/manifest/site/external_dns/config.toml"
  "--http-address" "$HTTP_ADDR"
  "--dns-address" "$OPTE_IP:$DNS_PORT"
)

exec /opt/oxide/dns-server/bin/dns-server "${args[@]}" &