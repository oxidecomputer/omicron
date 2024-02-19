#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

HTTP_ADDR="$(svcprop -c -p config/http_address "${SMF_FMRI}")"
DNS_ADDR="$(svcprop -c -p config/dns_addr "${SMF_FMRI}")"

args=(
  "--config-file" "/var/svc/manifest/site/external_dns/config.toml"
  "--http-address" "$HTTP_ADDR"
  "--dns-address" "$DNS_ADDR"
)

exec /opt/oxide/dns-server/bin/dns-server "${args[@]}" &