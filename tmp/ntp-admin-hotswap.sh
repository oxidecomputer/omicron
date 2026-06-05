#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

usage() {
    echo "usage: $0 [-a G0_ADDR]" >&2
    echo "  G0_ADDR may also be supplied via the G0_ADDR env var." >&2
    exit 2
}

while getopts ":a:h" opt; do
    case "$opt" in
        a) G0_ADDR="$OPTARG" ;;
        h|*) usage ;;
    esac
done

: "${G0_ADDR:?set G0_ADDR via -a <addr> or the G0_ADDR env var}"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

cargo build -p omicron-ntp-admin --bin ntp-admin

scp "${SSH_OPTS[@]}" ./target/debug/ntp-admin "root@${G0_ADDR}:/tmp/ntp-admin.new"

ssh "${SSH_OPTS[@]}" "root@${G0_ADDR}" 'bash -s' <<'REMOTE'
set -o errexit
set -o pipefail
set -o xtrace

read -r zone zpath < <(pfexec zoneadm list -p | awk -F: '/:oxz_ntp/ { print $2, $4; exit }')
if [[ -z "$zone" || -z "$zpath" ]]; then
    echo "no NTP zone found on this sled" >&2
    exit 1
fi

dest="${zpath}/root/opt/oxide/ntp-admin/bin/ntp-admin"
pfexec cp /tmp/ntp-admin.new "$dest"
pfexec chmod +x "$dest"

pfexec zlogin "$zone" svcadm disable -s svc:/oxide/ntp-admin:default
REMOTE
