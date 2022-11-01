#!/bin/bash
#
# Small tool to _uninstall_ OPTE and the xde kernel driver and ONU bits.
#
# This should bring the system back to the stock Helios bits.

set -e
set -u
set -x

MARKER=/etc/opt/oxide/NO_INSTALL
if [[ -f "$MARKER" ]]; then
    echo "This system has the marker file $MARKER, aborting." >&2
    exit 1
fi

if [[ "$(uname)" != "SunOS" ]]; then
    echo "This script is intended for Helios only"
    exit 1
fi

# Generate a random hex string of the provided number of octets.
function random_string {
    local LENGTH="${1:-4}"
    xxd -l "$LENGTH" -c "$LENGTH" -p < /dev/random
}

# Create a boot environment in which to perform the operations.
BE_NAME="helios-$(random_string 4)"
BE_MOUNT_POINT="/tmp/$BE_NAME"

# Remove the BE if this script fails.
function remove_helios_be() {
    pfexec beadm destroy -sfF "$BE_NAME"
}
trap remove_helios_be ERR

pfexec beadm create "$BE_NAME"
pfexec beadm mount "$BE_NAME" "$BE_MOUNT_POINT"

# Remove the `helios-netdev` publisher, if needed.
pfexec pkg -R "$BE_MOUNT_POINT" unset-publisher helios-netdev \
        && echo "helios-netdev publisher has been removed" \
        || echo "helios-netdev publisher already removed"
pfexec pkg -R "$BE_MOUNT_POINT" set-publisher --sticky --search-first helios-dev

# Uninstall OPTE if needed.
pfexec pkg -R "$BE_MOUNT_POINT" uninstall -v driver/network/opte \
        && echo "Uninstalled xde driver" \
        || echo "xde driver already uninstalled"

# Explicitly reinstall the system to the `helios-dev` bits if needed.
#
# Note that we manually reject a few packages, as the `entire` consolidation
# depends on them in the `helios-netdev` publisher. That would either require
# two operations, or leave the system in a broken state with an `entire` that
# had now-removed dependents. `--reject` causes them to be explicitly
# uninstalled with a single operation.
RC=0
MAYBE_REJECT="/system/library/libt6mfg /system/library/libispi"
REJECTABLES="$(pkg -R $BE_MOUNT_POINT list -H $MAYBE_REJECT | \
        awk 'BEGIN { ORS=" " } { print("--reject " $1) }')"
pfexec pkg -R "$BE_MOUNT_POINT" install -v \
        $REJECTABLES \
        pkg://helios-dev/consolidation/osnet/osnet-incorporation@latest || RC=$?
if [[ $RC -eq 0 ]]; then
    pfexec beadm activate "$BE_NAME"
    echo "Installed / updated the system to stock helios-dev bits."
    echo "Please reboot for the changes to take effect."
elif [[ $RC -eq 4 ]]; then
    remove_helios_be
    echo "Your system appears up-to-date!"
else
    echo "Unknown failure uninstalling OPTE"
    echo "Return code from \`pkg update\` is: $RC"
    echo "Temporary boot environment $BE_NAME is still around."
    echo "You may wish to destroy it with \`beadm destroy\`."
    exit $RC
fi
