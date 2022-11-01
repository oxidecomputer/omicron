#!/bin/bash
#
# Small tool to install OPTE and the xde kernel driver and ONU bits.

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

# Add the `helios-netdev` publisher, which contains both the `xde` driver and
# `opteadm` CLI tool, as well as the kernel bits required for the driver and
# other Oxide networking components to operate.
pfexec pkg \
    -R "$BE_MOUNT_POINT" \
    set-publisher \
    --search-first \
    -O \
    https://pkg.oxide.computer/helios-netdev/ \
    helios-netdev

# Make the stock `helios-dev` publisher non-sticky, to allow packages it
# provides to be updated to those provided by `helios-netdev.
pfexec pkg -R "$BE_MOUNT_POINT" set-publisher --non-sticky helios-dev

# Actually install the xde kernel module and opteadm tool.
RC=0
pfexec pkg -R "$BE_MOUNT_POINT" install -v driver/network/opte@latest || RC=$?
if [[ "$RC" -ne 0 ]] && [[ "$RC" -ne 4 ]]; then
    echo "Unknown failure installing OPTE"
    echo "Return code from \`pkg install\` is: $?"
    echo "Temporary boot environment $BE_NAME is still around."
    echo "You may wish to destroy it with \`beadm destroy\`."
    exit 1
    exit "$RC"
fi

# Notify the user about placing `opteadm` on their path.
if [[ $? -eq 0 ]] || [[ $? -eq 4 ]]; then
    if [[ -z "$(which opteadm 2> /dev/null)" ]]; then
        echo "The \`opteadm\` administration tool has been installed."
        echo "You may wish to add \"/opt/oxide/opte/bin\" to your path to access it."
    fi
fi

# Install the kernel bits as well, if needed.
RC=0
pfexec pkg -R "$BE_MOUNT_POINT" update -v || RC=$?

# If all the above worked, and updates are in fact required, activate the BE and
# request a reboot into it.
if [[ $RC -eq 0 ]]; then 
    pfexec beadm activate "$BE_NAME"
    echo "Installed / updated OPTE and kernel bits."
    echo "Please reboot for the changes to take effect."
elif [[ $RC -eq 4 ]]; then
    remove_helios_be
    echo "Your system appears up-to-date!"
else
    echo "Unknown failure installing OPTE"
    echo "Return code from \`pkg update\` is: $RC"
    echo "Temporary boot environment $BE_NAME is still around."
    echo "You may wish to destroy it with \`beadm destroy\`."
    exit $RC
fi
