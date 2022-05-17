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

if [[ $(id -u) -ne 0 ]]; then
    echo "This must be run as root"
    exit 1
fi

# Remove these publishers if they exist
PUBLISHERS=("on-nightly" "helios-netdev")

# The stock Helios publisher and incorporation base information to revert to
HELIOS_PUBLISHER="helios-dev"
STOCK_CONSOLIDATION="pkg://$HELIOS_PUBLISHER/consolidation/osnet/osnet-incorporation"

# Remove a publisher in $1, assuming it matches a few basic assumptions
function remove_publisher {
    local PUBLISHER="$1"
    local LINE="$(pkg publisher | grep "$PUBLISHER" | tr -s ' ')"
    if [[ -z "$LINE" ]]; then
        echo "Publisher \"$PUBLISHER\" does not exist, ignoring"
        return 0
    fi
    local ORIGIN="$(echo "$LINE" | cut -d ' ' -f 5)"
    if [[ -z "$ORIGIN" ]]; then
        echo "Cannot determine origin URI for publisher \"$PUBLISHER\""
        exit 1
    fi
    if [[ "${ORIGIN:0:7}" != "file://" ]]; then
        echo "Expected a file origin URI for publisher \"$PUBLISHER\""
        exit 1
    fi

    # Unset the publisher, but do not do anything with the file.
    echo "Unsetting publisher \"$PUBLISHER\""
    pkg unset-publisher "$PUBLISHER"
}

# Install stock incorporation from helios-dev, pushing the publisher to the top
function to_stock_helios {
    local CONSOLIDATION="$1"
    echo "Installing stock Helios kernel and networking bits"
    echo "provided by the consolidation package:"
    echo "\"$CONSOLIDATION\""
    echo "Note that this may entail an upgrade, depending on"
    echo "how your Helios system is configured."
    pkg set-publisher --sticky --search-first "$HELIOS_PUBLISHER"
    pkg install --no-refresh -v "$CONSOLIDATION"
}

# If helios-dev exists, echo the full osnet-incorporation package we'll be
# installing to. If it does _not_ exist, fail.
function ensure_helios_publisher_exists {
    pkg publisher "$HELIOS_PUBLISHER" > /dev/null || \
        echo "No \"$HELIOS_PUBLISHER\" publisher exists on this system!"
    local CONSOLIDATION="$(pkg list --no-refresh -H -af "$STOCK_CONSOLIDATION"@latest || echo "")"
    if [[ -z "$CONSOLIDATION" ]]; then
        echo "No osnet-incorporation package exists on this system,"
        echo "so we cannot determine the exact package to install"
        echo "to revert to stock Helios. You may need to update your"
        echo "helios-dev publisher to refresh that publishers list of"
        echo "available packages, with \"pkg refresh helios-dev\""
        exit 1
    fi
    echo "$CONSOLIDATION" | tr -s ' ' | cut -d ' ' -f 1,3 | tr ' ' '@'
}

# Actually uninstall the opteadm tool and xde driver
function uninstall_xde_and_opte {
    local RC=0
    pkg uninstall -v --ignore-missing pkg://helios-netdev/driver/network/opte || RC=$?
    if [[ $RC -ne 0 ]] && [[ $RC -ne 4 ]]; then
        exit $RC
    fi
}

function ensure_not_already_on_helios {
    local RC=0
    pkg list "$STOCK_CONSOLIDATION"* || RC=$?
    if [[ $RC -eq 0 ]]; then
        echo "This system appears to already be running stock Helios"
        exit 1
    fi
}

CONSOLIDATION="$(ensure_helios_publisher_exists)"
ensure_not_already_on_helios
uninstall_xde_and_opte
for PUBLISHER in "${PUBLISHERS[@]}"; do
    remove_publisher "$PUBLISHER"
done
to_stock_helios "$CONSOLIDATION"
