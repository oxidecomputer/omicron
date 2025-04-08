#!/usr/bin/env bash
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

# Saved list of publisher origins we've removed if we need to restore them
REMOVED_PUBLISHERS=()

# The version of OPTE currently installed, if any
INSTALLED_OPTE_VER=""

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
        echo "Ignorning non-file origin publisher ($PUBLISHER: \"$ORIGIN\")"
        return 0
    fi

    # Unset the publisher, but do not do anything with the file.
    echo "Unsetting publisher \"$PUBLISHER\""
    pkg unset-publisher "$PUBLISHER"

    # Add the publisher to the list of removed publishers
    REMOVED_PUBLISHERS+=("$ORIGIN")
}

# Restores publishers removed by remove_publisher
function restore_removed_publishers {
    echo "Restoring previously removed publishers"
    for ORIGIN in "${REMOVED_PUBLISHERS[@]}"; do
        pkg set-publisher -p "$ORIGIN" --search-first
    done
}

# Install stock incorporation from helios-dev, pushing the publisher to the top
function to_stock_helios {
    local CONSOLIDATION="$1"
    echo "Installing stock Helios kernel and networking bits"
    echo "provided by the consolidation package:"
    echo "\"$CONSOLIDATION\""
    echo "Note that this may entail an upgrade, depending on"
    echo "how your Helios system is configured."

    # Some packages which don't exist in stock might've been pulled in and
    # prevent us from installing the stock consolidation package. So, we'll
    # do a dry-run and pick out any packages we need to reject.
    local PKGS_TO_REJECT=($(\
        LANG= LC_CTYPE= LC_NUMERIC= LC_TIME= LC_COLLATE= LC_MONETARY= LC_MESSAGES= LC_ALL=C.UTF-8 \
        pkg install -n --no-refresh -v "$CONSOLIDATION" 2>&1 |\
        grep -o "Package '.*' must be uninstalled or upgraded" |\
        tr -d \' |\
        cut -d ' ' -f 2
    ))

    local REJECT_ARGS=()

    # If we have packages to reject, prompt the user to confirm.
    if [[ ${#PKGS_TO_REJECT[@]} -gt 0 ]]; then
        echo "These packages will be removed: ${PKGS_TO_REJECT[*]}"
        read -p "Confirm (Y/n): " RESPONSE
        case $(echo $RESPONSE | tr '[A-Z]' '[a-z]') in
            n|no )
                echo "Packages will NOT be removed"

                # Undo the publisher removals
                restore_removed_publishers
                # Reinstall the packages we removed
                restore_xde_and_opte

                exit 1

                ;;
            * )
                echo "Removing packages"
                ;;
        esac

        for PKG in "${PKGS_TO_REJECT[@]}"; do
            REJECT_ARGS+=("--reject $PKG")
        done
    fi

    pkg set-publisher --sticky --search-first "$HELIOS_PUBLISHER"

    # Now install stock consolidation but reject the packages we don't want.
    pkg install --no-refresh -v "${REJECT_ARGS[@]}" "$CONSOLIDATION"
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
    echo "$CONSOLIDATION" | tr -s ' ' | awk '{ print $1 "@" $(NF-1); }'
}

# Actually uninstall the opteadm tool and xde driver
function uninstall_xde_and_opte {
    local RC=0

    pkg info -lq driver/network/opte || RC=$?
    if [[ "$RC" -eq 0 ]]; then
        # Save currently installed version
        INSTALLED_OPTE_VER="$(pkg info -l driver/network/opte | grep FMRI | awk '{ print $NF; }')"
    fi

    pkg uninstall -v --ignore-missing pkg://helios-netdev/driver/network/opte || RC=$?
    if [[ $RC -ne 0 ]] && [[ $RC -ne 4 ]]; then
        exit $RC
    fi
}

function restore_xde_and_opte {
    if [[ -n "$INSTALLED_OPTE_VER" ]]; then
        echo "Restoring previously installed opteadm and xde driver"
        pkg install --no-refresh -v "$INSTALLED_OPTE_VER"
    fi
}

function unfreeze_opte_pkg {
    OMICRON_FROZEN_PKG_COMMENT="OMICRON-PINNED-PACKAGE"

    # If we've frozen a particular version, let's be good citizens
    # and clear that as well.
    if PKG_FROZEN=$(pkg freeze | grep driver/network/opte); then
        FROZEN_COMMENT=$(echo "$PKG_FROZEN" | awk '{ print $(NF) }')
        if [ "$FROZEN_COMMENT" == "$OMICRON_FROZEN_PKG_COMMENT" ]; then
            pkg unfreeze driver/network/opte
        fi
    fi
}

function ensure_not_already_on_helios {
    local RC=0
    pkg list "$STOCK_CONSOLIDATION"* || RC=$?
    if [[ $RC -eq 0 ]]; then
        echo "This system appears to already be running stock Helios"
        exit 0
    fi
}

CONSOLIDATION="$(ensure_helios_publisher_exists)"
uninstall_xde_and_opte
for PUBLISHER in "${PUBLISHERS[@]}"; do
    remove_publisher "$PUBLISHER"
done
unfreeze_opte_pkg
ensure_not_already_on_helios
to_stock_helios "$CONSOLIDATION"
