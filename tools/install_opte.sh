#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

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

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."
OMICRON_TOP="$PWD"

# The xde driver no longer requires separate kernel bits as of API version 21
# see https://github.com/oxidecomputer/opte/pull/321. Check if an older version
# of the driver is installed and prompt the user to remove it first.
RC=0
pkg info -lq driver/network/opte || RC=$?
if [[ "$RC" -eq 0 ]]; then
    # Grab the minor version of the package which corresponds to the API version
    # and prompt the user to run the uninstall script first if the version is < 21
    OPTE_VERSION="$(pkg info -l driver/network/opte | grep Version | tr -s ' ' | cut -d ' ' -f 3 | cut -d '.' -f 2)"
    if [[ "$OPTE_VERSION" -lt 21 ]]; then
        echo "The xde driver no longer requires custom kernel bits."
        echo "Please run \`tools/uninstall_opte.sh\` first to remove the old xde driver and associated kernel bits."
        exit 1
    fi
fi

# While separate kernel bits are no longer required, we still need to make sure that the
# required APIs are available i.e., a build including https://www.illumos.org/issues/15342
# Just checking for the presence of the mac_getinfo(9f) man page is a good enough proxy for this.
RC=0
man -l mac_getinfo || RC=$?
if [[ "$RC" -ne 0 ]]; then
    echo "xde driver requires updated kernel bits."
    echo "Please run \`pkg update\` first."
    exit 1
fi

# Grab the version of the opte package to install
OPTE_VERSION="$(cat "$OMICRON_TOP/tools/opte_version")"

# Source the OPTE override (if any). When OPTE_COMMIT is set, the desired
# OPTE version isn't published to the helios pkg repo yet, so we install
# directly from the override p5p built by OPTE CI on buildomat (see the
# branch on $OPTE_COMMIT below).
source "$OMICRON_TOP/tools/opte_version_override"

OMICRON_FROZEN_PKG_COMMENT="OMICRON-PINNED-PACKAGE"

# Once we install, we mark the package as frozen at that particular version.
# This makes sure that a `pkg update` won't automatically move us forward
# (and hence defeat the whole point of pinning).
# But this also prevents us from installing the next version, so we must
# unfreeze before a pkg install. The unfreeze is deferred until an install
# is actually about to happen: every read-only validation (archive, digest,
# package identity, payload) runs first, so a rejected input leaves the
# existing installation pinned exactly as it was.
OPTE_FROZEN_BY_US=0
if PKG_FROZEN=$(pkg freeze | grep driver/network/opte); then
    FROZEN_COMMENT=$(echo "$PKG_FROZEN" | awk '{ print $(NF) }')

    # Compare the comment to make sure this is indeed our previous doing
    if [ "$FROZEN_COMMENT" != "$OMICRON_FROZEN_PKG_COMMENT" ]; then
        echo "Found driver/network/opte previously frozen but not by us:"
        echo $PKG_FROZEN
        exit 1
    fi
    OPTE_FROZEN_BY_US=1
fi

unfreeze_opte_if_pinned() {
    if [[ "$OPTE_FROZEN_BY_US" -eq 1 ]]; then
        pfexec pkg unfreeze driver/network/opte
        OPTE_FROZEN_BY_US=0
    fi
}

if [[ -n "${OPTE_P5P:-}" ]]; then
    # Install exactly the package contained in an explicitly supplied local
    # p5p archive (a developer build staged next to a recorded sha256).
    #
    # Adding the archive as a package source is not a selection: with
    # `pkg install -g <archive> driver/network/opte` the solver picks the
    # newest version visible from ANY source (the helios origin can publish
    # a newer opte than a local build), and it reports "nothing to do"
    # (exit 4) when an equal *version* is already installed even though the
    # payload differs -- successive local builds of the same commit share a
    # version (0.<api>.<commit count>) and differ only in their publish
    # timestamp. So: take the exact FMRI (version + timestamp) from the
    # archive, install precisely that (pkg install with an exact FMRI moves
    # to it whether the installed one is older, newer, or a same-version
    # different-timestamp build; pkg update refuses downgrades), and then
    # compare the installed payload hashes with the archive's manifest.
    # An explicitly selected local archive never falls back to a buildomat
    # download or the pkg repo.
    P5P_PATH="$OPTE_P5P"
    if [[ ! -s "$P5P_PATH" ]]; then
        echo "OPTE_P5P is set but $P5P_PATH is missing or empty" >&2
        exit 1
    fi
    if [[ ! -s "$P5P_PATH.sha256" ]]; then
        echo "OPTE_P5P is set but its recorded digest $P5P_PATH.sha256 is missing" >&2
        exit 1
    fi
    WANT_SHA=$(awk '{print $1}' "$P5P_PATH.sha256")
    if command -v sha256sum > /dev/null; then
        GOT_SHA=$(sha256sum "$P5P_PATH" | awk '{print $1}')
    else
        GOT_SHA=$(digest -a sha256 "$P5P_PATH")
    fi
    if [[ "$GOT_SHA" != "$WANT_SHA" ]]; then
        echo "OPTE_P5P digest mismatch for $P5P_PATH:" >&2
        echo "  recorded $WANT_SHA" >&2
        echo "  found    $GOT_SHA" >&2
        echo "restage the archive and its digest together" >&2
        exit 1
    fi

    # The exact package identity contained in the archive.
    ARCHIVE_FMRIS=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && ARCHIVE_FMRIS+=("$line")
    done < <(pkgrepo list -s "$P5P_PATH" -H -F tsv driver/network/opte | awk -F'\t' '{print $NF}')
    if [[ "${#ARCHIVE_FMRIS[@]}" -ne 1 ]]; then
        echo "expected exactly one driver/network/opte package in $P5P_PATH, found ${#ARCHIVE_FMRIS[@]}: ${ARCHIVE_FMRIS[*]:-none}" >&2
        exit 1
    fi
    WANT_FMRI="${ARCHIVE_FMRIS[0]}"
    # `pkg list -v` prints FMRIs without the ",<build release>" component
    # (0.42.508-3.0:TS rather than 0.42.508,5.11-3.0:TS); compare in that form.
    WANT_NORM=$(sed -e 's/,[0-9.]*-/-/' <<< "$WANT_FMRI")
    installed_opte_fmri() {
        pkg list -Hv driver/network/opte 2>/dev/null | awk '{print $1}'
    }
    HAVE_NORM=$(installed_opte_fmri)
    echo "Selected local p5p $P5P_PATH (sha256 $GOT_SHA) contains $WANT_FMRI"
    if [[ "$HAVE_NORM" == "$WANT_NORM" ]]; then
        echo "The archive's exact package is already installed; verifying its payload on disk"
    else
        if [[ -n "$HAVE_NORM" ]]; then
            echo "Replacing installed $HAVE_NORM with the archive's $WANT_NORM"
        else
            echo "Installing $WANT_NORM from the local p5p"
        fi
        unfreeze_opte_if_pinned
        RC=0
        pfexec pkg install -g "$P5P_PATH" "$WANT_FMRI" || RC=$?
        if [[ "$RC" -eq 4 ]]; then
            echo "pkg reports nothing to do, but the installed package (${HAVE_NORM:-none}) is not the archive's ($WANT_NORM)" >&2
            exit 1
        elif [[ "$RC" -ne 0 ]]; then
            echo "Installing xde driver from local p5p failed (pkg exit $RC)" >&2
            exit "$RC"
        fi
        HAVE_NORM=$(installed_opte_fmri)
        if [[ "$HAVE_NORM" != "$WANT_NORM" ]]; then
            echo "after install the installed package is '${HAVE_NORM:-none}', expected $WANT_NORM" >&2
            exit 1
        fi
    fi

    # Verify the installed payload against the ARCHIVE manifest (not merely
    # the installed manifest): every non-preserved file the archive delivers
    # must be on disk with the archive's content hash (pkg's `hash` is the
    # SHA-1 of the uncompressed file). `preserve=` config files such as
    # xde.conf may legitimately differ and are skipped.
    IMAGE_ROOT="${PKG_IMAGE:-/}"
    VERIFIED=0
    while IFS=$'\t' read -r fpath fhash fpreserve; do
        [[ -n "$fpath" ]] || continue
        [[ -z "$fpreserve" ]] || continue
        target="${IMAGE_ROOT%/}/$fpath"
        if [[ ! -f "$target" ]]; then
            echo "archive file $fpath is not installed at $target" >&2
            exit 1
        fi
        got=$(digest -a sha1 "$target")
        if [[ "$got" != "$fhash" ]]; then
            echo "installed $fpath (sha1 $got) does not match the archive's payload ($fhash)" >&2
            exit 1
        fi
        VERIFIED=$((VERIFIED + 1))
    done < <(pkg contents -g "$P5P_PATH" -H -t file -o path,hash,preserve "$WANT_FMRI")
    if [[ "$VERIFIED" -eq 0 ]]; then
        echo "no file payloads found in $WANT_FMRI to verify" >&2
        exit 1
    fi
    echo "xde driver from local p5p installed and verified: $WANT_NORM ($VERIFIED files match the archive)"

    # Installed files are not the running driver. If xde is already loaded,
    # the in-memory module stays whatever was loaded earlier until it is
    # reloaded (reboot, or rem_drv/add_drv while no xde devices exist).
    if [[ "$IMAGE_ROOT" == "/" ]] && modinfo 2>/dev/null | awk '{print $6}' | grep -qx xde; then
        echo "NOTE: the xde kernel module is currently loaded; the installed driver takes effect only after it is reloaded (reboot, or 'rem_drv xde && add_drv xde' when no xde devices exist)."
    fi
elif [[ "x$OPTE_COMMIT" != "x" ]]; then
    # Install from the override p5p archive built by OPTE CI. The p5p
    # contains exactly one version (built from $OPTE_COMMIT), which
    # generally won't match the canonical $OPTE_VERSION, so we let pkg
    # pick the version from the p5p rather than pinning here.
    P5P_URL="https://buildomat.eng.oxide.computer/public/file/oxidecomputer/opte/repo/$OPTE_COMMIT/opte.p5p"
    P5P_PATH="/tmp/opte-override.p5p"
    echo "Downloading override p5p from $P5P_URL"
    curl -fL -o "$P5P_PATH" "$P5P_URL"

    unfreeze_opte_if_pinned
    RC=0
    # The OPTE CI p5p publishes under the "helios" publisher.
    pfexec pkg install -g "$P5P_PATH" pkg://helios/driver/network/opte || RC=$?
    if [[ "$RC" -eq 0 ]]; then
        echo "xde driver installed from override p5p"
    elif [[ "$RC" -eq 4 ]]; then
        echo "Correct xde driver already installed"
    else
        echo "Installing xde driver from override p5p failed"
        exit "$RC"
    fi
    rm -f "$P5P_PATH"
else
    # Install the published version from the helios pkg repo.
    unfreeze_opte_if_pinned
    RC=0
    pfexec pkg install -v pkg://helios/driver/network/opte@"$OPTE_VERSION" || RC=$?
    if [[ "$RC" -eq 0 ]]; then
        echo "xde driver installed successfully"
    elif [[ "$RC" -eq 4 ]]; then
        echo "Correct xde driver already installed"
    else
        echo "Installing xde driver failed"
        exit "$RC"
    fi
fi

# Discover the actually-installed version (may differ from $OPTE_VERSION
# when an override is active) and freeze at that. (Re)freezing a package
# that is still frozen at the same version succeeds and keeps the pin.
INSTALLED_VERSION=$(pkg info -l driver/network/opte | awk '/^[[:space:]]*Version:/ {print $2}')
RC=0
pfexec pkg freeze -c "$OMICRON_FROZEN_PKG_COMMENT" "driver/network/opte@$INSTALLED_VERSION" || RC=$?
if [[ "$RC" -ne 0 ]]; then
    echo "Failed to pin opte package to $INSTALLED_VERSION"
    exit $RC
fi

# Check the user's path
RC=0
which opteadm > /dev/null || RC=$?
if [[ "$RC" -ne 0 ]]; then
    echo "The \`opteadm\` administration tool is not on your path."
    echo "You may add \"/opt/oxide/opte/bin\" to your path to access it."
fi
