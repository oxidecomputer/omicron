#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

function usage
{
    echo "usage: $0 [-fRB] HELIOS_PATH PACKAGES_TARBALL"
    echo
    echo "  -f   Force helios build despite git hash mismatch"
    echo "  -R   Build recovery (trampoline) image"
    echo "  -B   Build standard image"
    exit 1
}

function main
{
    while getopts ":hfRBS:" opt; do
        case $opt in
            f)
                FORCE=1
                ;;
            R)
                BUILD_RECOVERY=1
                HELIOS_BUILD_EXTRA_ARGS=-R
                IMAGE_PREFIX=recovery
                ;;
            B)
                BUILD_STANDARD=1
                HELIOS_BUILD_EXTRA_ARGS=-B
                IMAGE_PREFIX=ci
                ;;
            S)
                SWITCH_ZONE=$OPTARG
                ;;
            h | \?)
                usage
                ;;
        esac
    done
    shift $((OPTIND-1))

    # Ensure we got either -R or -B but not both
    case "x$BUILD_RECOVERY$BUILD_STANDARD" in
        x11)
            echo "specify at most one of -R, -B"
            exit 1
            ;;
        x)
            echo "must specify either -R or -B"
            exit 1
            ;;
        *) ;;
    esac

    if [ "$#" != "2" ]; then
        usage
    fi
    HELIOS_PATH=$1
    GLOBAL_ZONE_TARBALL_PATH=$2

    # Read expected helios commit into $COMMIT
    TOOLS_DIR="$(pwd)/$(dirname $0)"
    source "$TOOLS_DIR/helios_version"

    # Grab the opte version
    OPTE_VER=$(cat "$TOOLS_DIR/opte_version")

    # Assemble global zone files in a temporary directory.
    if ! tmp_gz=$(mktemp -d); then
        exit 1
    fi
    trap 'cd /; rm -rf "$tmp_gz"' EXIT

    # Extract the trampoline global zone tarball into a tmp_gz directory
    echo "Extracting gz packages into $tmp_gz"
    ptime -m tar xvzf $GLOBAL_ZONE_TARBALL_PATH -C $tmp_gz

    # If the user specified a switch zone (which is probably named
    # `switch-SOME_VARIANT.tar.gz`), stage it in the right place and rename it
    # to just `switch.tar.gz`.
    if [ "x$SWITCH_ZONE" != "x" ]; then
        mkdir -p "$tmp_gz/root/opt/oxide"
        cp "$SWITCH_ZONE" "$tmp_gz/root/opt/oxide/switch.tar.gz"
    fi

    # Move to the helios checkout
    cd $HELIOS_PATH

    # Unless the user passed -f, check that the helios commit matches the one we
    # have specified in `tools/helios_version`
    if [ "x$FORCE" == "x" ]; then
        CURRENT_COMMIT=$(git rev-parse HEAD)
        if [ "x$COMMIT" != "x$CURRENT_COMMIT" ]; then
            echo "WARNING: omicron/tools/helios_version specifies helios commit"
            echo "  $COMMIT"
            echo "but you have"
            echo "  $CURRENT_COMMIT"
            echo "Either check out the expected commit or pass -f to this"
            echo "script to disable this check."
            exit 1
        fi
    fi

    # Create the "./helios-build" command, which lets us build images
    gmake setup

    # Commands that "./helios-build" would ask us to run (either explicitly
    # or implicitly, to avoid an error).
    rc=0
    pfexec pkg install -q /system/zones/brand/omicron1/tools || rc=$?
    case $rc in
        # `man pkg` notes that exit code 4 means no changes were made because
        # there is nothing to do; that's fine. Any other exit code is an error.
        0 | 4) ;;
        *) exit $rc ;;
    esac

    pfexec zfs create -p rpool/images/$USER

    HELIOS_REPO=https://pkg.oxide.computer/helios-netdev

    # We have to do a bit of extra work to find the host OS hash
    OS_HASH="unknown"

    tmpd=$(mktemp -d)
    if [[ -d "$tmpd" ]]; then
        pkgrecv -s "$HELIOS_REPO" -d "$tmpd" --raw -m latest SUNWcs
        if (($? == 0)); then
            for PKGDIR in "$tmpd/SUNWcs/"*; do
                if [[ -d "$PKGDIR" ]]; then
                    BUILDHASH=$( \
                        grep 'path=etc/versions/build ' \
                        "$PKGDIR/manifest.file" | \
                        awk '{print $2}' \
                    )
                    if [[ -n "$BUILDHASH" && -f "$PKGDIR/$BUILDHASH" ]]; then
                        OS_HASH="$(awk -F- '{print $NF}' "$PKGDIR/$BUILDHASH")"
                        break
                    fi
                fi
            done
        fi
        rm -rf "$tmpd"
    fi

    # Build an image name that includes the omicron and host OS hashes
    IMAGE_NAME="$IMAGE_PREFIX ${GITHUB_SHA:0:7}/${OS_HASH:1:7}"
    IMAGE_NAME+=" $(date +'%Y-%m-%d %H:%M')"

    ./helios-build experiment-image \
        -p helios-netdev="$HELIOS_REPO" \
        -F optever="$OPTE_VER" \
        -P "$tmp_gz/root" \
        -N "$IMAGE_NAME" \
        $HELIOS_BUILD_EXTRA_ARGS
}

main "$@"
