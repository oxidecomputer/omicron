#!/bin/bash

set -o errexit
set -o pipefail

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
    while getopts ":hfRB" opt; do
        case $opt in
            f)
                FORCE=1
                shift
                ;;
            R)
                BUILD_RECOVERY=1
                HELIOS_BUILD_EXTRA_ARGS=-R
                shift
                ;;
            B)
                BUILD_STANDARD=1
                HELIOS_BUILD_EXTRA_ARGS=-B
                shift
                ;;
            h | \?)
                usage
                ;;
        esac
    done

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

    # Read expected helios commit into $COMMIT
    TOOLS_DIR="$(pwd)/$(dirname $0)"
    source "$TOOLS_DIR/helios_version"

    # Convert args to absolute paths
    case $1 in
        /*) HELIOS_PATH=$1 ;;
        *) HELIOS_PATH=$(pwd)/$1 ;;
    esac
    case $2 in
        /*) TRAMPOLINE_PATH=$2 ;;
        *) TRAMPOLINE_PATH=$(pwd)/$2 ;;
    esac

    # Extract the trampoline global zone tarball into a tmp_gz directory
    if ! tmp_gz=$(mktemp -d); then
        exit 1
    fi
    trap 'cd /; rm -rf "$tmp_gz"' EXIT

    echo "Extracting trampoline gz packages into $tmp_gz"
    ptime -m tar xvzf $TRAMPOLINE_PATH -C $tmp_gz

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

    ./helios-build experiment-image \
        -p helios-netdev=https://pkg.oxide.computer/helios-netdev \
        -F optever=0.21 \
        -P $tmp_gz/root \
        $HELIOS_BUILD_EXTRA_ARGS
}

main "$@"
