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

    TOOLS_DIR="$(pwd)/$(dirname "$0")"

    # Grab the opte version
    OPTE_VER=$(cat "$TOOLS_DIR/opte_version")

    # Assemble global zone files in a temporary directory.
    if ! tmp_gz=$(mktemp -d); then
        exit 1
    fi
    trap 'cd /; rm -rf "$tmp_gz"' EXIT

    # Extract the global zone tarball into a tmp_gz directory
    echo "Extracting gz packages into $tmp_gz"
    ptime -m tar xvzf "$GLOBAL_ZONE_TARBALL_PATH" -C "$tmp_gz"

    # If the user specified a switch zone (which is probably named
    # `switch-SOME_VARIANT.tar.gz`), stage it in the right place and rename it
    # to just `switch.tar.gz`.
    if [ "x$SWITCH_ZONE" != "x" ]; then
        mkdir -p "$tmp_gz/root/opt/oxide"
        cp "$SWITCH_ZONE" "$tmp_gz/root/opt/oxide/switch.tar.gz"
    fi

    if [ "x$BUILD_STANDARD" != "x" ]; then
        mkdir -p "$tmp_gz/root/root"
        echo "# Add opteadm, ddmadm to PATH" >> "$tmp_gz/root/root/.profile"
        echo 'export PATH=$PATH:/opt/oxide/opte/bin:/opt/oxide/mg-ddm' >> "$tmp_gz/root/root/.profile"
    fi

    # Move to the helios checkout
    cd "$HELIOS_PATH"

    HELIOS_REPO=https://pkg.oxide.computer/helios/2/dev/

    # Build an image name that includes the omicron and host OS hashes
    IMAGE_NAME="$IMAGE_PREFIX ${GITHUB_SHA:0:7}"
    # The ${os_short_commit} token will be expanded by `helios-build`
    IMAGE_NAME+='/${os_short_commit}'
    IMAGE_NAME+=" $(date +'%Y-%m-%d %H:%M')"

    ./helios-build experiment-image \
        -p helios-dev="$HELIOS_REPO" \
        -F optever="$OPTE_VER" \
        -P "$tmp_gz/root" \
        -N "$IMAGE_NAME" \
        $HELIOS_BUILD_EXTRA_ARGS
}

main "$@"
