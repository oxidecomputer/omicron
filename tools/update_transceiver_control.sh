#!/usr/bin/env bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

function usage {
    echo "usage: $0 [-c COMMIT] [-n]"
    echo
    echo "  -b COMMIT   Ask to update transceiver_control to HEAD on the named branch."
    echo "  -c COMMIT   Ask to update transceiver_control to a specific commit."
    echo "              If this is unset, Github is queried."
    echo "  -n          Dry-run"
    exit 1
}

REPO="oxidecomputer/transceiver-control"

. "$SOURCE_DIR/update_helpers.sh"

function update_transceiver_control {
    TARGET_COMMIT="$1"
    DRY_RUN="$2"
    SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "xcvradm.gz" "bins")
    OUTPUT=$(printf "COMMIT=\"%s\"\nCIDL_SHA256_ILLUMOS=\"%s\"\n" \
        "$TARGET_COMMIT" "$SHA")

    if [ -n "$DRY_RUN" ]; then
        OPENAPI_PATH="/dev/null"
    else
        OPENAPI_PATH="$SOURCE_DIR/transceiver_control_version"
    fi
    echo "Updating transceiver control from: $TARGET_COMMIT"
    set -x
    echo "$OUTPUT" > "$OPENAPI_PATH"
    set +x
}

function main {
    TARGET_COMMIT=""
    DRY_RUN=""
    while getopts "c:n" o; do
      case "${o}" in
        c)
          TARGET_COMMIT="$OPTARG"
          ;;
        n)
          DRY_RUN="yes"
          ;;
        *)
          usage
          ;;
      esac
    done

    if [[ -z "$TARGET_COMMIT" ]]; then
	    TARGET_COMMIT=$(get_latest_commit_from_gh "$REPO" "$TARGET_BRANCH")
    fi
    update_transceiver_control "$TARGET_COMMIT" "$DRY_RUN"
}

main "$@"
