#!/bin/bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ARG0="$(basename "${BASH_SOURCE[0]}")"

function usage {
    echo "usage: $0 [-c COMMIT] [-n]"
    echo
    echo "  -c COMMIT   Ask to update Maghemite to a specific commit."
    echo "              If this is unset, Github is queried."
    echo "  -n          Dry-run"
    exit 1
}

PACKAGES=(
  "maghemite"
  "mg-ddm"
)

REPO="oxidecomputer/maghemite"

. "$SOURCE_DIR/update_helpers.sh"

function update_openapi {
    TARGET_COMMIT="$1"
    DRY_RUN="$2"
    SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "ddm-admin.json" "openapi")
    OUTPUT=$(printf "COMMIT=\"%s\"\nSHA2=\"%s\"\n" "$TARGET_COMMIT" "$SHA")

    if [ -n "$DRY_RUN" ]; then
        OPENAPI_PATH="/dev/null"
    else
        OPENAPI_PATH="$SOURCE_DIR/maghemite_openapi_version"
    fi
    echo "Updating Maghemite OpenAPI from: $TARGET_COMMIT"
    set -x
    echo "$OUTPUT" > $OPENAPI_PATH
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

    TARGET_COMMIT=$(get_latest_commit_from_gh "$REPO" "$TARGET_COMMIT")
    install_toml2json
    do_update_packages "$TARGET_COMMIT" "$DRY_RUN" "$REPO" "${PACKAGES[@]}"
    update_openapi "$TARGET_COMMIT" "$DRY_RUN"
    do_update_packages "$TARGET_COMMIT" "$DRY_RUN" "$REPO" "${PACKAGES[@]}"
}

main "$@"
