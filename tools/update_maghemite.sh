#!/usr/bin/env bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

function usage {
    echo "usage: $0 [-c COMMIT] [-n]"
    echo
    echo "  -b COMMIT   Ask to update Maghemite to HEAD on the named branch."
    echo "  -c COMMIT   Ask to update Maghemite to a specific commit."
    echo "              If this is unset, Github is queried."
    echo "  -n          Dry-run"
    exit 1
}

PACKAGES=(
  "mg-ddm-gz"
  "mg-ddm"
  "mgd"
)
REPO="oxidecomputer/maghemite"
. "$SOURCE_DIR/update_helpers.sh"

function update_openapi {
    TARGET_COMMIT="$1"
    DRY_RUN="$2"
    DAEMON="$3"
    SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "${DAEMON}-admin.json" "openapi")
    OUTPUT=$(printf "COMMIT=\"%s\"\nSHA2=\"%s\"\n" "$TARGET_COMMIT" "$SHA")

    if [ -n "$DRY_RUN" ]; then
        OPENAPI_PATH="/dev/null"
    else
        OPENAPI_PATH="$SOURCE_DIR/maghemite_${DAEMON}_openapi_version"
    fi
    echo "Updating Maghemite OpenAPI from: $TARGET_COMMIT"
    set -x
    echo "$OUTPUT" > "$OPENAPI_PATH"
    set +x
}

function update_mgd {
    TARGET_COMMIT="$1"
    DRY_RUN="$2"
    DAEMON="$3"
    SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "mgd" "image")
    OUTPUT=$(printf "CIDL_SHA256=\"%s\"\n" "$SHA")

    SHA_LINUX=$(get_sha "$REPO" "$TARGET_COMMIT" "mgd" "linux")
    OUTPUT_LINUX=$(printf "MGD_LINUX_SHA256=\"%s\"\n" "$SHA_LINUX")

    if [ -n "$DRY_RUN" ]; then
        MGD_PATH="/dev/null"
    else
        MGD_PATH="$SOURCE_DIR/maghemite_mgd_checksums"
    fi
    echo "Updating Maghemite mgd from: $TARGET_COMMIT"
    set -x
    printf "$OUTPUT\n$OUTPUT_LINUX" > $MGD_PATH
    set +x
}

function main {
    TARGET_COMMIT=""
    DRY_RUN=""
    while getopts "b:c:n" o; do
      case "${o}" in
        b)
          TARGET_BRANCH="$OPTARG"
          ;;
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
    install_toml2json
    update_mgd "$TARGET_COMMIT" "$DRY_RUN"
    update_openapi "$TARGET_COMMIT" "$DRY_RUN" ddm
    update_openapi "$TARGET_COMMIT" "$DRY_RUN" mg
    do_update_packages "$TARGET_COMMIT" "$DRY_RUN" "$REPO" "${PACKAGES[@]}"
}

main "$@"
