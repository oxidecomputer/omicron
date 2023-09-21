#!/bin/bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

function usage {
    echo "usage: $0 [-c COMMIT] [-n]"
    echo
    echo "  -c COMMIT   Ask to update Dendrite to a specific commit."
    echo "              If this is unset, Github is queried."
    echo "  -n          Dry-run"
    exit 1
}

PACKAGES=(
  "dendrite-asic"
  "dendrite-softnpu"
  "dendrite-stub"
)

REPO="oxidecomputer/dendrite"

. "$SOURCE_DIR/update_helpers.sh"

function update_openapi {
    TARGET_COMMIT="$1"
    DRY_RUN="$2"
    SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "dpd.json" "openapi")
    OUTPUT=$(printf "COMMIT=\"%s\"\nSHA2=\"%s\"\n" "$TARGET_COMMIT" "$SHA")

    if [ -n "$DRY_RUN" ]; then
        OPENAPI_PATH="/dev/null"
    else
        OPENAPI_PATH="$SOURCE_DIR/dendrite_openapi_version"
    fi
    echo "Updating Dendrite OpenAPI from: $TARGET_COMMIT"
    set -x
    echo "$OUTPUT" > "$OPENAPI_PATH"
    set +x
}

function update_dendrite_stub_shas {
    TARGET_COMMIT="$1"
    DRY_RUN="$2"
    ILLUMOS_SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "dendrite-stub" "image")
    LINUX_DPD_SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "dpd" "linux-bin")
    LINUX_SWADM_SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "swadm" "linux-bin")
    OUTPUT=$(printf \
        "CIDL_SHA256_ILLUMOS=\"%s\"\nCIDL_SHA256_LINUX_DPD=\"%s\"\nCIDL_SHA256_LINUX_SWADM=\"%s\"\n" \
        "$ILLUMOS_SHA" \
        "$LINUX_DPD_SHA" \
        "$LINUX_SWADM_SHA" \
    )
    if [ -n "$DRY_RUN" ]; then
        STUB_CHECKSUM_PATH="/dev/null"
    else
        STUB_CHECKSUM_PATH="$SOURCE_DIR/dendrite_stub_checksums"
    fi
    echo "Updating Dendrite stub checksums from: $TARGET_COMMIT"
    set -x
    echo "$OUTPUT" > "$STUB_CHECKSUM_PATH"
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
    update_dendrite_stub_shas "$TARGET_COMMIT" "$DRY_RUN"
}

main "$@"
