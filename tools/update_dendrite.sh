#!/usr/bin/env bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

function usage {
    echo "usage: $0 [-c COMMIT] [-n]"
    echo
    echo "  -b COMMIT   Ask to update dendrite to HEAD on the named branch."
    echo "  -c COMMIT   Ask to update dendrite to a specific commit."
    echo "              If this is unset, Github is queried."
    echo "  -n          Dry-run"
    exit 1
}

PACKAGES=(
  "dendrite-asic"
  "dendrite-softnpu"
  "dendrite-stub"
)

CRATES=(
  "dpd-client"
)

REPO="oxidecomputer/dendrite"

. "$SOURCE_DIR/update_helpers.sh"

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
    do_update_packages "$TARGET_COMMIT" "$DRY_RUN" "$REPO" "${PACKAGES[@]}"
    do_update_crates "$TARGET_COMMIT" "$DRY_RUN" "$REPO" "${CRATES[@]}"
    update_dendrite_stub_shas "$TARGET_COMMIT" "$DRY_RUN"
    echo COMMIT=\""$TARGET_COMMIT"\" > tools/dendrite_version
}

main "$@"
