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
}

main "$@"
