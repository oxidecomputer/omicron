#!/usr/bin/env bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

function usage {
    echo "usage: $0 [-c COMMIT] [-n]"
    echo
    echo "  -c COMMIT   Ask to update Lldp to a specific commit."
    echo "              If this is unset, Github is queried."
    echo "  -n          Dry-run"
    exit 1
}

PACKAGES=(
  "lldp"
)

CRATES=(
  "lldp"
)

REPO="oxidecomputer/lldp"

. "$SOURCE_DIR/update_helpers.sh"

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
    do_update_crates "$TARGET_COMMIT" "$DRY_RUN" "$REPO" "${CRATES[@]}"
}

main "$@"
