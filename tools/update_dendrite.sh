#!/bin/bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ARG0="$(basename "${BASH_SOURCE[0]}")"

function usage {
    echo "usage: $0 -c COMMIT [-n]"
    echo
    echo "  -c COMMIT   Ask to update Dendrite to a specific commit."
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
        usage
    fi
    install_toml2json
    do_update_packages "$TARGET_COMMIT" "$DRY_RUN" "$REPO" "${PACKAGES[@]}"
}

main "$@"
