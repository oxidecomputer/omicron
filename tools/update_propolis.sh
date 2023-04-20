#!/bin/bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ARG0="$(basename "${BASH_SOURCE[0]}")"

source "$SOURCE_DIR/propolis_version"

not_found() {
    echo "ERROR: This script is pretty simple - it checks for the following:"
    echo "  - A commit in ./tools/propolis_version"
    echo "  - That SAME commit in ./package-manifest.toml"
    echo "  - That SAME commit in ./Cargo.toml"
    echo
    echo "If all those commits match, we can replace them all with a later version from Propolis"
    echo "However, this message means they don't match! If they don't parse, we can't find/replace."
    exit 1
}

bad_source_commit() {
    echo "ERROR: The source commit does not appear to correspond with a known package"
    exit 1
}

bad_target_commit() {
    echo "ERROR: The target commit does not appear to correspond with a known package"
    exit 1
}

function usage {
    echo "usage: $0 [-c COMMIT] [-n]"
    echo
    echo "  -c COMMIT   Ask to update Propolis to a specific commit."
    echo "              If this is unset, Github is queried."
    echo "  -n          Dry-run"
    exit 1
}

function get_sha {
    REPO="$1"
    COMMIT="$2"
    ARTIFACT="$3"
    curl -fsS "https://buildomat.eng.oxide.computer/public/file/$REPO/image/$COMMIT/$ARTIFACT.sha256.txt"
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
      TARGET_COMMIT=$(curl -fsS https://api.github.com/repos/oxidecomputer/propolis/commits | jq -r '.[0].sha')
      echo "Github thinks the latest commit is: $TARGET_COMMIT"
    fi

    if [[ "$TARGET_COMMIT" == "$COMMIT" ]]; then
      echo "OK: Already up-to-date (requested commit matches 'tools/propolis_version')"
      exit 0
    fi

    echo "UPDATING: From $COMMIT -> $TARGET_COMMIT"

    # Check that the commit we think is latest actually does appear in the manifests
    grep -Fq "$COMMIT" "$SOURCE_DIR/../package-manifest.toml" || not_found
    grep -Fq "$COMMIT" "$SOURCE_DIR/../Cargo.toml" || not_found

    # Check that the commit we think is latest actually has published SHAs,
    # and that those SHAs are currently in-use.
    REPO="oxidecomputer/propolis"
    PROPOLIS_SHA=$(get_sha "$REPO" "$COMMIT" propolis-server) || bad_source_commit
    grep -Fq "$PROPOLIS_SHA" "$SOURCE_DIR/../package-manifest.toml" || not_found

    # Check that the commit we are aiming for actually has published SHAs.
    TARGET_PROPOLIS_SHA=$(get_sha "$REPO" "$TARGET_COMMIT" propolis-server) || bad_target_commit

    IN_PLACE="-i"
    if [[ "$DRY_RUN" == "yes" ]]; then
      IN_PLACE="-n"
    fi

    # All modifications should happen after this point

    set -o xtrace
    # Update package manifest with the new commit and SHAs
    sed $IN_PLACE -e "s/$COMMIT/$TARGET_COMMIT/g" "$SOURCE_DIR/../package-manifest.toml"
    sed $IN_PLACE -e "s/$PROPOLIS_SHA/$TARGET_PROPOLIS_SHA/g" "$SOURCE_DIR/../package-manifest.toml"

    # Update Cargo.toml with the new commit
    sed $IN_PLACE -e "s/$COMMIT/$TARGET_COMMIT/g" "$SOURCE_DIR/../Cargo.toml"

    # Update the "stored version" with the new commit
    sed $IN_PLACE -e "s/$COMMIT/$TARGET_COMMIT/g" "$SOURCE_DIR/propolis_version"
    set +o xtrace

    echo "OK: Update complete"
}

main "$@"
