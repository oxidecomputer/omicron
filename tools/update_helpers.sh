#!/usr/bin/env bash

set -o pipefail
set -o errexit

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

not_found() {
    VALUE="$1"
    FILE="$2"
    echo "ERROR: Failed to find $VALUE in $FILE"
    exit 1
}

bad_target_commit() {
    echo "ERROR: The target commit does not appear to correspond with a known package"
    exit 1
}

# Get the SHA for a Buildomat artifact.
#
# Note the "series" component of the Buildomat public file hierarchy
# is the optional 4th argument, and defaults to "image".
function get_sha {
    REPO="$1"
    COMMIT="$2"
    ARTIFACT="$3"
    SERIES="${4:-image}"
    curl -fsS "https://buildomat.eng.oxide.computer/public/file/$REPO/$SERIES/$COMMIT/$ARTIFACT.sha256.txt"
}

function get_latest_commit_from_gh {
    REPO="$1"
    if [[ -z "$2" ]]; then
	    TARGET_BRANCH=main
    else
	    TARGET_BRANCH="$2"
    fi

    curl -fsS "https://buildomat.eng.oxide.computer/public/branch/$REPO/$TARGET_BRANCH"
}

function install_toml2json {
    type toml2json &> /dev/null || cargo install toml2json
}

function do_update_packages {
    TARGET_COMMIT="$1"
    shift
    DRY_RUN="$1"
    shift
    REPO="$1"
    shift
    PACKAGES=("$@")

    echo "UPDATING PACKAGES ---"
    echo "  target: $TARGET_COMMIT"
    echo "  dry_run: $DRY_RUN"
    echo "  repo: $REPO"
    echo "  packages:" "${PACKAGES[@]}"

    # Find the old commits and hashes
    PKG_MANIFEST="$SOURCE_DIR/../package-manifest.toml"
    PACKAGE_COMMITS=()
    PACKAGE_SHAS=()
    for PACKAGE in "${PACKAGES[@]}"; do
        COMMIT=$(toml2json "$PKG_MANIFEST" | jq -r ".package | .[\"$PACKAGE\"].source.commit") \
          || not_found "$PACKAGE commit" "$PKG_MANIFEST"
        SHA=$(toml2json "$PKG_MANIFEST" | jq -r ".package | .[\"$PACKAGE\"].source.sha256") \
          || not_found "$PACKAGE sha" "$PKG_MANIFEST"

        PACKAGE_COMMITS=("${PACKAGE_COMMITS[@]}" "$COMMIT")
        PACKAGE_SHAS=("${PACKAGE_SHAS[@]}" "$SHA")
    done

    # Check that the commit we are aiming for actually has published SHAs.
    TARGET_SHAS=()
    for PACKAGE in "${PACKAGES[@]}"; do
        TARGET_SHA=$(get_sha "$REPO" "$TARGET_COMMIT" "$PACKAGE") || bad_target_commit
        TARGET_SHAS=("${TARGET_SHAS[@]}" "$TARGET_SHA")
    done

    IN_PLACE="-i"
    if [[ "$DRY_RUN" == "yes" ]]; then
      IN_PLACE="-n"
    fi

    # All modifications should happen after this point

    set -o xtrace

    # Update package manifest with the new commit and SHAs
    for IDX in "${!PACKAGES[@]}"; do
        echo "UPDATING: [${PACKAGES[$IDX]}]"
        sed $IN_PLACE -e "s/${PACKAGE_COMMITS[$IDX]}/$TARGET_COMMIT/g" "$PKG_MANIFEST"
        sed $IN_PLACE -e "s/${PACKAGE_SHAS[$IDX]}/${TARGET_SHAS[$IDX]}/g" "$PKG_MANIFEST"
    done

    set +o xtrace

    echo "OK: Update complete"
}

function do_update_crates {
    TARGET_COMMIT="$1"
    shift
    DRY_RUN="$1"
    shift
    REPO="$1"
    shift
    CRATES=("$@")

    echo "UPDATING CRATES ---"
    echo "  target: $TARGET_COMMIT"
    echo "  dry_run: $DRY_RUN"
    echo "  repo: $REPO"
    echo "  crates:" "${CRATES[@]}"

    CARGO_TOML="$SOURCE_DIR/../Cargo.toml"
    CARGO_COMMITS=()
    for CRATE in "${CRATES[@]}"; do
        COMMIT=$(toml2json "$CARGO_TOML" | jq -r ".workspace.dependencies | .[\"$CRATE\"].rev") \
          || not_found "$CRATE commit" "$CARGO_TOML"

        CARGO_COMMITS=("${CARGO_COMMITS[@]}" "$COMMIT")
    done

    IN_PLACE="-i"
    if [[ "$DRY_RUN" == "yes" ]]; then
      IN_PLACE="-n"
    fi

    # All modifications should happen after this point

    set -o xtrace

    # Update Cargo.toml with the new commit
    for IDX in "${!CRATES[@]}"; do
        echo "UPDATING: [${CRATES[$IDX]}]"
        sed $IN_PLACE -e "s/${CARGO_COMMITS[$IDX]}/$TARGET_COMMIT/g" "$CARGO_TOML"
    done

    set +o xtrace

    echo "OK: Update complete"
}
