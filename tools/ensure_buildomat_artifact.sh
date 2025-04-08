#!/usr/bin/env bash
#
# Ensure a buildomat artifact is downloaded and available locally.


set -o errexit
set -o nounset
set -o pipefail

# Published buildomat artifacts are available at a predictable URL:
# https://buildomat.eng.oxide.computer/public/file/ORG/REPO/SERIES/HASH/ARTIFACT
BUILDOMAT_BASE_URL="https://buildomat.eng.oxide.computer/public/file"

# Default value for optional flags
ORG="oxidecomputer"
SERIES="image"
CHECK_HASH=true
OUTDIR="$PWD"

function usage() {
    cat <<EOF
Usage: $0 [OPTIONS] <ARTIFACT> <REPO> <COMMIT>

    REPO:     The repository that published the artifact.
    ARTIFACT: The name of the artifact.
    COMMIT:   The commit the artifact was published from.

Options:
    -o <ORG>    Org containing the repository. [default: $ORG]
    -s <SERIES> The series artifact was published to. [default: $SERIES]
    -f          Disable artifact validation.
    -O <OUTDIR> Directory to output artifact to. [default: $OUTDIR]
    -h          Print help and exit

By default, this script expects a SHA256 hash to be published alongside
the artifact with the same name but with a '.sha256.txt' suffix. This hash
is used to validate the downloaded artifact as well as to check if we can
skip downloading the artifact if it already exists locally. To disable this
behavior, pass the -f flag.

Note that if hash validation is disabled, we'll always download the artifact
even if it already exists locally.
EOF
}

function validate_file_hash() {
    local file="$1"
    local hash="$2"
    echo "$hash  $file" | shasum -a 256 --check --status
}

function main() {
    # Parse flags
    local opt
    while getopts "o:s:fO:h" opt; do
        case $opt in
            o) ORG="$OPTARG" ;;
            s) SERIES="$OPTARG" ;;
            f) CHECK_HASH=false ;;
            O) OUTDIR="$OPTARG" ;;

            h)
                usage
                exit 0
                ;;
            *)
                usage
                exit 1
                ;;
        esac
    done
    shift $((OPTIND-1))

    # Grab required arguments
    if [[ $# -ne 3 ]]; then
        usage
        exit 1
    fi

    ARTIFACT="$1"
    REPO="$2"
    COMMIT="$3"

    ARTIFACT_URL="$BUILDOMAT_BASE_URL/$ORG/$REPO/$SERIES/$COMMIT/$ARTIFACT"
    ARTIFACT_OUT="$OUTDIR/$ARTIFACT"

    echo "Ensuring $ORG/$REPO/$SERIES/$ARTIFACT in $OUTDIR"
    echo "  (commit: $COMMIT)"

    local hash=""
    # If hash checking is enabled, grab the expected hash
    if $CHECK_HASH; then
        local hash_url="$ARTIFACT_URL.sha256.txt"
        echo "Getting hash for $ARTIFACT"
        hash="$(curl --silent --show-error --fail --location "$hash_url")"
        echo "  (hash: $hash)"
    fi

    # Check if the artifact already exists
    if [[ -f "$ARTIFACT_OUT" ]]; then
        if $CHECK_HASH; then
            # If the artifact exists and has the correct hash, we're done.
            if validate_file_hash "$ARTIFACT_OUT" "$hash"; then
                echo "$ARTIFACT already exists with correct hash"
                exit 0
            else
                echo "$ARTIFACT already exists but has incorrect hash, re-downloading"
            fi
        else
            echo "$ARTIFACT already exists but hash validation disabled, re-downloading"
        fi
    fi

    # Either the artifact doesn't exist or it has the wrong hash. Download it.
    mkdir -p "$OUTDIR"
    curl --silent --show-error --fail --location --output "$ARTIFACT_OUT" "$ARTIFACT_URL"

    # If hash checking is enabled, validate the downloaded artifact.
    if $CHECK_HASH; then
        if ! validate_file_hash "$ARTIFACT_OUT" "$hash"; then
            echo "Downloaded artifact failed verification"
            exit 1
        fi
    fi

    echo "$ARTIFACT downloaded successfully"

}

main "$@"