#!/usr/bin/env bash

set -eu

MARKER=/etc/opt/oxide/NO_INSTALL
if [[ -f "$MARKER" ]]; then
  echo "This system has the marker file $MARKER, aborting." >&2
  exit 1
fi

# Set the CWD to Omicron's source.
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

function on_exit
{
  echo "Something went wrong, but this script is idempotent - If you can fix the issue, try re-running"
}

trap on_exit ERR

function usage
{
  echo "Usage: ./install_builder_prerequisites.sh <OPTIONS>"
  echo "  Options: "
  echo "   -y: Assume 'yes' instead of showing confirmation prompts"
  echo "   -p: Skip checking paths"
  echo "   -r: Number of retries to perform for network operations (default: 3)"
  exit 1
}

ASSUME_YES="false"
SKIP_PATH_CHECK="false"
RETRY_ATTEMPTS=3
while getopts ypr: flag
do
  case "${flag}" in
    y) ASSUME_YES="true" ;;
    p) SKIP_PATH_CHECK="true" ;;
    r) RETRY_ATTEMPTS=${OPTARG} ;;
    *) usage
  esac
done

# Offers a confirmation prompt, unless we were passed `-y`.
#
# Args:
#  $1: Text to be displayed
function confirm
{
  if [[ "${ASSUME_YES}" == "true" ]]; then
    response=y
  else
    read -r -p "$1 (y/n): " response
  fi
  case $response in
    [yY])
      true
      ;;
    *)
      false
      ;;
  esac
}

# Function which executes all provided arguments, up to ${RETRY_ATTEMPTS}
# times, or until the command succeeds.
function retry
{
  attempts="${RETRY_ATTEMPTS}"
  # Always try at least once
  attempts=$((attempts < 1 ? 1 : attempts))
  for i in $(seq 1 $attempts); do
    retry_rc=0
    "$@" || retry_rc=$?;
    if [[ "$retry_rc" -eq 0 ]]; then
      return
    fi

    if [[ $i -ne $attempts ]]; then
      echo "Failed to run command -- will try $((attempts - i)) more times"
    fi
  done

  exit $retry_rc
}

function xtask
{
  if [ -z ${XTASK_BIN+x} ]; then
    cargo xtask "$@"
  else
    "$XTASK_BIN" "$@"
  fi
}

# Packages to be installed on all OSes:
#
# - libpq, the PostgreSQL client lib.
# We use Diesel's PostgreSQL support to connect to CockroachDB (which is
# wire-compatible with PostgreSQL). Diesel uses the native libpq to do this.
# `pg_config` is a utility which may be used to query for the installed
# PostgreSQL libraries, and is expected by the Omicron build to exist in
# the developer's PATH variable.
# - pkg-config, a tool for querying installed libraries.
#
# Packages to be installed on Helios only:
#
# - pkg, the IPS client (though likely it will just be updated)
# - build-essential: Common development tools
# - brand/omicron1/tools: Oxide's omicron1-brand Zone

HOST_OS=$(uname -s)

function install_packages {
  if [[ "${HOST_OS}" == "Linux" ]]; then
    # If Nix is in use, we don't need to install any packagess here,
    # as they're provided by the Nix flake. 
    if [[ "${OMICRON_USE_FLAKE-}" = 1 ]] && nix flake show &> /dev/null; then
      echo "OMICRON_USE_FLAKE=1 in environment and nix detected in PATH, skipping package installation"
      return
    fi

    packages=(
      'libpq-dev'
      'pkg-config'
      'xmlsec1'
      'libxmlsec1-dev'
      'libxmlsec1-openssl'
      'libclang-dev'
      'libsqlite3-dev'
    )
    sudo apt-get update
    if [[ "${ASSUME_YES}" == "true" ]]; then
        sudo apt-get install -y "${packages[@]}"
    else
        confirm "Install (or update) [${packages[*]}]?" && sudo apt-get install "${packages[@]}"
    fi
  elif [[ "${HOST_OS}" == "SunOS" ]]; then
    CLANGVER=15
    PGVER=13
    packages=(
      "pkg:/package/pkg"
      "build-essential"
      "cmake"
      "library/postgresql-$PGVER"
      "pkg-config"
      "library/libxmlsec1"
      # "bindgen leverages libclang to preprocess, parse, and type check C and C++ header files."
      "pkg:/ooce/developer/clang-$CLANGVER"
      "system/library/gcc-runtime"
      "system/library/g++-runtime"
    )

    # The clickhouse binary depends on the {gcc,g++}-runtime packages being
    # at least version 13. If we are on a version below that, update to the
    # latest.
    RTVER=13
    for p in system/library/gcc-runtime system/library/g++-runtime; do
        # buildmat runners do not yet have a new enough `pkg` for -o
        #v=$(pkg list -Ho release $p)
        v=$(pkg list -H -F tsv $p | awk '{split($2, a, "-"); print a[1]}')
        ((v < RTVER)) && packages+=($p@latest)
    done

    # Install/update the set of packages.
    # Explicitly manage the return code using "rc" to observe the result of this
    # command without exiting the script entirely (due to bash's "errexit").
    rc=0
    confirm "Install (or update) [${packages[*]}]?" && { pfexec pkg install -v "${packages[@]}" || rc=$?; }
    # Return codes:
    #  0: Normal Success
    #  4: Failure because we're already up-to-date. Also acceptable.
    if ((rc != 4 && rc != 0)); then
      exit "$rc"
    fi

    confirm "Set mediators?" && {
      pfexec pkg set-mediator -V $CLANGVER clang llvm
      pfexec pkg set-mediator -V $PGVER postgresql
    }

    pkg mediator -a
    pkg publisher
    pkg list -afv "${packages[@]}"
  elif [[ "${HOST_OS}" == "Darwin" ]]; then
    packages=(
      'coreutils'
      'postgresql'
      'pkg-config'
      'libxmlsec1'
    )
    confirm "Install (or update) [${packages[*]}]?" && brew install "${packages[@]}"
  else
    echo "Unsupported OS: ${HOST_OS}"
    exit 1
  fi
}

retry install_packages

# CockroachDB and Clickhouse are used by Omicron for storage of
# control plane metadata and metrics.
#
# They are used in a couple of spots within Omicron:
#
# - Test Suite: The test suite, regardless of host OS, builds temporary
# databases for testing, and expects `cockroach` and `clickhouse` to
# exist as a part of the PATH.
# - Packaging: When constructing packages on Helios, these utilities
# are packaged into zones which may be launched by the sled agent.

retry xtask download \
    cockroach \
    clickhouse \
    console \
    dendrite-stub \
    maghemite-mgd \
    transceiver-control

# Validate the PATH:
expected_in_path=(
  'pg_config'
  'pkg-config'
  'cockroach'
  'clickhouse'
  'dpd'
)

function show_hint
{
  case "$1" in
    "pg_config")
      if [[ "${HOST_OS}" == "SunOS" ]]; then
        echo "On illumos, $1 is typically found in '/opt/ooce/bin'"
      fi
      ;;
    "pkg-config")
      if [[ "${HOST_OS}" == "SunOS" ]]; then
        echo "On illumos, $1 is typically found in '/usr/bin'"
      fi
      ;;
    "cockroach")
      echo "$1 should have been installed to '$PWD/out/cockroachdb/bin'"
      ;;
    "clickhouse")
      echo "$1 should have been installed to '$PWD/out/clickhouse'"
      ;;
    "dpd")
      echo "$1 should have been installed to '$PWD/out/dendrite-stub/bin'"
      ;;
    *)
      ;;
  esac
}

# Check all paths before returning an error, unless we were told not too.
if [[ "$SKIP_PATH_CHECK" == "true" ]]; then
  echo "All prerequisites installed successfully"
  exit 0
fi

ANY_PATH_ERROR="false"
for command in "${expected_in_path[@]}"; do
  rc=0
  which "$command" &> /dev/null || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    echo "ERROR: $command seems installed, but was not found in PATH. Please add it."
    show_hint "$command"
    ANY_PATH_ERROR="true"
  fi
done

if [[ "$ANY_PATH_ERROR" == "true" ]]; then
  exit 1
fi

echo "All builder prerequisites installed successfully, and PATH looks valid"
