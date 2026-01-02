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
  echo "Usage: ./install_runner_prerequisites.sh <OPTIONS>"
  echo "  Options: "
  echo "   -y: Assume 'yes' instead of showing confirmation prompts"
  echo "   -p: Skip checking paths"
  echo "   -r: Number of retries to perform for network operations (default: 3)"
  exit 1
}

ASSUME_YES="false"
OMIT_SUDO="false"
RETRY_ATTEMPTS=3
while getopts ypsr: flag
do
  case "${flag}" in
    y) ASSUME_YES="true" ;;
    p) continue ;;
    s) OMIT_SUDO="true" ;;
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

# Packages to be installed Helios:
#
# - libpq, the PostgreSQL client lib.
# We use Diesel's PostgreSQL support to connect to CockroachDB (which is
# wire-compatible with PostgreSQL). Diesel uses the native libpq to do this.
# `pg_config` is a utility which may be used to query for the installed
# PostgreSQL libraries, and is expected by the Omicron build to exist in
# the developer's PATH variable.
# - pkg-config, a tool for querying installed libraries.
# - pkg, the IPS client (though likely it will just be updated)
# - brand/omicron1/tools: Oxide's omicron1-brand Zone
HOST_OS=$(uname -s)

function install_packages {
  if [[ "${HOST_OS}" == "SunOS" ]]; then
    packages=(
      'pkg:/package/pkg'
      'library/postgresql-13'
      'pkg-config'
      'brand/omicron1/tools'
      'library/libxmlsec1'
      'chrony'
      'oxide/platform-identity-cacerts' # necessary for sprockets
    )

    # Install/update the set of packages.
    # Explicitly manage the return code using "rc" to observe the result of this
    # command without exiting the script entirely (due to bash's "errexit").
    rc=0
    confirm "Install (or update) [${packages[*]}]?" && { pfexec pkg install -v "${packages[@]}" || rc=$?; }
    # Return codes:
    #  0: Normal Success
    #  4: Failure because we're already up-to-date. Also acceptable.
    if [[ "$rc" -ne 4 ]] && [[ "$rc" -ne 0 ]]; then
      exit "$rc"
    fi

    pkg list -v "${packages[@]}"
  elif [[ "${HOST_OS}" == "Linux" ]]; then
    packages=(
      'ca-certificates'
      'libpq5'
      'libsqlite3-0'
      'libssl3'
      'libxmlsec1-openssl'
    )
    if [[ "${OMIT_SUDO}" == "false" ]]; then
      maybe_sudo="sudo"
    else
      maybe_sudo=""
    fi
    $maybe_sudo apt-get update
    if [[ "${ASSUME_YES}" == "true" ]]; then
      $maybe_sudo apt-get install -y "${packages[@]}"
    else
      confirm "Install (or update) [${packages[*]}]?" && $maybe_sudo apt-get install "${packages[@]}"
    fi
  else
    echo "Skipping runner prereqs for unsupported OS: ${HOST_OS}"
    exit 1
  fi
}

retry install_packages

if [[ "${HOST_OS}" == "SunOS" ]]; then
    # Install OPTE
    #
    # OPTE is a Rust package that is consumed by a kernel module called xde. This
    # installs the `xde` driver and some kernel bits required to work with that
    # driver.
    retry ./tools/install_opte.sh

    # Grab the SoftNPU machinery (ASIC simulator, scadm, P4 program, etc.)
    #
    # "cargo xtask virtual-hardware create" will use those to setup the softnpu zone
    retry xtask download softnpu
fi

echo "All runner prerequisites installed successfully"
