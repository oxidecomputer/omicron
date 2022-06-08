#!/bin/bash

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

# Parse command line options:
#
# -y  Assume "yes" intead of showing confirmation prompts.
ASSUME_YES="false"
SKIP_PATH_CHECK="false"
while getopts yp flag
do
  case "${flag}" in
    y) ASSUME_YES="true" ;;
    p) SKIP_PATH_CHECK="true" ;;
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
if [[ "${HOST_OS}" == "Linux" ]]; then
  packages=(
    'libpq-dev'
    'pkg-config'
    'xmlsec1'
    'libxmlsec1-dev'
    'libxmlsec1-openssl'
  )
  sudo apt-get update
  confirm "Install (or update) [${packages[*]}]?" && sudo apt-get install ${packages[@]}
elif [[ "${HOST_OS}" == "SunOS" ]]; then
  packages=(
    'pkg:/package/pkg'
    'build-essential'
    'library/postgresql-13'
    'pkg-config'
    'brand/omicron1/tools'
    'library/libxmlsec1'
    # "bindgen leverages libclang to preprocess, parse, and type check C and C++ header files."
    'pkg:/ooce/developer/clang-120'
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
elif [[ "${HOST_OS}" == "Darwin" ]]; then
  packages=(
    'postgresql'
    'pkg-config'
    'libxmlsec1'
  )
  confirm "Install (or update) [${packages[*]}]?" && brew install ${packages[@]}
else
  echo "Unsupported OS: ${HOST_OS}"
  exit -1
fi

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

./tools/ci_download_cockroachdb
./tools/ci_download_clickhouse

# Install static console assets. These are used when packaging Nexus.
./tools/ci_download_console

# Install OPTE
#
# OPTE is a Rust package that is consumed by a kernel module called xde. This
# installs the `xde` driver and some kernel bits required to work with that
# driver.
if [[ "${HOST_OS}" == "SunOS" ]]; then
    pfexec ./tools/install_opte.sh
fi

# Validate the PATH:
expected_in_path=(
  'pg_config'
  'pkg-config'
  'cockroach'
  'clickhouse'
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
  exit -1
fi

echo "All prerequisites installed successfully, and PATH looks valid"
