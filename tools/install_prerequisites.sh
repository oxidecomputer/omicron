#!/bin/bash

set -eu

# Set the CWD to Omicron's source.
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

function on_exit
{
  echo "Something went wrong, but this script is idempotent - If you can fix the issue, try re-running"
}

trap on_exit ERR

# Offers a confirmation prompt.
#
# Args:
#  $1: Text to be displayed
function confirm
{
  read -r -p "$1 (y/n): " response
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
  )
  confirm "Install (or update) [${packages[*]}]?" && sudo apt-get install ${packages[@]}
elif [[ "${HOST_OS}" == "SunOS" ]]; then
  packages=(
    'pkg:/package/pkg'
    'build-essential'
    'library/postgresql-13'
    'pkg-config'
    'brand/omicron1/tools'
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

# Check all paths before returning an error.
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
