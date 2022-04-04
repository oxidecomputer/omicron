#!/bin/bash

set -eu

# Set the CWD to Omicron's source.
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

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
  sudo apt-get install libpq-dev
  sudo apt-get install pkg-config
elif [[ "${HOST_OS}" == "SunOS" ]]; then
  need=(
    'pkg:/package/pkg'
    'build-essential'
    'library/postgresql-13'
    'pkg-config'
    'brand/omicron1/tools'
  )

  # Perform updates
  if (( ${#need[@]} > 0 )); then
    rc=0
    pfexec pkg install -v "${need[@]}" || rc=$?
    # Return codes:
    #  0: Normal Success
    #  4: Failure because we're already up-to-date. Also acceptable.
    if [ "$rc" -ne 4 ] && [ "$rc" -ne 0 ]; then
      exit "$rc"
    fi
  fi

  pkg list -v "${need[@]}"
elif [[ "${HOST_OS}" == "Darwin" ]]; then
  brew install postgresql
  brew install pkg-config
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

# Validate the PATH:
expected_in_path=(
  'pg_config'
  'pkg-config'
  'cockroach'
  'clickhouse'
)

declare -A illumos_path_hints=(
  ['pg_config']="On illumos, this is typically found in '/opt/ooce/bin'"
  ['pkg-config']="On illumos, this is typically found in '/usr/bin'"
)

declare -A path_hints=(
  ['cockroach']="This should have been installed to '$PWD/out/cockroachdb/bin'"
  ['clickhouse']="This should have been installed to '$PWD/out/clickhouse'"
)

# Check all paths before returning an error.
ANY_PATH_ERROR="false"
for command in "${expected_in_path[@]}"; do
  rc=0
  which "$command" &> /dev/null || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "$command seems installed, but not found in PATH. Please add it."

    if [[ "${HOST_OS}" == "SunOS" ]]; then
      if [ "${illumos_path_hints[$command]+_}" ]; then
        echo "${illumos_path_hints[$command]}"
      fi
    fi

    if [ "${path_hints[$command]+_}" ]; then
      echo "${path_hints[$command]}"
    fi

    ANY_PATH_ERROR="true"
  fi
done

if [ "$ANY_PATH_ERROR" == "true" ]; then
  exit -1
fi

echo "All prerequisites installed successfully, and PATH looks valid"
