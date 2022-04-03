#!/bin/bash

set -eu

# Set the CWD to Omicron's source.
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

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

./tools/ci_download_cockroachdb
./tools/ci_download_clickhouse

# Validate the PATH:
expected_in_path=(
  'pg_config'
  'pkg-config'
)

declare -A illumos_hints=(
  ['pg_config']="On illumos, this is typically found in '/opt/ooce/bin'"
  ['pkg-config']="On illumos, this is typically found in '/usr/bin'"
)

for command in "${expected_in_path[@]}"; do
  rc=0
  which "$command" &> /dev/null || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "$command seems installed, but not found in PATH. Please add it."


    if [[ "${HOST_OS}" == "SunOS" ]]; then
      if [ "${illumos_hints[$command]+_}" ]; then
        echo "${illumos_hints[$command]}"
      fi
    fi
    exit -1
  fi
done
