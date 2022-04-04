#!/bin/bash

set -eu

on_exit () {
    echo "Something went wrong, but this script is idempotent - If you can fix the issue, try re-running"
}

trap on_exit ERR

# Set the CWD to Omicron's source.
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

HOST_OS=$(uname -s)
if [[ "${HOST_OS}" == "Linux" ]]; then
  sudo apt-get install libpq-dev
  sudo apt-get install pkg-config
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
  pfexec pkg install -v "${packages[@]}" || rc=$?
  # Return codes:
  #  0: Normal Success
  #  4: Failure because we're already up-to-date. Also acceptable.
  if [[ "$rc" -ne 4 ]] && [[ "$rc" -ne 0 ]]; then
    exit "$rc"
  fi

  pkg list -v "${packages[@]}"
elif [[ "${HOST_OS}" == "Darwin" ]]; then
  brew install postgresql
  brew install pkg-config
else
  echo "Unsupported OS: ${HOST_OS}"
  exit -1
fi

./tools/ci_download_cockroachdb
./tools/ci_download_clickhouse
