#!/bin/bash

set -eu

# Set the CWD to Omicron's source.
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

HOST_OS=$(uname -s)
if [[ "${HOST_OS}" == "Linux" ]]; then
  echo "Linux"
  sudo apt-get install libpq-dev
  sudo apt-get install pkg-config
elif [[ "${HOST_OS}" == "SunOS" ]]; then
  echo "illumos"
  need=(
    'build-essential'
    'library/postgresql-13'
    'pkg-config'
    'brand/omicron1/tools'
    'pkg:/package/pkg'
  )
  missing=()
  for (( i = 0; i < ${#need[@]}; i++ )); do
    p=${need[$i]}
    if ! pkg info -q "$p"; then
      missing+=( "$p" )
    fi
  done
  if (( ${#missing[@]} > 0 )); then
    pfexec pkg install -v "${missing[@]}"
  fi
  pkg list -v "${need[@]}"
elif [[ "${HOST_OS}" == "Darwin" ]]; then
  echo "Mac"
  brew install postgresql
  brew install pkg-config
else
  echo "Unsupported OS"
  exit -1
fi

./tools/ci_download_cockroachdb
./tools/ci_download_clickhouse
