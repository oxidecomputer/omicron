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

./tools/ci_download_cockroachdb
./tools/ci_download_clickhouse
