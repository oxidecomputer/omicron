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
while getopts yp flag
do
  case "${flag}" in
    y) ASSUME_YES="true" ;;
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
if [[ "${HOST_OS}" == "SunOS" ]]; then
  packages=(
    'pkg:/package/pkg'
    'library/postgresql-13'
    'pkg-config'
    'brand/omicron1/tools'
    'library/libxmlsec1'
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
else
  echo "Unsupported OS: ${HOST_OS}"
  exit -1
fi

# Install OPTE
#
# OPTE is a Rust package that is consumed by a kernel module called xde. This
# installs the `xde` driver and some kernel bits required to work with that
# driver.
if [[ "${HOST_OS}" == "SunOS" ]]; then
    ./tools/install_opte.sh
fi

echo "All runner prerequisites installed successfully"
