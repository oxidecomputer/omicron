#!/usr/bin/env bash

set -eu

# Set the CWD to Omicron's source.
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

./tools/install_builder_prerequisites.sh "$@"
./tools/install_runner_prerequisites.sh "$@"
