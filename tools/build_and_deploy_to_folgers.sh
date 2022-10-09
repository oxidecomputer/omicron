#!/bin/bash

set -eux

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

TARGET="folgers"

echo "Packaging Omicron"
cargo run --release --bin omicron-package -- package

echo "Transferring Omicron to ${TARGET}"
rsync --delete -Paz out target/release/omicron-package package-manifest.toml root@${TARGET}:/root/omicron

echo "Deploying Omicron to ${TARGET}"
ssh root@${TARGET} "cd /root/omicron && ./omicron-package install"
