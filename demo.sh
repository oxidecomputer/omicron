#!/bin/bash
set -eu

trap "kill 0" EXIT

cargo build --release --package omicron-package
# cargo build --release --package omicron-common --bin omicron-package
echo "Packaging..."
./target/release/omicron-package package

echo "Launching DB..."
cargo run --bin=omicron-dev -- db-run &> /dev/null &
cargo run --bin=omicron-dev -- ch-run &> /dev/null &

echo "Installing..."
pfexec ./target/release/omicron-package install

echo "Sled Agent and Nexus Online"
sleep 8
./tools/oxapi_demo organization_create_demo myorg
./tools/oxapi_demo project_create_demo      myorg myproject
wait
