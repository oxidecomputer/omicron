#!/bin/bash
#:
#: name = "helios / CI tools"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.78.0"
#: output_rules = [
#:	"=/work/end-to-end-tests/*.gz",
#:	"=/work/caboose-util.gz",
#:	"=/work/tufaceous.gz",
#:	"=/work/commtest",
#:	"=/work/permslip.gz",
#: ]
#: access_repos = [
#:      "oxidecomputer/permission-slip",
#:      "oxidecomputer/sshauth"
#: ]

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

ptime -m ./tools/install_builder_prerequisites.sh -yp

########## end-to-end-tests ##########

banner end-to-end-tests

#
# Reduce debuginfo just to line tables.
#
export CARGO_PROFILE_DEV_DEBUG=1
export CARGO_PROFILE_TEST_DEBUG=1
export CARGO_INCREMENTAL=0

ptime -m cargo build --locked -p end-to-end-tests --tests --bin bootstrap \
	--message-format json-render-diagnostics >/tmp/output.end-to-end.json

mkdir -p /work
ptime -m cargo build --locked -p end-to-end-tests --tests --bin commtest
cp target/debug/commtest /work/commtest

mkdir -p /work/end-to-end-tests
for p in target/debug/bootstrap $(/opt/ooce/bin/jq -r 'select(.profile.test) | .executable' /tmp/output.end-to-end.json); do
	# shellcheck disable=SC2094
	ptime -m gzip < "$p" > /work/end-to-end-tests/"$(basename "$p").gz"
done

########## caboose-util ##########

banner caboose-util

ptime -m cargo build --locked -p caboose-util --release
ptime -m gzip < target/release/caboose-util > /work/caboose-util.gz

########## tufaceous ##########

banner tufaceous

ptime -m cargo build --locked -p tufaceous --release
ptime -m gzip < target/release/tufaceous > /work/tufaceous.gz

########## permission-slip ##########

banner permission-slip

source "./tools/permslip_commit"
git init /work/permission-slip-build
pushd /work/permission-slip-build
git remote add origin https://github.com/oxidecomputer/permission-slip.git
ptime -m git fetch --depth 1 origin "$COMMIT"
git checkout FETCH_HEAD
ptime -m cargo build --locked -p permission-slip-client --release
ptime -m gzip < target/release/permslip > /work/permslip.gz
