#!/bin/bash
#:
#: name = "helios / build-end-to-end-tests"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "stable-1.66.1"
#: output_rules = [
#:	"=/work/*.gz",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

ptime -m ./tools/install_builder_prerequisites.sh -yp

#
# Reduce debuginfo just to line tables.
#
export CARGO_PROFILE_DEV_DEBUG=1
export CARGO_PROFILE_TEST_DEBUG=1

ptime -m cargo build -p end-to-end-tests --tests --bin bootstrap \
	--message-format json-render-diagnostics >/tmp/output.end-to-end.json

for p in target/debug/bootstrap $(/opt/ooce/bin/jq -r 'select(.profile.test) | .executable' /tmp/output.end-to-end.json); do
	# shellcheck disable=SC2094
	ptime -m gzip < "$p" > /work/"$(basename "$p").gz"
done
