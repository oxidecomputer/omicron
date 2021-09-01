#!/bin/bash

#
# Regenerate the Crucible Agent Client using the experimental Progenitor tool.
# Expects "omicron" to be checked out alongside "crucible" and "progenitor".
#

set -o errexit
set -o pipefail
set -o xtrace

OMICRON_DIR=$(cd "$(dirname "$0")/.." && pwd)
CRUCIBLE_DIR=$(cd "$OMICRON_DIR/../crucible" && pwd)
PROGENITOR_DIR=$(cd "$OMICRON_DIR/../progenitor" && pwd)

SPEC="$OMICRON_DIR/crucible-agent-client/openapi.json"
rm -f "$SPEC"

CRATE="$OMICRON_DIR/crucible-agent-client"
rm -rf "$CRATE/src" "$CRATE/Cargo.toml"

(cd "$CRUCIBLE_DIR/agent" && cargo run -- open-api -o "$SPEC")
(cd "$PROGENITOR_DIR" && cargo run -- \
    -i "$SPEC" \
    -o "$CRATE" \
    -n crucible-agent-client \
    -v 0.0.0)
