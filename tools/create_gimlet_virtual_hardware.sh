#!/bin/bash
#
# Make me a Gimlet!
#
# The entire control plane stack is designed to run and operate on the Oxide
# rack, on each Gimlet. However, Gimlets are not always available for
# development. This script can be used to create a few pieces of virtual
# hardware that _simulate_ a Gimlet, allowing us to develop software that
# approximates operation on Oxide hardware.
#
# This script is specific to gimlet virtual hardware setup. If you only need a
# gimlet, use create_gimlet_virtual_hardware.sh.
#
# See `docs/how-to-run.adoc` section "Set up virtual hardware" for more details.

set -e
set -u
set -x

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
OMICRON_TOP="$SOURCE_DIR/.."

. "$SOURCE_DIR/virtual_hardware.sh"

MARKER=/etc/opt/oxide/NO_INSTALL
if [[ -f "$MARKER" ]]; then
    echo "This system has the marker file $MARKER, aborting." >&2
    exit 1
fi

ensure_run_as_root
ensure_zpools
