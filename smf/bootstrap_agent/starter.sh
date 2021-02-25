#!/bin/bash

# The sole purpose of this script is to launch the bootstrap_agent
# in the background.

set -eou pipefail

EXECUTABLE="/opt/oxide/oxcp/bootstrap_agent/bootstrap_agent"

ctrun -l child -o noorphan,regent "$EXECUTABLE" $(uuidgen) 127.0.0.1:12346 &
disown
exit 0
