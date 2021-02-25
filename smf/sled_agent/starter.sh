#!/bin/bash

# The sole purpose of this script is to launch the sled_agent
# in the background.

set -eou pipefail

EXECUTABLE="/opt/oxide/oxcp/sled_agent/sled_agent"

ctrun -l child -o noorphan,regent "$EXECUTABLE" $(uuidgen) 127.0.0.1:12345 127.0.0.1:12221 &
disown
exit 0
