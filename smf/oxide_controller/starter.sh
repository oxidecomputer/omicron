#!/bin/bash

# The sole purpose of this script is to launch the oxide_controller
# in the background.

set -eou pipefail

CONTROLLER="/opt/oxide/oxcp/oxide_controller"
EXECUTABLE="$CONTROLLER/oxide_controller"
CONFIG="$CONTROLLER/smf/oxide_controller/config.toml"

ctrun -l child -o noorphan,regent "$EXECUTABLE" "$CONFIG" &
disown
exit 0
