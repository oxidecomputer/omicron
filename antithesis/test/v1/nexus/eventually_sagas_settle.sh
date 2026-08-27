#!/usr/bin/env bash
# TODO-RAINCLAUDE: runs after faults stop; asserts every saga reaches a terminal state.
set -euo pipefail
exec /opt/oxide/bin/omicron-antithesis-workload sagas-settle
