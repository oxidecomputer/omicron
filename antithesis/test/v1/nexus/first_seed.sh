#!/usr/bin/env bash
# TODO-RAINCLAUDE: runs once per timeline before any driver; creates the project and IP pool the drivers use.
set -euo pipefail
exec /opt/oxide/bin/omicron-antithesis-workload seed
