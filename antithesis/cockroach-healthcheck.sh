#!/usr/bin/env bash
# TODO-RAINCLAUDE: compose healthcheck; ready=1 makes CockroachDB report unhealthy while draining or before it accepts SQL.

set -euo pipefail

exec curl --silent --fail --output /dev/null "http://127.0.0.1:8080/health?ready=1"
