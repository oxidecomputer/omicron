#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# TLA+ check script.
#
# This script runs all, or the defined, configurations through TLC, and compares
# TLC's exit status with the table below.
#
# The exit codes are:
#
# * 0: the properties are satisfied.
# * 12: safety violation.
# * 13: temporal/action violation.
#
# A non-zero entry indicates a configuration that is expected to fail. This
# could either be an expected failure (i.e., a precondition which we know does
# not satisfy the properties), or a current issue that a subsequent commit will
# fix.
set -euo pipefail

cd "$(dirname "$0")"

REPO_ROOT="$(cd ../.. && pwd)"
DEFAULT_JAR="$REPO_ROOT/out/tla2tools/tla2tools.jar"

usage() {
    cat <<USAGE
usage: $0 [CFG...]

Runs every cfg in the expectation table (or only the CFGs given) through TLC
and checks the exit status.

Environment:
  TLA2TOOLS_JAR  path to tla2tools.jar
                 (default: $DEFAULT_JAR)
  TLC_WORKERS    passed to TLC's -workers (default: auto)

To download the pinned tla2tools.jar to the default location, run
\`cargo xtask download tla2tools\` from the repository root.

Logs and TLC state directories go under the directory printed at the start.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if ! command -v java >/dev/null 2>&1; then
    echo "error: java not found in PATH (TLC requires a Java runtime)" >&2
    exit 2
fi

TLA2TOOLS_JAR="${TLA2TOOLS_JAR:-$DEFAULT_JAR}"
if [[ ! -f "$TLA2TOOLS_JAR" ]]; then
    echo "error: TLA+ tools jar not found at $TLA2TOOLS_JAR" >&2
    if [[ "$TLA2TOOLS_JAR" == "$DEFAULT_JAR" ]]; then
        echo "To download it, run \`cargo xtask download tla2tools\` from $REPO_ROOT." >&2
    else
        echo "Check TLA2TOOLS_JAR, or unset it to use $DEFAULT_JAR." >&2
    fi
    exit 2
fi
echo "tla2tools: $TLA2TOOLS_JAR"

WORKERS="${TLC_WORKERS:-auto}"

# cfg  module  expected-exit-status
EXPECTED=(
    "trust_quorum.cfg          trust_quorum  0"
    "trust_quorum_nocrash.cfg  trust_quorum  13"
)

selected=("$@")
want() {
    [[ ${#selected[@]} -eq 0 ]] && return 0
    local s
    for s in "${selected[@]}"; do
        [[ "$s" == "$1" ]] && return 0
    done
    return 1
}

outdir="$(mktemp -d "${TMPDIR:-/tmp}/tlc-check.XXXXXX")"
echo "output: $outdir"

failures=0
ran=0
for entry in "${EXPECTED[@]}"; do
    read -r cfg module expected <<<"$entry"
    want "$cfg" || continue
    ran=$((ran + 1))
    name="${cfg%.cfg}"
    log="$outdir/$name.log"
    start=$SECONDS
    set +e
    java -XX:+UseParallelGC -cp "$TLA2TOOLS_JAR" tlc2.TLC \
        -workers "$WORKERS" \
        -metadir "$outdir/$name.states" \
        -config "$cfg" "$module.tla" >"$log" 2>&1
    actual=$?
    set -e
    elapsed=$((SECONDS - start))
    if [[ "$actual" == "$expected" ]]; then
        status="ok"
    else
        status="FAIL"
        failures=$((failures + 1))
    fi
    printf '%-4s %-28s expected %-3s got %-3s %4ds  %s\n' \
        "$status" "$cfg" "$expected" "$actual" "$elapsed" "$log"
done

if [[ $ran -eq 0 ]]; then
    echo "error: no cfg matched: ${selected[*]}" >&2
    exit 2
fi

if [[ $failures -ne 0 ]]; then
    echo "$failures of $ran cfg(s) did not produce the expected exit status" >&2
    exit 1
fi
