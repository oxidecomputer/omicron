# omicron environment file: source this file to set up your PATH to use the
# various tools in the Omicron workspace where this file lives.  The test suite
# and other commands expect these copies of these tools to be on your PATH.
#
# See also: ./.envrc

set -o xtrace
OMICRON_WS="$(cd $(dirname "${BASH_SOURCE[0]}") && echo $PWD)"
export PATH="$OMICRON_WS/out/cockroachdb/bin:$PATH"
export PATH="$OMICRON_WS/out/clickhouse:$PATH"
export PATH="$OMICRON_WS/out/dendrite-stub/bin:$PATH"
unset OMICRON_WS
set +o xtrace
