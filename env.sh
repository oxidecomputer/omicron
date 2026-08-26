# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# omicron environment file: source this file to set up your PATH to use the
# various tools in the Omicron workspace where this file lives.  The test suite
# and other commands expect these copies of these tools to be on your PATH.
#
# See also: ./.envrc

OLD_SHELL_OPTS=$-
set -o xtrace

OMICRON_WS=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
export PATH="$OMICRON_WS/out/cockroachdb/bin:$PATH"
export PATH="$OMICRON_WS/out/clickhouse:$PATH"
export PATH="$OMICRON_WS/out/dendrite-stub/bin:$PATH"
export PATH="$OMICRON_WS/out/mgd/root/opt/oxide/mgd/bin:$PATH"
export PATH="$OMICRON_WS/out/mg-ddm/root/opt/oxide/mg-ddm/bin:$PATH"

# if xtrace was set previously, do not unset it
case $OLD_SHELL_OPTS in
    *x*)
        unset OLD_SHELL_OPTS OMICRON_WS
        ;;
    *)
        unset OLD_SHELL_OPTS OMICRON_WS
        set +o xtrace
        ;;
esac
