# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# Setup shared across Buildomat CI builds.
#
# This file contains environment variables shared across Buildomat CI jobs.

# Color the output for easier readability.
export CARGO_TERM_COLOR=always

# On illumos, store crashing process cores in the test temp dir so that:
#
# * Any core files are uploaded by Buildomat.
# * Core files cause the CI job to fail even if the run is otherwise successful.
#
# `-p` sets the pattern on this shell, and child processes of the shell inherit
# it.
if [[ "$(uname -s)" == "SunOS" ]]; then
    mkdir -p /var/tmp/omicron_tmp/cores
    coreadm -p '/var/tmp/omicron_tmp/cores/core.%f.%p.core'
fi
