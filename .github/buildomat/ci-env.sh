# Setup shared across Buildomat CI builds.
#
# This file contains environment variables shared across Buildomat CI jobs.

# Color the output for easier readability.
export CARGO_TERM_COLOR=always
# Always enable the "tokio_unstable" cfg in order to use
# Tokio's unstable features for `tokio-dtrace`'s probes.
export RUSTFLAGS="--cfg tokio_unstable"
