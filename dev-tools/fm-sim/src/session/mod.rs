// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The session vocabulary: commands that act on the session (e.g. navigating
//! its history) rather than on the current simulated state.

use clap::Subcommand;

pub(crate) mod state;

pub(crate) use state::StateCmd;

/// Commands that act on the session (e.g. navigating its history) rather than
/// on the current simulated state.
#[derive(Debug, PartialEq, Subcommand)]
pub(crate) enum SessionCommand {
    /// States: the tree of every state this session has produced.
    #[command(subcommand)]
    State(StateCmd),
    // TODO(#10209): Add support for the rest of the planned session commands:
    // `op`, `script`.
}
