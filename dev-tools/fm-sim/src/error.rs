// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SimStateId;

/// The ways in which a command can fail, one variant per layer it can fail in.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The input line could not be split into words (e.g., an unclosed quote).
    #[error(transparent)]
    Split(#[from] shell_words::ParseError),
    /// The input line was split into words but did not parse as a command. This
    /// includes `--help`, which clap reports as an error.
    #[error(transparent)]
    Parse(#[from] clap::Error),
    /// The input line parsed as a command successfully but failed when run
    /// against the simulated environment.
    #[error(transparent)]
    Sim(#[from] nexus_fm_simulation::Error),
    /// The input line parsed as a command successfully but failed when run
    /// against the session's history.
    #[error(transparent)]
    Session(#[from] SessionError),
}

/// Why a session command failed.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// The session has no state with the given id.
    #[error("no such state: {0}")]
    NoSuchState(SimStateId),
}
