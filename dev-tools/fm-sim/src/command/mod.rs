// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The simulator's command vocabulary.
//!
//! Commands are `clap` subcommands, organized noun-first with the verbs
//! under each noun. Each noun's verbs are split into two enums: queries
//! implementing the [`Query`] trait, and mutations implementing the [`Mutate`]
//! trait. [`Verb`] glues the queries and mutations together for clap, and the
//! [`Simulator`](crate::Simulator) matches on it to run the half it got.

// TODO(#10209): So far there's just one noun, `time`. Subsequent PRs will add
// the rest of the vocabulary described in RFD 726.

use std::fmt;

use clap::Subcommand;
use nexus_fm_simulation::{Error, SimState, SimStateBuilder};

mod time;

pub(crate) use time::{TimeMutate, TimeQuery};

/// A command that reads some aspect of simulator state, as a string.
pub(crate) trait Query: fmt::Debug {
    fn run(&self, state: &SimState) -> Result<String, Error>;
}

/// A command that mutates simulator state using a given `SimStateBuilder`, and
/// produces some output string.
pub(crate) trait Mutate: fmt::Debug {
    fn apply(&self, builder: &mut SimStateBuilder) -> Result<String, Error>;
}

/// A verb subcommand is either a query or a mutation.
///
/// We do a bit of clap-fu here, taking the query `Q` and mutation `M` as type
/// parameters and flattening them both into a single subcommand so clap parses
/// them uniformly.
#[derive(Debug, PartialEq, Subcommand)]
pub(crate) enum Verb<Q: Subcommand + Query, M: Subcommand + Mutate> {
    #[command(flatten)]
    Query(Q),
    #[command(flatten)]
    Mutate(M),
}

/// The full simulation command vocabulary.
#[derive(Debug, PartialEq, Subcommand)]
pub(crate) enum SimCommand {
    /// The simulated clock.
    #[command(subcommand)]
    Time(Verb<TimeQuery, TimeMutate>),
    // TODO(#10209): add the rest of the vocabulary.
}
