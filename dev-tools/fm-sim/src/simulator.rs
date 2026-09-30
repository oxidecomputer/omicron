// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::Subcommand;
use nexus_fm_simulation::{SimState, SimStateBuilder};

use crate::command::{Mutate, Query, SimCommand, Verb};
use crate::{CliCommand, Error, parse};

/// Holds the current state of the simulated world, and parses and executes
/// commands (queries and mutations) over that state.
#[derive(Debug)]
pub struct Simulator {
    state: SimState,
    // TODO(#10209): Add session history and op log, so we're not just storing
    // the current state.
}

impl Simulator {
    /// Constructs a simulator starting from the initial empty state.
    pub fn new() -> Self {
        Self { state: SimState::new() }
    }

    /// Returns the current state.
    pub fn current(&self) -> &SimState {
        &self.state
    }

    /// Parses `line` as a command and executes it.
    pub fn run(&mut self, line: &str) -> Result<String, Error> {
        match parse(line)? {
            CliCommand::Sim(SimCommand::Time(verb)) => self.verb(verb),
        }
    }

    /// Runs one verb command against the current state.
    ///
    /// A query leaves the state as it was. A successful mutation replaces the
    /// current state with the new successor state the mutation produced.
    fn verb<Q, M>(&mut self, verb: Verb<Q, M>) -> Result<String, Error>
    where
        Q: Subcommand + Query,
        M: Subcommand + Mutate,
    {
        match verb {
            // TODO(#10209): When we implement history, record query commands
            // on the current state node (the state the query ran against).
            Verb::Query(query) => query.run(&self.state),
            // TODO(#10209): When we implement history, record each mutation
            // command on the result state node that the mutation produced.
            Verb::Mutate(mutate) => {
                let mut builder = SimStateBuilder::from_state(&self.state);
                let output = mutate.apply(&mut builder)?;
                self.state = builder.build();
                Ok(output)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use nexus_fm_simulation::Error as SimError;

    use super::*;

    /// We use simulated time (how many minutes past the epoch) as a sort of
    /// proxy in the tests below for observing state changes, although nothing
    /// in the tests is really about simulated time. The `time` noun just
    /// happens to be minimal and convenient, with one query and one mutation
    /// that can fail.
    fn minutes(sim: &Simulator) -> i64 {
        (sim.current().now() - SimState::EPOCH).num_minutes()
    }

    #[test]
    fn query_returns_command_output() {
        let output = Simulator::new().run("time show").unwrap();
        assert_eq!(output, "1970-01-01T00:00:00Z");
    }

    #[test]
    fn mutate_returns_command_output() {
        let output = Simulator::new().run("time advance 1m").unwrap();
        assert_eq!(output, "advanced time by 1m to 1970-01-01T00:01:00Z");
    }

    #[test]
    fn mutate_replaces_current_state() {
        let mut sim = Simulator::new();
        assert_eq!(minutes(&sim), 0);

        sim.run("time advance 1m").unwrap();
        assert_eq!(minutes(&sim), 1);
    }

    #[test]
    fn mutate_error_preserves_state() {
        let mut sim = Simulator::new();
        let error = sim
            .run("time advance 1000000years")
            .expect_err("a million years is past the end of the clock");
        assert_matches!(error, Error::Sim(SimError::TimeOutOfRange));
        assert_eq!(minutes(&sim), 0);
    }

    #[test]
    fn parse_errors_arent_panics() {
        let mut sim = Simulator::new();
        assert_matches!(sim.run(""), Err(Error::Parse(_)));
        assert_matches!(sim.run("   "), Err(Error::Parse(_)));
        assert_matches!(sim.run("time advance soon"), Err(Error::Parse(_)));
        assert_matches!(sim.run("time advance '1h"), Err(Error::Split(_)));
        assert_eq!(minutes(&sim), 0);
    }
}
