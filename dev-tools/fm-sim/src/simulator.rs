// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The [`Simulator`] comprises a session's state history, with the
//! corresponding commands that produced each state.

use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

use clap::Subcommand;
use nexus_fm_simulation::{SimState, SimStateBuilder};

use crate::command::{Mutate, Query, SimCommand, Verb};
use crate::session::{SessionCommand, state};
use crate::{CliCommand, Error, SessionError, parse};

/// Uniquely identifies a [`SimStateNode`] within a [`Simulator`]'s history
/// tree.
///
/// States are numbered in creation order; the initial state is 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimStateId(usize);

impl SimStateId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

impl fmt::Display for SimStateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SimStateId {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

/// A state snapshot in the session's history.
#[derive(Debug)]
pub struct SimStateNode {
    /// The state snapshot itself.
    pub state: SimState,
    /// What produced this state.
    pub origin: SimStateOrigin,
    /// The query commands run while this state was current, in order. These are
    /// stored as strings rather than as parsed command objects, to be able to
    /// replay them verbatim.
    pub queries: Vec<String>,
}

/// Where a state came from.
#[derive(Debug, PartialEq, Eq)]
pub enum SimStateOrigin {
    /// The initial state, before any command has run.
    Root,
    /// The state was produced by a mutation command on some parent state.
    Command {
        /// The state the mutation ran against.
        parent: SimStateId,
        /// The mutation command, stored as a string rather than as a parsed
        /// command object, to be able to replay it verbatim.
        line: String,
    },
}

/// Holds every state of the simulated world the session has produced, and
/// parses and executes commands against the current state.
#[derive(Debug)]
pub struct Simulator {
    /// Every state the session has produced, in creation order; the first is
    /// the root.
    states: Vec<SimStateNode>,
    /// The current state that all commands run against. This isn't necessarily
    /// the last entry in `states`, since we can navigate history with
    /// `state switch`.
    //
    // TODO(#10209): This is a plain scalar id for now; replace it with a proper
    // op log for undo/redo of state changes.
    current: SimStateId,
}

impl Simulator {
    /// Constructs a simulator starting from the initial state.
    pub fn new() -> Self {
        let root = SimStateNode {
            state: SimState::new(),
            origin: SimStateOrigin::Root,
            queries: Vec::new(),
        };
        Self { states: vec![root], current: SimStateId(0) }
    }

    /// The id of the current state.
    pub fn current_id(&self) -> SimStateId {
        self.current
    }

    /// The current state.
    pub fn current(&self) -> &SimState {
        &self.states[self.current.0].state
    }

    /// The node containing the state with the given id.
    pub fn node(&self, id: SimStateId) -> Result<&SimStateNode, SessionError> {
        self.states.get(id.0).ok_or(SessionError::NoSuchState(id))
    }

    /// Every node and its id, in creation order.
    pub fn nodes(
        &self,
    ) -> impl DoubleEndedIterator<Item = (SimStateId, &SimStateNode)>
    + ExactSizeIterator {
        self.states
            .iter()
            .enumerate()
            .map(|(index, node)| (SimStateId(index), node))
    }

    /// Parses `line` as a command and executes it.
    pub fn run(&mut self, line: &str) -> Result<String, Error> {
        let line = line.trim();
        match parse(line)? {
            CliCommand::Sim(SimCommand::Time(verb)) => self.verb(line, verb),
            CliCommand::Session(SessionCommand::State(cmd)) => {
                state::run(self, cmd)
            }
        }
    }

    /// Runs one verb command against the current state.
    ///
    /// A query is recorded on the current state and changes nothing else. A
    /// mutation adds the state it produced as a child of the current state,
    /// recording `line` as that state's origin, and makes it current. A
    /// failed verb is recorded nowhere and changes nothing.
    fn verb<Q, M>(
        &mut self,
        line: &str,
        verb: Verb<Q, M>,
    ) -> Result<String, Error>
    where
        Q: Subcommand + Query,
        M: Subcommand + Mutate,
    {
        match verb {
            Verb::Query(query) => {
                let output = query.run(self.current())?;
                // Record the query command on the current state.
                let current = self.current_id();
                self.states[current.0].queries.push(line.to_owned());
                Ok(output)
            }
            Verb::Mutate(mutate) => {
                let mut builder = SimStateBuilder::from_state(self.current());
                let output = mutate.apply(&mut builder)?;
                // Record the mutation command as the origin for the new state.
                let origin = SimStateOrigin::Command {
                    parent: self.current_id(),
                    line: line.to_owned(),
                };
                self.states.push(SimStateNode {
                    state: builder.build(),
                    origin,
                    queries: Vec::new(),
                });
                self.current = SimStateId(self.states.len() - 1);
                Ok(output)
            }
        }
    }

    /// Makes `state` the current state.
    ///
    /// Returns [`SessionError::NoSuchState`] and leaves the current state
    /// unchanged if the session has no state with the given id.
    pub fn switch(&mut self, state: SimStateId) -> Result<(), SessionError> {
        // Validate that the state exists first.
        let _ = self.node(state)?;
        self.current = state;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use nexus_fm_simulation::Error as SimError;
    use proptest::prelude::*;
    use test_strategy::proptest;

    use super::*;
    use crate::test_utils::{run_err, run_ok};

    // Note, we use `time` commands in these tests as our canonical example for
    // testing the simulator's behavior, although nothing in the tests is really
    // about simulated time per se. The `time` noun just happens to be minimal
    // and convenient, with one query and one mutation that can fail.

    /// Each state's origin, in creation order.
    fn origins(sim: &Simulator) -> Vec<&SimStateOrigin> {
        sim.nodes().map(|(_, node)| &node.origin).collect()
    }

    /// Constructs a dummy `SimStateOrigin` for some mutation command.
    fn command(parent: usize, line: &str) -> SimStateOrigin {
        SimStateOrigin::Command {
            parent: SimStateId(parent),
            line: line.to_owned(),
        }
    }

    #[proptest]
    fn state_id_round_trips(id: usize) {
        let id = SimStateId(id);
        let parsed: SimStateId =
            id.to_string().parse().expect("a displayed id parses");
        prop_assert_eq!(parsed, id);
    }

    #[test]
    fn new_simulator_has_only_root() {
        let sim = Simulator::new();
        assert_eq!(origins(&sim), [&SimStateOrigin::Root]);
        assert_eq!(sim.current(), &SimState::new());
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
    fn mutate_adds_state_and_makes_it_current() {
        let mut sim = Simulator::new();
        run_ok(&mut sim, "time advance 1m");

        assert_eq!(sim.nodes().len(), 2);
        assert_eq!(sim.current_id(), SimStateId(1));
        assert_ne!(sim.current(), &SimState::new());
    }

    #[test]
    fn mutate_records_line_as_origin() {
        let mut sim = Simulator::new();
        run_ok(&mut sim, "time advance 1m");
        run_ok(&mut sim, "  time advance '2m'\n");

        // The line is recorded as typed, without surrounding whitespace.
        assert_eq!(
            origins(&sim),
            [
                &SimStateOrigin::Root,
                &command(0, "time advance 1m"),
                &command(1, "time advance '2m'"),
            ]
        );
    }

    #[test]
    fn mutate_keeps_earlier_states() {
        let mut sim = Simulator::new();
        run_ok(&mut sim, "time advance 1m");

        assert_eq!(sim.states[0].state, SimState::new());
    }

    #[test]
    fn node_looks_up_state_by_id() {
        let mut sim = Simulator::new();
        run_ok(&mut sim, "time advance 1m");

        let node = sim.node(SimStateId(1)).expect("the mutation added it");
        assert_eq!(node.origin, command(0, "time advance 1m"));
        assert_matches!(
            sim.node(SimStateId(2)),
            Err(SessionError::NoSuchState(SimStateId(2)))
        );
    }

    #[test]
    fn query_records_line_on_current_state() {
        let mut sim = Simulator::new();
        run_ok(&mut sim, "time show");
        run_ok(&mut sim, "time advance 1m");
        run_ok(&mut sim, "time  show");
        run_ok(&mut sim, "time show");

        // Session commands are not queries.
        run_ok(&mut sim, "state log");

        // The line is recorded as typed, on the state it ran against. That
        // is the only trace: no state is added, and the current one stays.
        assert_eq!(sim.states[0].queries, ["time show"]);
        assert_eq!(sim.states[1].queries, ["time  show", "time show"]);
        assert_eq!(sim.nodes().len(), 2);
        assert_eq!(sim.current_id(), SimStateId(1));
    }

    #[test]
    fn errors_leave_no_trace() {
        let mut sim = Simulator::new();
        run_ok(&mut sim, "time advance 1m");

        let error = run_err(&mut sim, "time advance 1000000years");
        assert_matches!(error, Error::Sim(SimError::TimeOutOfRange));
        let error = run_err(&mut sim, "state switch 2");
        assert_matches!(
            error,
            Error::Session(SessionError::NoSuchState(SimStateId(2)))
        );

        assert_eq!(sim.nodes().len(), 2);
        assert!(sim.nodes().all(|(_, node)| node.queries.is_empty()));
        assert_eq!(sim.current_id(), SimStateId(1));
    }

    #[test]
    fn switch_makes_state_current() {
        let mut sim = Simulator::new();
        run_ok(&mut sim, "time advance 1m");
        run_ok(&mut sim, "time advance 2m");

        let output = run_ok(&mut sim, "state switch 1");
        assert_eq!(output, "switched to state 1");
        assert_eq!(sim.current_id(), SimStateId(1));
    }

    #[test]
    fn mutate_after_switch_grows_branch() {
        let mut sim = Simulator::new();
        run_ok(&mut sim, "time advance 1m");
        run_ok(&mut sim, "state switch 0");
        run_ok(&mut sim, "time advance 2m");

        // States 1 and 2 are both children of the root.
        assert_eq!(
            origins(&sim),
            [
                &SimStateOrigin::Root,
                &command(0, "time advance 1m"),
                &command(0, "time advance 2m"),
            ]
        );
    }

    #[test]
    fn parse_errors_arent_panics() {
        let mut sim = Simulator::new();
        assert_matches!(sim.run(""), Err(Error::Parse(_)));
        assert_matches!(sim.run("   "), Err(Error::Parse(_)));
        assert_matches!(sim.run("time advance soon"), Err(Error::Parse(_)));
        assert_matches!(sim.run("time advance '1h"), Err(Error::Split(_)));
        assert_eq!(sim.nodes().len(), 1);
    }
}
