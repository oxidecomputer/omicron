// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The `state` noun: the tree of every state the session has produced.

use clap::Subcommand;
use renderdag::{Ancestor, GraphRowRenderer, Renderer};

use crate::{Error, SimStateId, SimStateOrigin, Simulator};

/// The glyph for the current state's row.
const CURRENT: &str = "@";

/// The glyph for a row other than the current one.
const OTHER: &str = "\u{25cb}";

/// `state` verbs.
#[derive(Debug, PartialEq, Subcommand)]
pub(crate) enum StateCmd {
    /// Show the tree of states.
    Log,
    /// Make a state current.
    Switch {
        /// The requested state's id.
        state: SimStateId,
    },
}

/// Runs a `state` command.
pub(crate) fn run(sim: &mut Simulator, cmd: StateCmd) -> Result<String, Error> {
    match cmd {
        StateCmd::Log => Ok(log(sim)),
        StateCmd::Switch { state } => {
            sim.switch(state)?;
            Ok(format!("switched to state {state}"))
        }
    }
}

/// `state log`: renders the tree of states as a string, newest first, with `@`
/// marking the current state.
fn log(sim: &Simulator) -> String {
    let current = sim.current_id();
    let mut renderer = GraphRowRenderer::new().output().build_box_drawing();
    let mut log = String::new();

    for (id, node) in sim.nodes().rev() {
        let (parents, description) = match &node.origin {
            SimStateOrigin::Root => (Vec::new(), "(root)"),
            SimStateOrigin::Command { parent, line } => {
                (vec![Ancestor::Parent(*parent)], line.as_str())
            }
        };
        let glyph = if id == current { CURRENT } else { OTHER };
        let message = format!("{id}  {description}");
        log.push_str(&renderer.next_row(id, parents, glyph.into(), message));
    }

    log.trim_end().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CliCommand;
    use crate::session::SessionCommand;
    use crate::test_utils::{parse_err, parse_ok, run_ok};

    #[test]
    fn state_commands_parse() {
        let state = |cmd| CliCommand::Session(SessionCommand::State(cmd));
        let log = state(StateCmd::Log);
        let switch =
            |id| state(StateCmd::Switch { state: SimStateId::new(id) });

        assert_eq!(parse_ok("state log"), log);
        assert_eq!(parse_ok("state switch 0"), switch(0));
        assert_eq!(parse_ok("state switch 3"), switch(3));

        parse_err(&[
            "state",
            "state switch",
            "state switch latest",
            "state switch -1",
            "state log extra",
            "state your business",
        ]);
    }

    #[test]
    fn switching_to_the_current_state_succeeds() {
        let mut sim = Simulator::new();
        let root = sim.current_id();

        run_ok(&mut sim, &format!("state switch {root}"));
        assert_eq!(sim.current_id(), root);
    }
}
