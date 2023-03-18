// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::{keymap::Cmd, state::ComponentId, State};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use wicketd_client::types::{
    ArtifactId, RackV1Inventory, SemverVersion, UpdateLogAll,
};

/// An event that will update state
///
/// This can be a keypress, mouse event, or response from a downstream service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    /// An input event from the terminal translated to a Cmd
    Term(Cmd),

    /// An Inventory Update Event
    Inventory { inventory: RackV1Inventory, mgs_last_seen: Duration },

    /// Update Log Event
    UpdateLog(UpdateLogAll),

    /// TUF repo artifacts unpacked by wicketd
    UpdateArtifacts {
        system_version: Option<SemverVersion>,
        artifacts: Vec<ArtifactId>,
    },

    /// The tick of a Timer
    /// This can be used to draw a frame to the terminal
    Tick,

    /// A terminal resize event
    Resize { width: u16, height: u16 },

    /// ctrl-c was pressed
    Shutdown,
}

/// An event that can be recorded.

/// Instructions for the [`crate::Runner`]
///
/// Event's fed through the [`crate::Control::on`] methods return an [`Action`]
/// informing the [`crate::Runner`] that it needs to do something. This allows
/// separation of the UI from the rest of the system and makes each more
/// testable.
pub enum Action {
    Redraw,
    Update(ComponentId),
}

impl Action {
    /// Should the action result in a redraw
    ///
    /// For now, all actions result in a redraw.
    /// Some downstream operations will not trigger this in the future.
    pub fn should_redraw(&self) -> bool {
        match self {
            Action::Redraw | Action::Update(_) => true,
        }
    }
}

/// A recorder for [`Event`]s to allow playback and debugging later.
///
/// The recorder maintains a copy of state, and a log of events called the
/// `history`. The number of events can be restricted to limit memory usage. A
/// user can only `play` as many `frames` of a recording  of history as there
/// are events.
///
/// Once the log is full of events, the recorder automatically takes an in-
/// memory "snapshot" by saving the current state, clearing the log, and
/// appending the event that did not fit before.
pub struct Recorder {
    snapshot: Snapshot,
}

impl Recorder {
    pub fn new(max_events: usize) -> Self {
        Recorder { snapshot: Snapshot::new(max_events) }
    }

    /// Append an event to the recorder.
    ///
    /// Return true if the in-memory snapshot was taken, false if not.
    pub fn push(&mut self, state: &State, event: Event) -> bool {
        if self.snapshot.history_full() {
            self.snapshot.take(state, event);
            return false;
        }
        self.snapshot.history.push(event);
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: usize,
    pub start: usize,
    pub state: State,
    pub history: Vec<Event>,
    pub max_events: usize,
}

impl Snapshot {
    fn new(max_events: usize) -> Self {
        Snapshot {
            version: 1,
            start: 0,
            state: State::new(&slog::Logger::root(slog::Discard, slog::o!())),
            history: Vec::with_capacity(max_events),
            max_events,
        }
    }

    fn history_full(&self) -> bool {
        self.max_events == self.history.len()
    }

    fn take(&mut self, state: &State, event: Event) {
        self.start += self.history.len();
        self.state = state.clone();
        self.history.clear();
        self.history.push(event);
    }
}
