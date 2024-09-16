// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::{keymap::Cmd, state::ComponentId, State};
use camino::Utf8PathBuf;
use humantime::format_rfc3339;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::time::{Duration, SystemTime};
use wicket_common::inventory::RackV1Inventory;
use wicket_common::update_events::EventReport;
use wicketd_client::types::{
    ArtifactId, CurrentRssUserConfig, GetLocationResponse, IgnitionCommand,
    RackOperationStatus, SemverVersion,
};

/// Event report type returned by the get_artifacts_and_event_reports API call.
pub type EventReportMap = HashMap<String, HashMap<String, EventReport>>;

/// An event that will update state
///
/// This can be a keypress, mouse event, or response from a downstream service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    /// An input event from the terminal translated to a Cmd
    Term(Cmd),

    /// An Inventory Update Event
    Inventory { inventory: RackV1Inventory, mgs_last_seen: Duration },

    /// TUF repo artifacts unpacked by wicketd, and event reports
    ArtifactsAndEventReports {
        system_version: Option<SemverVersion>,
        artifacts: Vec<(ArtifactId, Option<Vec<u8>>)>,
        event_reports: EventReportMap,
    },

    /// The current RSS configuration.
    RssConfig(CurrentRssUserConfig),

    /// The current state of rack initialization.
    RackSetupStatus(Result<RackOperationStatus, String>),

    /// The location within the rack where wicketd is running.
    WicketdLocation(GetLocationResponse),

    /// The tick of a Timer
    /// This can be used to draw a frame to the terminal
    Tick,

    /// A terminal resize event
    Resize { width: u16, height: u16 },

    /// ctrl-c was pressed
    Shutdown,
}

impl Event {
    pub fn is_tick(&self) -> bool {
        if let Event::Tick = self {
            true
        } else {
            false
        }
    }
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
    StartUpdate(ComponentId),
    AbortUpdate(ComponentId),
    ClearUpdateState(ComponentId),
    Ignition(ComponentId, IgnitionCommand),
    StartRackSetup,
    StartRackReset,
}

impl Action {
    /// Should the action result in a redraw
    ///
    /// For now, all actions result in a redraw.
    /// Some downstream operations will not trigger this in the future.
    pub fn should_redraw(&self) -> bool {
        match self {
            Action::Redraw
            | Action::StartUpdate(_)
            | Action::AbortUpdate(_)
            | Action::ClearUpdateState(_)
            | Action::Ignition(_, _)
            | Action::StartRackSetup
            | Action::StartRackReset => true,
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

    /// Dump the current snapshot to disk
    pub fn dump(&mut self) -> anyhow::Result<()> {
        let timestamp = format_rfc3339(SystemTime::now());
        let mut path: Utf8PathBuf = match std::env::var("WICKET_DUMP_PATH") {
            Ok(path) => path.into(),
            Err(std::env::VarError::NotPresent) => "/tmp/".into(),
            Err(std::env::VarError::NotUnicode(_)) => {
                anyhow::bail!("WICKET_DUMP_PATH is not valid utf8");
            }
        };
        path.push(format!("{}.wicket.dump", timestamp));
        let file = File::create(path)?;
        ciborium::ser::into_writer(&self.snapshot, file)?;
        Ok(())
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
            state: State::new(),
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
