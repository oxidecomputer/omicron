// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A library capable of playing and inspecting wicket recordings

mod client;
mod runner;

use std::{collections::BTreeSet, time::Duration};

pub use client::Client;
pub use runner::{Runner, RunnerHandle};

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use wicket::State;

/// All commands capable of operating the debugger
///
/// Commands are issued by the user via wicket-dbg and sent
/// to wicket-dbg-server so that they can be run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cmd {
    /// Load a recording in the wicket-dbg-server
    Load(Utf8PathBuf),

    /// Restart at the beginning of the recording
    Reset,

    /// Play a recording at a given speed.
    ///
    /// Speed is changed by manipulating the interval between ticks for a
    /// wicket recording.
    ///
    /// `speedup` and `slowdown` are mutually exclusive factors to the rate
    /// of play.
    Run { speedup: Option<u32>, slowdown: Option<u32> },

    /// Issue a breakpoint for the given event
    Break { event: u32 },

    /// Execute the next event, including ticks
    Step,

    /// Jump to the next event which is not a `Tick`, and execute it.
    Jump,

    /// Retrieve the current `State` of wicket
    GetState,

    /// Pause a running playback and return the current frame
    Pause,

    /// Resume running playback
    Resume,

    /// Return the current event index
    GetEvent,

    /// Return information about the currently loaded recording
    DebugState,
}

/// Replies for Commands
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Rpy {
    Ok,
    Err(String),
    State(Box<State>),
    Event(usize),
    Debug(DebugState),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugState {
    snapshot_path: Option<Utf8PathBuf>,
    snapshot_start_event: Option<usize>,
    total_events: Option<usize>,
    current_event: usize,
    breakpoints: BTreeSet<u32>,
    is_playing: bool,
    playback_speedup: Option<u32>,
    playback_slowdown: Option<u32>,
    tick_interval: Duration,
}

impl Rpy {
    fn is_err(&self) -> bool {
        if let Rpy::Err(_) = self { true } else { false }
    }
}
