// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A library capable of playing and inspecting wicket recordings

mod client;
mod server;

pub use client::Client;
pub use server::Server;

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// All commands capable of operating the debugger
///
/// Commands are issued by the user via wicket-dbg and sent
/// to wicket-dbg-server so that they can be run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cmd {
    /// Load a recording in the wicket-dbg-server
    Load(PathBuf),

    /// Restart at the beginning of the recording
    Reset,

    /// Play a recording at a given speed.
    ///
    /// Speed is changed by manipulating the interval between ticks for a
    /// wicket recording.
    ///
    /// `speedup` and `slowdown` are mutually exclusive factors to the rate
    /// of play.
    Run { speedup: Option<u8>, slowdown: Option<u8> },

    /// Issue a breakpoint for the given event
    Break { event: u32 },

    /// Step over `n` events sequentially, including `Tick`s.
    Step { n: u32 },

    /// Jump forward `n` non-`Tick` events.
    Jump { n: u32 },

    /// Retrieve the current `State` of wicket
    GetState,
}
