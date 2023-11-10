// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The TUI library that is used via the technician port to initialize a rack
//! and perform disaster recovery.

use std::time::Duration;

mod cli;
mod dispatch;
mod events;
mod helpers;
mod keymap;
mod runner;
mod state;
mod ui;
mod wicketd;

pub const TICK_INTERVAL: Duration = Duration::from_millis(30);

pub use crate::dispatch::*;
pub use crate::runner::*;
pub use events::{Action, Event, Recorder, Snapshot};
pub use keymap::{Cmd, KeyHandler};
pub use state::State;
pub use ui::{Control, Screen};
