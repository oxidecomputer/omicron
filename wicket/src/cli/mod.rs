// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Command-line interface to wicket.
//!
//! By default, the main interface to wicket is via a TUI. However, some
//! commands and use cases must be done via the CLI, and this module contains
//! support for that.

mod command;
mod inventory;
mod preflight;
mod rack_setup;
mod rack_update;
mod upload;

pub(super) use command::{CommandOutput, GlobalOpts, ShellApp};
