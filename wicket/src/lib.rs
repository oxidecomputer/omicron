// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The library that is used via the technician port to initialize a rack
//! and perform disaster recovery.
//!
//! This interface is a text user interface (TUI) based wizard
//! that will guide the user through the steps the need to take
//! in an intuitive manner.

pub(crate) mod defaults;
mod dispatch;
pub(crate) mod inventory;
mod screens;
mod upload;
mod wicketd;
mod widgets;
mod wizard;

pub use crate::dispatch::*;
pub use crate::wizard::*;
