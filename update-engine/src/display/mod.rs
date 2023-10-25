// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Displayers for the update engine.
//!
//! Currently implemented are:
//!
//! * [`LineDisplay`]: a line-oriented display suitable for the command line.
//! * [`GroupLineDisplay`]: manages state and shows the results of several
//!   [`LineDisplay`]s at once.

mod group_line_display;
mod line_display;

pub use group_line_display::GroupLineDisplay;
pub use line_display::{LineDisplay, LineDisplayStyles};
