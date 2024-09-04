// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Displayers for the update engine.
//!
//! Currently implemented are:
//!
//! * [`LineDisplay`]: a line-oriented display suitable for the command line.
//! * [`GroupDisplay`]: manages state and shows the results of several
//!   [`LineDisplay`]s at once.
//! * Some utility displayers which can be used to build custom displayers.

mod group_display;
mod line_display;
mod line_display_shared;
mod utils;

pub use group_display::GroupDisplay;
pub use line_display::{LineDisplay, LineDisplayStyles};
use line_display_shared::*;
pub use utils::*;
