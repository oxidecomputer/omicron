// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod inventory;

use tui::backend::Backend;
use tui::Frame;

use crate::Action;
use crate::ScreenEvent;

pub trait Screen {
    /// Draw the [`Screen`]
    fn draw<'a, B: Backend>(&self, f: &mut Frame<'a, B>);

    /// Handle a [`ScreenEvent`] to update internal display state and output
    /// any necessary actions for the system to take.
    fn on(&mut self, event: ScreenEvent) -> Vec<Action>;
}
