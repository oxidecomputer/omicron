// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod controls;
mod main;
mod panes;
mod splash;

use crate::{
    wizard::{Action, Event, State, Term},
    Frame,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tui::layout::Rect;

use main::MainScreen;
use splash::SplashScreen;

pub use controls::{get_control_id, Control, ControlId};
pub use panes::{NullPane, OverviewPane, Pane};

/// The primary display representation. It's sole purpose is to dispatch events
/// to the underlying splash and main screens.
///
// Note: It would be nice to use an enum here, but swapping between enum
// variants requires taking the screen by value or having a wrapper struct with
// an option so we can `take` the inner value. This is unergomic, so we just go
// with the simple solution.
pub struct Screen {
    splash: Option<SplashScreen>,
    main: MainScreen,
}

impl Screen {
    pub fn new() -> Screen {
        Screen { splash: Some(SplashScreen::new()), main: MainScreen::new() }
    }

    pub fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        if let Some(splash) = &mut self.splash {
            if splash.on(event) {
                self.splash = None;
            }
            Some(Action::Redraw)
        } else {
            self.main.on(state, event)
        }
    }

    pub fn draw(
        &mut self,
        state: &State,
        terminal: &mut Term,
    ) -> anyhow::Result<()> {
        if let Some(splash) = &self.splash {
            splash.draw(terminal)?;
        } else {
            self.main.draw(state, terminal)?;
        }
        Ok(())
    }
}
