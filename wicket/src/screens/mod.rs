// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod controls;
mod main;
mod panes;
mod splash;

use crate::wizard::{Action, Event, State, Term};
use tui::widgets::ListState;

use main::MainScreen;
use splash::SplashScreen;

pub use controls::Control;
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

// Helper type to wrap a list
pub struct StatefulList<T> {
    pub state: ListState,
    pub items: Vec<T>,
}

impl<T> StatefulList<T> {
    pub fn new(items: Vec<T>) -> StatefulList<T> {
        StatefulList { state: ListState::default(), items }
    }

    pub fn next(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i >= self.items.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn previous(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i == 0 {
                    self.items.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }
}
