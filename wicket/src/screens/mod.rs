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

//
//----------------------------------------------------
//---- Old stuff -----
//----------------------------------------------------
//

// A mechanism for keeping track of user `tab` presses inside a screen.
// The index wraps around after `max` and `0`.
//
// Each screen maintains a mapping of TabIndex to the appropriate screen
// objects/widgets.
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Ord)]
pub struct TabIndex {
    current: Option<u16>,
    max: u16,
}

impl TabIndex {
    // Create an unset TabIndex
    pub fn new_unset(max: u16) -> TabIndex {
        assert!(max < u16::MAX);
        TabIndex { current: None, max }
    }

    // Create a TabIndex with a set value
    pub fn new(max: u16, val: u16) -> TabIndex {
        assert!(max < u16::MAX);
        assert!(val <= max);
        TabIndex { current: Some(val), max }
    }

    // Unset the current index
    pub fn clear(&mut self) {
        self.current = None;
    }

    // Return true if current tab index is set, false otherwise
    pub fn is_set(&self) -> bool {
        self.current.is_some()
    }

    // Set the current tab index
    pub fn set(&mut self, i: u16) {
        assert!(i <= self.max);
        self.current = Some(i);
    }

    // Get the next tab index
    pub fn next(&self) -> TabIndex {
        self.current.as_ref().map_or_else(
            || *self,
            |&i| {
                let current = if i == self.max { 0 } else { i + 1 };
                TabIndex { current: Some(current), max: self.max }
            },
        )
    }

    // Get the previous tab index
    pub fn prev(&self) -> TabIndex {
        self.current.as_ref().map_or_else(
            || *self,
            |&i| {
                let current = if i == 0 { self.max } else { i - 1 };
                TabIndex { current: Some(current), max: self.max }
            },
        )
    }

    // Increment the current value
    pub fn inc(&mut self) {
        let cur = self.current.get_or_insert(self.max);
        if *cur == self.max {
            *cur = 0;
        } else {
            *cur += 1;
        }
    }

    // Decrement the current value
    pub fn dec(&mut self) {
        let cur = self.current.get_or_insert(0);
        if *cur == 0 {
            *cur = self.max;
        } else {
            *cur -= 1;
        }
    }
}
