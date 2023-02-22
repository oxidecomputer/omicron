// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod main;
mod splash;

use crate::wizard::{Action, Event, State, Term};
use std::sync::atomic::{AtomicUsize, Ordering};

use main::MainScreen;
use splash::SplashScreen;

/// A specific functionality such as `Update` or `Recovery` that is selectable
/// from the [`MainScreen`] navbar on the left.
///
/// Individual [`View`]s representing a subset of functionlity can be selected
/// indside a pane using the navbar at the top of the pane.
pub struct Pane {}

/// The viewing area of a specific [`Screen`], where functionally specific
/// widgets are rendered. [`View`]s contain [`Control`]s that represent the pre-
/// rendered state of a widget. Controls may be focused and "stacked", such
/// that the user can contextually deal with a single control at a time.
pub struct View {
    selected: ControlId,
}

/// A unique id for a [`Control`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ControlId(pub usize);

/// Return a unique id for a [`Control`]
pub fn get_control_id() -> ControlId {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    ControlId(COUNTER.fetch_add(1, Ordering::Relaxed))
}

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
        &self,
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

#[derive(Debug, Clone, Copy)]
pub struct Height(pub u16);

#[derive(Debug, Clone, Copy)]
pub struct Width(pub u16);

/// Ensure that a u16 is an even number by adding 1 if necessary.
pub fn make_even(val: u16) -> u16 {
    if val % 2 == 0 {
        val
    } else {
        val + 1
    }
}

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
