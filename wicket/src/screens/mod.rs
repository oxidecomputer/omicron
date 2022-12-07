// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod component;
mod rack;
mod splash;

use crate::Action;
use crate::ScreenEvent;
use crate::State;
use crate::Term;
use crate::TermEvent;
use slog::Logger;

use component::ComponentScreen;
use rack::RackScreen;
use splash::SplashScreen;

/// An identifier for a specific [`Screen`] in the [`Wizard`](crate::Wizard).
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScreenId {
    Splash,
    Rack,
    Component,
}

impl ScreenId {
    pub fn name(&self) -> &'static str {
        match self {
            ScreenId::Splash => "splash",
            ScreenId::Rack => "rack",
            ScreenId::Component => "component",
        }
    }

    /// Width of the maximum string length of the name
    pub fn width() -> u16 {
        9
    }
}

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

pub trait Screen {
    /// Draw the [`Screen`]
    fn draw(&self, state: &State, terminal: &mut Term) -> anyhow::Result<()>;

    /// Handle a [`ScreenEvent`] to update internal display state and output
    /// any necessary actions for the system to take.
    fn on(&mut self, state: &mut State, event: ScreenEvent) -> Vec<Action>;

    /// Resize the screen via delivering a `Resize` event
    fn resize(
        &mut self,
        state: &mut State,
        width: u16,
        height: u16,
    ) -> Vec<Action> {
        self.on(state, ScreenEvent::Term(TermEvent::Resize(width, height)))
    }
}

/// All [`Screen`]s for wicket
pub struct Screens {
    splash: SplashScreen,
    rack: RackScreen,
    component: ComponentScreen,
}

impl Screens {
    pub fn new(log: &Logger) -> Screens {
        Screens {
            splash: SplashScreen::new(),
            rack: RackScreen::new(log),
            component: ComponentScreen::new(),
        }
    }

    pub fn get_mut(&mut self, id: ScreenId) -> &mut dyn Screen {
        match id {
            ScreenId::Splash => &mut self.splash,
            ScreenId::Rack => &mut self.rack,
            ScreenId::Component => &mut self.component,
        }
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
