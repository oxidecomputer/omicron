// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod inventory;
mod rack;
mod splash;

use crate::Action;
use crate::ScreenEvent;
use crate::State;
use crate::Term;
use slog::Logger;
use tui::layout::Rect;

use rack::RackScreen;
use splash::SplashScreen;

/// An identifier for a specific [`Screen`] in the [`Wizard`]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScreenId {
    Splash,
    Rack,
    Component,
    Update,
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
    fn draw(
        &mut self,
        state: &State,
        terminal: &mut Term,
    ) -> anyhow::Result<()>;

    /// Handle a [`ScreenEvent`] to update internal display state and output
    /// any necessary actions for the system to take.
    fn on(&mut self, state: &State, event: ScreenEvent) -> Vec<Action>;
}

/// All [`Screen`]s for wicket
pub struct Screens {
    splash: SplashScreen,
    rack: RackScreen,
}

impl Screens {
    pub fn new(log: &Logger) -> Screens {
        Screens { splash: SplashScreen::new(), rack: RackScreen::new(log) }
    }

    pub fn get(&self, id: ScreenId) -> &dyn Screen {
        match id {
            ScreenId::Rack => &self.rack,
            _ => unimplemented!(),
        }
    }

    pub fn get_mut(&mut self, id: ScreenId) -> &mut dyn Screen {
        match id {
            ScreenId::Splash => &mut self.splash,
            ScreenId::Rack => &mut self.rack,
            _ => unimplemented!(),
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
            || self.clone(),
            |&i| {
                let current = if i == self.max { 0 } else { i + 1 };
                TabIndex { current: Some(current), max: self.max }
            },
        )
    }

    // Get the previous tab index
    pub fn prev(&self) -> TabIndex {
        self.current.as_ref().map_or_else(
            || self.clone(),
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

/// How a specific Rect should be displayed.
#[derive(Debug, Default, Clone)]
pub struct RectState {
    pub rect: Rect,

    // Whether the user has tabbed to the given Rect
    pub tabbed: bool,

    // Whether the mouse is hovering over the Rect
    pub hovered: bool,

    // Whether the user has clicked or hit enter on a tabbed Rect
    pub selected: bool,

    // If the Rect is currently accessible. It can become inactive if, for
    // example, a sled has not reported inventory yet.
    pub active: bool,
}

/// Oxide specific colors from the website
/// Thanks for the idea JMC!
pub mod colors {
    use tui::style::Color;
    pub const OX_YELLOW: Color = Color::Rgb(0xF5, 0xCF, 0x65);
    pub const OX_OFF_WHITE: Color = Color::Rgb(0xE0, 0xE0, 0xE0);
    pub const OX_RED: Color = Color::Rgb(255, 145, 173);
    pub const OX_GREEN_LIGHT: Color = Color::Rgb(0x48, 0xD5, 0x97);
    pub const OX_GREEN_DARK: Color = Color::Rgb(0x11, 0x27, 0x25);
    pub const OX_GREEN_DARKEST: Color = Color::Rgb(0x0B, 0x14, 0x18);
    pub const OX_GRAY: Color = Color::Rgb(0x9C, 0x9F, 0xA0);
    pub const OX_GRAY_DARK: Color = Color::Rgb(0x62, 0x66, 0x68);
    pub const OX_WHITE: Color = Color::Rgb(0xE7, 0xE7, 0xE8);
    pub const OX_PINK: Color = Color::Rgb(0xE6, 0x68, 0x86);
    pub const OX_YELLOW_DIM: Color = Color::Rgb(0xAE, 0x96, 0x4E);
}
