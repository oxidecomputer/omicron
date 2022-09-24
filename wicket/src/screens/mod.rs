// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod inventory;

use crate::Action;
use crate::ScreenEvent;
use crate::State;
use crate::Term;

/// An identifier for a specific [`Screen`] in the [`Wizard`]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScreenId {
    Inventory,
    Update,
    RackInit,
}

pub struct Height(u16);
pub struct Width(u16);

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

pub use inventory::InventoryScreen;

/// All [`Screen`]s for wicket
pub struct Screens {
    inventory: InventoryScreen,
}

impl Screens {
    pub fn new() -> Screens {
        Screens { inventory: InventoryScreen::new() }
    }

    pub fn get(&self, id: ScreenId) -> &dyn Screen {
        match id {
            ScreenId::Inventory => &self.inventory,
            _ => unimplemented!(),
        }
    }

    pub fn get_mut(&mut self, id: ScreenId) -> &mut dyn Screen {
        match id {
            ScreenId::Inventory => &mut self.inventory,
            _ => unimplemented!(),
        }
    }
}

/// Oxide specific colors
/// Thanks JMC!
pub mod colors {
    use tui::style::Color;
    pub const OX_YELLOW: Color = Color::Rgb(0xF5, 0xCF, 0x75);
    pub const OX_OFF_WHITE: Color = Color::Rgb(0xE0, 0xE0, 0xE0);
    pub const OX_RED: Color = Color::Rgb(255, 145, 173);
    pub const OX_GREEN_LIGHT: Color = Color::Rgb(0x48, 0xD5, 0x97);
    pub const OX_GREEN_DARK: Color = Color::Rgb(0x11, 0x27, 0x25);
    pub const OX_GREEN_DARKEST: Color = Color::Rgb(0x0B, 0x14, 0x18);
}
