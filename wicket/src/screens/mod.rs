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

pub trait Screen {
    /// Draw the [`Screen`]
    fn draw(&self, state: &State, terminal: &mut Term) -> anyhow::Result<()>;

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

    pub fn get_mut(&mut self, id: ScreenId) -> &dyn Screen {
        match id {
            ScreenId::Inventory => &mut self.inventory,
            _ => unimplemented!(),
        }
    }
}
