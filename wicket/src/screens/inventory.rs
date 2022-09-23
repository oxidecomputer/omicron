// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inventory [`Screen`]

use super::Screen;
use crate::State;
use tui::layout::{Alignment, Constraint, Direction, Layout};
use tui::style::{Color, Modifier, Style};
use tui::text::Span;
use tui::widgets::{Block, Borders};

/// Show the rack inventory as learned from MGS
pub struct InventoryScreen {}

impl InventoryScreen {
    pub fn new() -> InventoryScreen {
        InventoryScreen {}
    }
}

impl Screen for InventoryScreen {
    fn draw(
        &self,
        state: &State,
        terminal: &mut crate::Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            let block = Block::default()
                .borders(Borders::NONE)
                .title("Inventory: Fuck yeah!")
                .title_alignment(Alignment::Center);

            f.render_widget(block, f.size());
        })?;
        Ok(())
    }

    fn on(
        &mut self,
        state: &State,
        event: crate::ScreenEvent,
    ) -> Vec<crate::Action> {
        unimplemented!()
    }
}
