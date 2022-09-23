// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inventory [`Screen`]

use super::colors::*;
use super::Screen;
use crate::widgets::Banner;
use crate::Frame;
use crate::State;
use tui::layout::{Alignment, Constraint, Direction, Layout};
use tui::style::{Color, Modifier, Style};
use tui::text::Span;
use tui::widgets::{Block, Borders};

/// Show the rack inventory as learned from MGS
pub struct InventoryScreen {
    watermark: &'static str,
}

impl InventoryScreen {
    pub fn new() -> InventoryScreen {
        InventoryScreen { watermark: include_str!("../../banners/oxide.txt") }
    }

    fn draw_background(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_OFF_WHITE).bg(OX_GREEN_DARK);
        let block = Block::default()
            .style(style)
            .borders(Borders::NONE)
            .title("Inventory: Fuck yeah!")
            .title_alignment(Alignment::Center);

        f.render_widget(block, f.size());
    }

    fn draw_watermark(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_GREEN_LIGHT).bg(OX_GREEN_DARK);
        let banner = Banner::new(self.watermark).style(style);
        let height = banner.height();
        let width = banner.width();

        let mut rect = f.size();
        rect.x = rect.width - width - 1;
        rect.y = rect.height - height - 1;
        rect.width = width;
        rect.height = height;

        f.render_widget(banner, rect);
    }
}

impl Screen for InventoryScreen {
    fn draw(
        &self,
        state: &State,
        terminal: &mut crate::Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.draw_background(f);
            self.draw_watermark(f);
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
