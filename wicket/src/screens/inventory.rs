// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inventory [`Screen`]

use super::colors::*;
use super::Screen;
use crate::widgets::Banner;
use crate::Action;
use crate::Frame;
use crate::State;
use tui::layout::{Alignment, Constraint, Direction, Layout};
use tui::style::{Color, Modifier, Style};
use tui::text::Span;
use tui::widgets::{Block, Borders};

/// Show the rack inventory as learned from MGS
pub struct InventoryScreen {
    count: u64,
    watermark: &'static str,
}

impl InventoryScreen {
    pub fn new() -> InventoryScreen {
        InventoryScreen {
            count: 0,
            watermark: include_str!("../../banners/oxide.txt"),
        }
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

        // Position the watermark in the lower right hand corner of the screen
        let mut rect = f.size();
        if width >= rect.width || height >= rect.height {
            // The banner won't fit.
            return;
        }
        rect.x = rect.width - width - 1;
        rect.y = rect.height - height - 1;
        rect.width = width;
        rect.height = height;

        f.render_widget(banner, rect);
    }

    fn draw_center_block(&self, f: &mut Frame) {
        if self.count % 2 == 0 {
            return;
        }
        let mut rect = f.size();
        rect.x = rect.width / 2 - 5;
        rect.y = rect.height / 2 - 5;
        rect.width = 10;
        rect.height = 10;

        let style = Style::default().bg(OX_OFF_WHITE);
        let block = Block::default().style(style);
        f.render_widget(block, rect);
    }
}

impl Screen for InventoryScreen {
    fn draw(
        &mut self,
        state: &State,
        terminal: &mut crate::Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.draw_background(f);
            self.draw_watermark(f);
            self.draw_center_block(f);
        })?;
        Ok(())
    }

    fn on(&mut self, state: &State, event: crate::ScreenEvent) -> Vec<Action> {
        self.count += 1;
        vec![Action::Redraw]
    }
}
