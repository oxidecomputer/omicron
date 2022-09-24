// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inventory [`Screen`]

use super::colors::*;
use super::Screen;
use super::{Height, Width};
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

    fn draw_watermark(&self, f: &mut Frame) -> (Height, Width) {
        let style = Style::default().fg(OX_GREEN_DARKEST).bg(OX_GREEN_DARK);
        let banner = Banner::new(self.watermark).style(style);
        let height = banner.height();
        let width = banner.width();

        // Position the watermark in the lower right hand corner of the screen
        let mut rect = f.size();
        if width >= rect.width || height >= rect.height {
            // The banner won't fit.
            return (Height(1), Width(1));
        }
        rect.x = rect.width - width - 1;
        rect.y = rect.height - height - 1;
        rect.width = width;
        rect.height = height;

        f.render_widget(banner, rect);

        (Height(height), Width(width))
    }

    /// Draw the rack in the center of the screen.
    /// Scale it to look nice.
    fn draw_rack(&self, f: &mut Frame, border: Height) {
        if self.count % 2 == 0 {
            return;
        }

        let mut rect = f.size();
        let width = rect.width;

        // Scale proportionally and center
        rect.y = border.0 - 1;
        rect.height = rect.height - (border.0 * 2) - 2;
        rect.width = rect.height * 2 / 3;
        rect.x = width / 2 - rect.width / 2;

        // Divide the rack into three vertical sections:
        //  * Top sleds
        //  * Switches
        //  * Bottom sleds
        let divs = Layout::default()
            .direction(Direction::Vertical)
            .constraints(&[
                Constraint::Percentage(45),
                Constraint::Percentage(10),
                Constraint::Percentage(45),
            ])
            .split(rect);

        let sleds = Block::default().style(Style::default().bg(OX_GREEN_LIGHT));
        let switches =
            Block::default().style(Style::default().bg(OX_OFF_WHITE));

        f.render_widget(sleds.clone(), divs[0]);
        f.render_widget(switches, divs[1]);
        f.render_widget(sleds, divs[2]);
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
            let (height, _) = self.draw_watermark(f);
            self.draw_rack(f, height);
        })?;
        Ok(())
    }

    fn on(&mut self, state: &State, event: crate::ScreenEvent) -> Vec<Action> {
        self.count += 1;
        vec![Action::Redraw]
    }
}
