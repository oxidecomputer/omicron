// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inventory [`Screen`]

use super::colors::*;
use super::make_even;
use super::Screen;
use super::{Height, Width};
use crate::widgets::Banner;
use crate::Action;
use crate::Frame;
use crate::State;
use slog::info;
use slog::Logger;
use tui::layout::Rect;
use tui::layout::{Alignment, Constraint, Direction, Layout};
use tui::style::{Color, Modifier, Style};
use tui::text::Span;
use tui::widgets::{Block, BorderType, Borders};

// This is the visual state of the screen. It's used both to perform stateful
// renders as well as provide a source for rectangle / point intersection.
struct DisplayState {
    rack: RackState,
}

/// How a specific Rect should be displayed.
#[derive(Debug, Default)]
struct RectState {
    rect: Rect,
    // Whether the mouse is hovering over the Rect
    hovered: bool,
    // Whether the tab is currently on this Rect
    tabbed: bool,

    // Whether the user has clicked or hit enter on a tabbed Rect
    selected: bool,

    // If the Rect is currently accessible. It can become inactive if, for
    // example, a sled has not reported inventory yet.
    active: bool,
}

// The visual state of the rack
#[derive(Debug, Default)]
struct RackState {
    rect: Rect,
    sleds: [RectState; 32],
    power_shelves: [RectState; 2],
    switches: [RectState; 2],
}

impl RackState {
    fn new(rect: &Rect, watermark_height: &Height) -> RackState {
        let mut rack = RackState::default();
        rack.resize(rect, watermark_height);
        rack
    }

    // Split the rect into 20 vertical chunks. 1 for each sled bay, 1 per
    // switch, 1 per power shelf.
    // Divide each rack chunk into two horizontal chunks, one per sled.
    fn resize(&mut self, rect: &Rect, watermark_height: &Height) {
        let width = rect.width;
        let max_height = rect.height;
        let mut rack = rect.clone();

        // Scale proportionally and center the rack horizontally
        rack.height = make_even(rack.height - (watermark_height.0 * 2) - 2);
        rack.width = make_even(rack.height * 2 / 3);
        rack.x = width / 2 - rack.width / 2;

        // Make the max_height divisible by 20
        let actual_height = rack.height / 20 * 20;
        rack.height = actual_height;

        // Center the rack vertically
        rack.y = (max_height - actual_height) / 2 - 1;

        self.rect = rack.clone();

        let sled_height = rack.height / 20;
        let sled_width = rack.width / 2;

        // Top Sleds
        for i in 0..16 {
            self.size_sled(i, &rack, sled_height, sled_width);
        }

        // Top Switch
        let switch = &mut self.switches[0];
        switch.rect.y = rack.y + sled_height * 8;
        switch.rect.x = rack.x;
        switch.rect.height = sled_height;
        switch.rect.width = sled_width * 2;

        // Power Shelves
        for i in [17, 18] {
            let shelf = &mut self.power_shelves[i - 17];
            shelf.rect.y = rack.y + sled_height * (i as u16 / 2 + 1);
            shelf.rect.x = rack.x;
            shelf.rect.height = sled_height;
            shelf.rect.width = sled_width * 2;
        }

        // Bottom Switch
        let switch = &mut self.switches[1];
        switch.rect.y = rack.y + sled_height * (11);
        switch.rect.x = rack.x;
        switch.rect.height = sled_height;
        switch.rect.width = sled_width * 2;

        // Bottom Sleds
        // We treat each non-sled as 2 sleds for layout purposes
        for i in 24..40 {
            self.size_sled(i, &rack, sled_height, sled_width);
        }
    }

    fn size_sled(
        &mut self,
        i: usize,
        rack: &Rect,
        sled_height: u16,
        sled_width: u16,
    ) {
        // The power shelves and switches are in between in the layout
        let index = if i < 16 { i } else { i - 8 };
        let sled = &mut self.sleds[index];
        sled.rect.y = rack.y + sled_height * (i as u16 / 2);
        if i % 2 == 0 {
            // left sled
            sled.rect.x = rack.x
        } else {
            // right sled
            sled.rect.x = rack.x + sled_width;
        }
        sled.rect.height = sled_height;
        sled.rect.width = sled_width;
    }
}

/// Show the rack inventory as learned from MGS
pub struct InventoryScreen {
    log: Logger,
    watermark: &'static str,
    rack_state: RackState,
}

impl InventoryScreen {
    pub fn new(log: &Logger) -> InventoryScreen {
        InventoryScreen {
            log: log.clone(),
            watermark: include_str!("../../banners/oxide.txt"),
            rack_state: RackState::default(),
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
    fn draw_rack(&mut self, f: &mut Frame, watermark_height: Height) {
        self.rack_state.resize(&f.size(), &watermark_height);

        let switches =
            Block::default().style(Style::default().bg(OX_OFF_WHITE));

        let power_shelves =
            Block::default().style(Style::default().bg(OX_YELLOW));

        let sled = Block::default()
            .style(Style::default().bg(Color::Gray))
            .borders(Borders::TOP | Borders::RIGHT)
            .border_style(Style::default().fg(Color::White));

        for (i, s) in self.rack_state.sleds.iter().enumerate() {
            let sled = sled.clone().title(format!("sled {}", i + 1));
            f.render_widget(sled, s.rect.clone());
        }

        for (i, s) in self.rack_state.switches.iter().enumerate() {
            f.render_widget(switches.clone(), s.rect.clone());
        }

        for (i, s) in self.rack_state.power_shelves.iter().enumerate() {
            f.render_widget(power_shelves.clone(), s.rect.clone());
        }
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
        vec![Action::Redraw]
    }
}
