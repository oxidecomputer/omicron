// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inventory [`Screen`]

use super::colors::*;
use super::make_even;
use super::RectState;
use super::Screen;
use super::TabIndex;
use super::{Height, Width};
use crate::widgets::{Banner, Rack, RackState};
use crate::Action;
use crate::Frame;
use crate::ScreenEvent;
use crate::State;
use crossterm::event::Event as TermEvent;
use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers,
};
use slog::info;
use slog::Logger;
use tui::layout::Rect;
use tui::layout::{Alignment, Constraint, Direction, Layout};
use tui::style::{Color, Modifier, Style};
use tui::text::Span;
use tui::widgets::{Block, BorderType, Borders};

// Currently we only allow tabbing through the rack
const MAX_TAB_INDEX: u16 = 35;

/// Show the rack inventory as learned from MGS
pub struct InventoryScreen {
    log: Logger,
    watermark: &'static str,
    rack_state: RackState,
    tab_index: TabIndex,
}

impl InventoryScreen {
    pub fn new(log: &Logger) -> InventoryScreen {
        let mut rack_state = RackState::default();
        rack_state.set_logger(log.clone());
        InventoryScreen {
            log: log.clone(),
            watermark: include_str!("../../banners/oxide.txt"),
            rack_state,
            tab_index: TabIndex::new(MAX_TAB_INDEX),
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

        let mut rack = Rack::default()
            .switch_style(Style::default().bg(OX_GRAY_DARK))
            .power_shelf_style(Style::default().bg(OX_GRAY))
            .sled_style(Style::default().bg(OX_GREEN_LIGHT).fg(Color::Black))
            .sled_selected_style(
                Style::default().fg(Color::Black).bg(OX_GRAY_DARK),
            )
            .border_style(Style::default().fg(Color::Black))
            .border_selected_style(Style::default().fg(OX_YELLOW))
            .switch_selected_style(Style::default().bg(OX_GRAY_DARK))
            .power_shelf_selected_style(Style::default().bg(OX_GRAY_DARK));

        let area = self.rack_state.rect.clone();
        f.render_stateful_widget(rack, area, &mut self.rack_state);
    }

    fn handle_key_event(
        &mut self,
        state: &State,
        event: KeyEvent,
    ) -> Vec<Action> {
        match event.code {
            KeyCode::Tab => {
                self.clear_tabbed();
                self.tab_index.inc();
                self.set_tabbed();
            }
            KeyCode::BackTab => {
                self.clear_tabbed();
                self.tab_index.dec();
                self.set_tabbed();
            }
            KeyCode::Esc => {
                self.clear_tabbed();
                self.tab_index.clear();
            }
            _ => (),
        }
        vec![Action::Redraw]
    }

    // Set the tabbed boolean to `true` for the current tab indexed rect
    fn set_tabbed(&mut self) {
        self.update_tabbed(true);
    }

    // Set the tabbed boolean to `false` for the current tab indexed rect
    fn clear_tabbed(&mut self) {
        self.update_tabbed(false);
    }

    fn update_tabbed(&mut self, val: bool) {
        if let Some(i) = self.tab_index.get() {
            let i = usize::from(i);
            // Sleds
            if i < 16 {
                self.rack_state.sleds[i].tabbed = val;
            }
            if i > 19 {
                self.rack_state.sleds[i - 4].tabbed = val;
            }

            // Switches
            if i == 16 {
                self.rack_state.switches[0].tabbed = val;
            }
            if i == 19 {
                self.rack_state.switches[1].tabbed = val;
            }

            // Power Shelves
            if i == 17 || i == 18 {
                self.rack_state.power_shelves[i - 17].tabbed = val;
            }
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

    fn on(&mut self, state: &State, event: ScreenEvent) -> Vec<Action> {
        match event {
            ScreenEvent::Term(TermEvent::Key(key_event)) => {
                self.handle_key_event(state, key_event)
            }
            _ => unimplemented!(),
        }
    }
}
