// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inventory [`Screen`]

use super::colors::*;
use super::Screen;
use super::TabIndex;
use super::{Height, Width};
use crate::inventory::ComponentId;
use crate::widgets::{
    Banner, ComponentModal, ComponentModalState, Rack, RackState,
};
use crate::Action;
use crate::Frame;
use crate::ScreenEvent;
use crate::State;
use crossterm::event::Event as TermEvent;
use crossterm::event::{KeyCode, KeyEvent};
use slog::Logger;
use tui::layout::Alignment;
use tui::style::{Color, Style};
use tui::widgets::{Block, Borders};

// Currently we only allow tabbing through the rack
const MAX_TAB_INDEX: u16 = 35;

/// Show the rack inventory as learned from MGS
pub struct InventoryScreen {
    log: Logger,
    watermark: &'static str,
    rack_state: RackState,
    tab_index: TabIndex,
    modal_active: bool,
}

impl InventoryScreen {
    pub fn new(log: &Logger) -> InventoryScreen {
        let mut rack_state = RackState::new();
        rack_state.set_logger(log.clone());
        InventoryScreen {
            log: log.clone(),
            watermark: include_str!("../../banners/oxide.txt"),
            rack_state,
            tab_index: TabIndex::new(MAX_TAB_INDEX),
            modal_active: false,
        }
    }

    fn draw_background(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_GREEN_DARK).bg(OX_GRAY);
        let block = Block::default()
            .style(style)
            .borders(Borders::NONE)
            .title("Inventory: Fuck yeah!")
            .title_alignment(Alignment::Center);

        f.render_widget(block, f.size());
    }

    fn draw_watermark(&self, f: &mut Frame) -> (Height, Width) {
        let style = Style::default().fg(OX_GRAY_DARK).bg(OX_GRAY);
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
    fn draw_rack(&mut self, f: &mut Frame, vertical_border: Height) {
        self.rack_state.resize(&f.size(), &vertical_border);

        let rack = Rack::default()
            .switch_style(Style::default().bg(OX_GRAY_DARK).fg(OX_WHITE))
            .power_shelf_style(Style::default().bg(OX_GRAY).fg(OX_OFF_WHITE))
            .sled_style(Style::default().bg(OX_GREEN_LIGHT).fg(Color::Black))
            .sled_selected_style(
                Style::default().fg(Color::Black).bg(OX_GRAY_DARK),
            )
            .border_style(Style::default().fg(OX_GRAY).bg(Color::Black))
            .border_selected_style(Style::default().fg(OX_YELLOW))
            .switch_selected_style(Style::default().bg(OX_GRAY_DARK))
            .power_shelf_selected_style(Style::default().bg(OX_GRAY));

        let area = self.rack_state.rect.clone();
        f.render_stateful_widget(rack, area, &mut self.rack_state);
    }

    /// Draw the current component in the modal if one is selected
    fn draw_modal(
        &mut self,
        state: &State,
        f: &mut Frame,
        vertical_border: Height,
    ) {
        if self.tab_index.get().is_none() {
            return;
        }

        let component = ComponentModal::default()
            .style(Style::default().fg(OX_YELLOW).bg(Color::Black))
            .status_bar_style(Style::default().bg(OX_GREEN_DARK).fg(OX_GRAY))
            .status_bar_selected_style(Style::default().fg(OX_GREEN_LIGHT))
            .inventory_style(Style::default().fg(OX_YELLOW_DIM));

        let mut rect = f.size();
        rect.y = vertical_border.0;
        rect.height = rect.height - vertical_border.0 * 2 - 2;
        rect.x = vertical_border.0;
        rect.width = rect.width - vertical_border.0 * 2;

        // Unwraps are safe because we verified self.tab_index.get().is_some() above.
        let current = self.component_id(self.tab_index.get().unwrap());
        let next = self.component_id(self.tab_index.next().unwrap());
        let prev = self.component_id(self.tab_index.prev().unwrap());

        // TODO: Fill in with actual inventory
        let current_component = None;

        let mut modal_state = ComponentModalState {
            log: self.log.clone(),
            prev,
            next,
            current,
            current_component,
            inventory: &state.inventory,
        };

        f.render_stateful_widget(component, rect, &mut modal_state);
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
                if !self.modal_active {
                    self.clear_tabbed();
                    self.tab_index.clear();
                } else {
                    // Close the modal on the next draw
                    self.modal_active = false;
                }
            }
            KeyCode::Enter => {
                if !self.modal_active && self.tab_index.get().is_some() {
                    // Open the modal on the next draw
                    self.modal_active = true;
                } else {
                    // TODO: Send the command through to the modal
                }
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
            let id = self.component_id(i);
            self.rack_state.component_rects.get_mut(&id).unwrap().tabbed = val;
        }
    }

    // Return the ComponentId for a given TabIndex value
    //
    // XXX: If the TabIndex for the rack components changes, this method must
    // also.
    fn component_id(&self, i: u16) -> ComponentId {
        // Sleds
        if i < 16 {
            ComponentId::Sled(i.try_into().unwrap())
        } else if i > 19 {
            ComponentId::Sled((i - 4).try_into().unwrap())
        } else
        // Switches
        if i == 16 {
            ComponentId::Switch(0)
        } else if i == 19 {
            ComponentId::Switch(1)
        } else
        // Power Shelves
        // We actually want to return the active component here, so
        // we name it "psc X"
        if i == 17 || i == 18 {
            ComponentId::Psc((i - 17).try_into().unwrap())
        } else {
            unreachable!();
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
            if self.modal_active {
                self.draw_modal(state, f, height);
            }
        })?;
        Ok(())
    }

    fn on(&mut self, state: &State, event: ScreenEvent) -> Vec<Action> {
        match event {
            ScreenEvent::Term(TermEvent::Key(key_event)) => {
                self.handle_key_event(state, key_event)
            }
            ScreenEvent::Tick => {
                vec![]
            }
            _ => unimplemented!(),
        }
    }
}
