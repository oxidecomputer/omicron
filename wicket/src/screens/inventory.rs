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
use crossterm::event::{
    KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use slog::Logger;
use std::collections::BTreeMap;
use tui::layout::{Alignment, Rect};
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
    hovered: Option<ComponentId>,
    tab_index_by_component_id: BTreeMap<ComponentId, TabIndex>,
    component_id_by_tab_index: BTreeMap<TabIndex, ComponentId>,
    modal_active: bool,
}

impl InventoryScreen {
    pub fn new(log: &Logger) -> InventoryScreen {
        let mut rack_state = RackState::new();
        rack_state.set_logger(log.clone());
        let mut screen = InventoryScreen {
            log: log.clone(),
            watermark: include_str!("../../banners/oxide.txt"),
            rack_state,
            tab_index: TabIndex::new_unset(MAX_TAB_INDEX),
            hovered: None,
            tab_index_by_component_id: BTreeMap::new(),
            component_id_by_tab_index: BTreeMap::new(),
            modal_active: false,
        };
        screen.init_tab_index();
        screen
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
        let mut rect = f.size();

        // Only draw the banner if there is enough horizontal whitespace to
        // make it look good.
        if self.rack_state.rect.width * 3 + width > rect.width {
            return (Height(0), Width(0));
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
            .border_selected_style(
                Style::default().fg(OX_YELLOW).bg(OX_GRAY_DARK),
            )
            .border_hover_style(Style::default().fg(OX_PINK).bg(OX_GRAY_DARK))
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
        if !self.tab_index.is_set() {
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

        // Unwraps are safe because we verified self.tab_index.is_set() above.
        let current =
            *self.component_id_by_tab_index.get(&self.tab_index).unwrap();
        let next = *self
            .component_id_by_tab_index
            .get(&self.tab_index.next())
            .unwrap();
        let prev = *self
            .component_id_by_tab_index
            .get(&self.tab_index.prev())
            .unwrap();

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
                if !self.modal_active && self.tab_index.is_set() {
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

    fn handle_mouse_event(
        &mut self,
        state: &State,
        event: MouseEvent,
    ) -> Vec<Action> {
        match event.kind {
            MouseEventKind::Moved => {
                self.set_hover_state(event.column, event.row)
            }
            MouseEventKind::Down(MouseButton::Left) => {
                self.handle_mouse_click(state)
            }
            _ => vec![],
        }
    }

    fn handle_mouse_click(&mut self, state: &State) -> Vec<Action> {
        // Do nothing if we're inside the modal
        if self.modal_active {
            return vec![];
        }

        // Set the tab index to the hovered component Id if there is one.
        // Remove the old tab_index, and make it match the clicked one
        if let Some(component_id) = self.hovered {
            self.clear_tabbed();
            self.tab_index = self
                .tab_index_by_component_id
                .get(&component_id)
                .unwrap()
                .clone();
            self.set_tabbed();

            // Now that we've set the TabIndex, just act as if `Enter` was
            // pressed.
            self.handle_key_event(
                state,
                KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            )
        } else {
            vec![]
        }
    }

    // Discover which rect the mouse is hovering over, remove any previous
    // hover state, and set any new state.
    fn set_hover_state(&mut self, x: u16, y: u16) -> Vec<Action> {
        let id = self.find_rect_intersection(x, y);
        if id == self.hovered {
            // No change
            vec![]
        } else {
            if let Some(id) = self.hovered {
                // Clear the old hover state
                self.rack_state.component_rects.get_mut(&id).unwrap().hovered =
                    false;
            }
            self.hovered = id;
            if let Some(id) = id {
                // Set the new state
                self.rack_state.component_rects.get_mut(&id).unwrap().hovered =
                    true;
            }
            vec![Action::Redraw]
        }
    }

    // See if the mouse is hovering over any part of the rack and return
    // the component if it is.
    fn find_rect_intersection(&self, x: u16, y: u16) -> Option<ComponentId> {
        let mouse_pointer = Rect { x, y, width: 1, height: 1 };
        if !self.rack_state.rect.intersects(mouse_pointer) {
            return None;
        }

        // Find the interesecting component.
        // I'm sure there's a faster way to do this with percentages, etc..,
        // but this works for now.
        for (id, rect_state) in self.rack_state.component_rects.iter() {
            if rect_state.rect.intersects(mouse_pointer) {
                return Some(*id);
            }
        }
        None
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
        if let Some(id) = self.component_id_by_tab_index.get(&self.tab_index) {
            self.rack_state.component_rects.get_mut(&id).unwrap().tabbed = val;
        }
    }

    // Map Rack [`ComponentId`]s
    fn init_tab_index(&mut self) {
        for i in 0..=MAX_TAB_INDEX {
            let tab_index = TabIndex::new(MAX_TAB_INDEX, i);
            let component_id = if i < 16 {
                ComponentId::Sled(i.try_into().unwrap())
            } else if i > 19 {
                ComponentId::Sled((i - 4).try_into().unwrap())
            } else if i == 16 {
                // Switches
                ComponentId::Switch(0)
            } else if i == 19 {
                ComponentId::Switch(1)
            } else if i == 17 || i == 18 {
                // Power Shelves
                // We actually want to return the active component here, so
                // we name it "psc X"
                ComponentId::Psc((i - 17).try_into().unwrap())
            } else {
                // If we add more items to tab through this will change
                unreachable!();
            };

            self.component_id_by_tab_index.insert(tab_index, component_id);
            self.tab_index_by_component_id.insert(component_id, tab_index);
        }
    }
}

const MARGIN: Height = Height(5);
impl Screen for InventoryScreen {
    fn draw(
        &mut self,
        state: &State,
        terminal: &mut crate::Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.draw_background(f);
            self.draw_rack(f, MARGIN);
            if self.modal_active {
                self.draw_modal(state, f, MARGIN);
            }
            self.draw_watermark(f);
        })?;
        Ok(())
    }

    fn on(&mut self, state: &State, event: ScreenEvent) -> Vec<Action> {
        match event {
            ScreenEvent::Term(TermEvent::Key(key_event)) => {
                self.handle_key_event(state, key_event)
            }
            ScreenEvent::Term(TermEvent::Mouse(mouse_event)) => {
                self.handle_mouse_event(state, mouse_event)
            }
            ScreenEvent::Tick => {
                vec![]
            }
            _ => unimplemented!(),
        }
    }
}
