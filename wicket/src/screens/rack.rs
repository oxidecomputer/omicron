// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Rack presentation [`Screen`]

use super::colors::*;
use super::Screen;
use super::TabIndex;
use super::{Height, Width};
use crate::inventory::ComponentId;
use crate::widgets::AnimationState;
use crate::widgets::Control;
use crate::widgets::{
    Banner, ComponentModal, ComponentModalState, HelpButton, HelpButtonState,
    HelpMenu, Rack, RackState,
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
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::style::{Color, Style};
use tui::widgets::{Block, Borders};

// Currently we only allow tabbing through the rack
const MAX_TAB_INDEX: u16 = 35;

// Is the mouse hovering over the HelpButton or rack?
#[derive(Debug, Clone, Copy)]
pub enum HoverState {
    Rack(ComponentId),
    HelpButton,
}

/// Show the rack view
pub struct RackScreen {
    log: Logger,
    watermark: &'static str,
    rack_state: RackState,
    tab_index: TabIndex,
    hovered: Option<HoverState>,
    help_button_state: HelpButtonState,
    help_menu_state: Option<AnimationState>,
    tab_index_by_component_id: BTreeMap<ComponentId, TabIndex>,
    component_id_by_tab_index: BTreeMap<TabIndex, ComponentId>,
}

impl RackScreen {
    pub fn new(log: &Logger) -> RackScreen {
        let mut rack_state = RackState::new();
        rack_state.set_logger(log.clone());
        let mut screen = RackScreen {
            log: log.clone(),
            watermark: include_str!("../../banners/oxide.txt"),
            rack_state,
            tab_index: TabIndex::new_unset(MAX_TAB_INDEX),
            hovered: None,
            help_button_state: HelpButtonState::new(1, 0),
            help_menu_state: None,
            tab_index_by_component_id: BTreeMap::new(),
            component_id_by_tab_index: BTreeMap::new(),
        };
        screen.init_tab_index();
        screen
    }

    fn draw_background(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_GREEN_DARK).bg(OX_GRAY);
        let block = Block::default().style(style).borders(Borders::NONE);
        f.render_widget(block, f.size());
    }

    fn draw_menubar(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_GREEN_DARK).bg(OX_GRAY);
        let button_style = Style::default().fg(OX_OFF_WHITE).bg(OX_GRAY_DARK);
        let selected_style = Style::default().fg(OX_YELLOW).bg(OX_GRAY_DARK);
        let hovered_style = Style::default().fg(OX_PINK).bg(OX_GRAY_DARK);
        let help_menu_style =
            Style::default().fg(OX_OFF_WHITE).bg(OX_GREEN_DARK);
        let help_menu_command_style =
            Style::default().fg(OX_GREEN_LIGHT).bg(OX_GREEN_DARK);

        // Draw the title
        let mut rect = f.size();
        let title = "Oxide Rack";
        rect.height = 1;
        rect.y = 1;
        let title_block = Block::default()
            .style(style)
            .title(title)
            .title_alignment(Alignment::Center);
        f.render_widget(title_block, rect);

        // Draw the help button if not selected, otherwise draw the help menu
        if self.help_button_state.selected {
            let menu = HelpMenu {
                style: help_menu_style,
                command_style: help_menu_command_style,
                state: *self.help_menu_state.as_ref().unwrap(),
            };
            f.render_widget(menu, f.size());
        } else {
            let button = HelpButton::new(
                &self.help_button_state,
                button_style,
                hovered_style,
                selected_style,
            );

            f.render_widget(button, f.size());
        }
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
                if self.help_button_state.selected {
                    self.close_help_menu();
                } else {
                    self.clear_tabbed();
                    self.tab_index.clear();
                }
            }
            KeyCode::Enter => {
                // TODO: Transition to the component screen
            }
            _ => (),
        }
        vec![Action::Redraw]
    }

    fn open_help_menu(&mut self) {
        self.help_button_state.selected = true;
        self.help_menu_state
            .get_or_insert(AnimationState::Opening { frame: 0, frame_max: 8 });
    }

    fn close_help_menu(&mut self) {
        let state = self.help_menu_state.take();
        match state {
            None => {
                // Already closed
                ()
            }
            Some(AnimationState::Opening { frame, frame_max }) => {
                // Transition to closing at the same position in the animation
                self.help_menu_state =
                    Some(AnimationState::Closing { frame, frame_max });
            }
            Some(s) => {
                // Already closing. Maintain same state
                self.help_menu_state = Some(s);
            }
        }
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

    fn handle_mouse_click(&mut self, _state: &State) -> Vec<Action> {
        // Set the tab index to the hovered component Id if there is one.
        // Remove the old tab_index, and make it match the clicked one
        if let Some(hover_state) = self.hovered {
            match hover_state {
                HoverState::HelpButton => {
                    self.open_help_menu();
                    vec![]
                }
                HoverState::Rack(component_id) => {
                    self.clear_tabbed();
                    self.tab_index = self
                        .tab_index_by_component_id
                        .get(&component_id)
                        .unwrap()
                        .clone();
                    self.set_tabbed();

                    // TODO: Transition to component screen
                    vec![Action::Redraw]
                }
            }
        } else {
            vec![]
        }
    }

    // Discover which rect the mouse is hovering over, remove any previous
    // hover state, and set any new state.
    fn set_hover_state(&mut self, x: u16, y: u16) -> Vec<Action> {
        // Are we currently hovering over the Help Button?
        if self.help_button_state.intersects_point(x, y) {
            let actions = match self.hovered {
                Some(HoverState::HelpButton) => {
                    // No change
                    vec![]
                }
                Some(HoverState::Rack(id)) => {
                    // Clear the old hover state
                    self.rack_state
                        .component_rects
                        .get_mut(&id)
                        .unwrap()
                        .hovered = false;
                    vec![Action::Redraw]
                }
                None => {
                    vec![Action::Redraw]
                }
            };

            self.help_button_state.hovered = true;
            self.hovered = Some(HoverState::HelpButton);
            return actions;
        }

        // Were we previously hovering over anything?
        let current_id = self.find_rack_rect_intersection(x, y);
        let actions = match self.hovered {
            Some(HoverState::HelpButton) => {
                self.help_button_state.hovered = false;
                vec![Action::Redraw]
            }
            Some(HoverState::Rack(id)) => {
                if current_id == Some(id) {
                    // No change
                    vec![]
                } else {
                    // Clear the old hover state
                    self.rack_state
                        .component_rects
                        .get_mut(&id)
                        .unwrap()
                        .hovered = false;
                    vec![Action::Redraw]
                }
            }
            None => {
                vec![Action::Redraw]
            }
        };

        // Are we actually hovering over the rack
        match current_id {
            Some(id) => {
                self.hovered = Some(HoverState::Rack(id));
                // Set the new state
                self.rack_state.component_rects.get_mut(&id).unwrap().hovered =
                    true;
            }
            None => {
                self.hovered = None;
            }
        };
        actions
    }

    // See if the mouse is hovering over any part of the rack and return
    // the component if it is.
    fn find_rack_rect_intersection(
        &self,
        x: u16,
        y: u16,
    ) -> Option<ComponentId> {
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
        // Exclude the last tab index, which is the hamburger menu
        for i in 0..MAX_TAB_INDEX {
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

    // TODO: This gets used in the component screen
    fn get_next_component_id(&self) -> ComponentId {
        let mut next = self.tab_index.next();
        *self.component_id_by_tab_index.get(&next).unwrap()
    }

    // TODO: This gets used in the component screen
    fn get_prev_component_id(&self) -> ComponentId {
        let mut prev = self.tab_index.prev();
        *self.component_id_by_tab_index.get(&prev).unwrap()
    }
}

const MARGIN: Height = Height(5);
impl Screen for RackScreen {
    fn draw(
        &mut self,
        _state: &State,
        terminal: &mut crate::Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.draw_background(f);
            self.draw_rack(f, MARGIN);
            self.draw_watermark(f);
            self.draw_menubar(f);
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
                if self.help_button_state.selected {
                    let done = self.help_menu_state.as_mut().unwrap().step();
                    if done
                        && self.help_menu_state.as_ref().unwrap().is_closing()
                        && self.help_button_state.selected
                    {
                        self.help_menu_state = None;
                        self.help_button_state.selected = false;
                        vec![Action::Redraw]
                    } else if !done {
                        vec![Action::Redraw]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            }
            _ => vec![],
        }
    }
}
