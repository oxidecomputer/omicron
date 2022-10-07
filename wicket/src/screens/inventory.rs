// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inventory [`Screen`]

use super::colors::*;
use super::Screen;
use super::TabIndex;
use super::{Height, Width};
use crate::inventory::ComponentId;
use crate::widgets::AnimationState;
use crate::widgets::HamburgerState;
use crate::widgets::{
    Banner, ComponentModal, ComponentModalState, MenuBar, Rack, RackState,
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
use tui::layout::Rect;
use tui::style::{Color, Style};
use tui::widgets::{Block, Borders};

// Currently we only allow tabbing through the rack
const MAX_TAB_INDEX: u16 = 36;

// Is the mouse hovering over the hamburger or rack?
#[derive(Debug, Clone, Copy)]
pub enum HoverState {
    Rack(ComponentId),
    Hamburger,
}

/// Show the rack inventory as learned from MGS
pub struct InventoryScreen {
    log: Logger,
    watermark: &'static str,
    rack_state: RackState,
    tab_index: TabIndex,
    hovered: Option<HoverState>,
    hamburger_state: HamburgerState,
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
            hamburger_state: HamburgerState::new(TabIndex::new(
                MAX_TAB_INDEX,
                MAX_TAB_INDEX,
            )),
            tab_index_by_component_id: BTreeMap::new(),
            component_id_by_tab_index: BTreeMap::new(),
            modal_active: false,
        };
        screen.init_tab_index();
        screen
    }

    fn help_menu_open(&self) -> bool {
        self.hamburger_state.help_menu.is_some()
    }

    fn draw_background(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_GREEN_DARK).bg(OX_GRAY);
        let block = Block::default().style(style).borders(Borders::NONE);
        f.render_widget(block, f.size());
    }

    fn draw_menubar(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_GREEN_DARK).bg(OX_GRAY);
        let selected_style = Style::default().fg(OX_YELLOW).bg(OX_GRAY);
        let hovered_style = Style::default().fg(OX_PINK).bg(OX_GRAY);
        let help_menu_style =
            Style::default().fg(OX_OFF_WHITE).bg(OX_GREEN_DARK);
        let help_menu_command_style =
            Style::default().fg(OX_GREEN_LIGHT).bg(OX_GREEN_DARK);
        let bar = MenuBar {
            hamburger_state: &self.hamburger_state,
            title: "Oxide Rack",
            style,
            selected_style,
            hovered_style,
            help_menu_style,
            help_menu_command_style,
        };
        f.render_widget(bar, f.size());
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

        // Unwrap is safe because we verified self.tab_index.is_set() above.
        let current =
            *self.component_id_by_tab_index.get(&self.tab_index).unwrap();
        let next = self.get_next_component_id();
        let prev = self.get_prev_component_id();

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
                // Don't process tab requests if the help menu is open
                if !self.help_menu_open() {
                    self.clear_tabbed();
                    self.tab_index.inc();
                    if self.modal_active
                        && self.tab_index == self.hamburger_state.tab_index
                    {
                        // We need to skip over the hamburger
                        self.tab_index.inc();
                    }
                    self.set_tabbed();
                }
            }
            KeyCode::BackTab => {
                // Don't process tab requests if the help menu is open
                if !self.help_menu_open() {
                    self.clear_tabbed();
                    self.tab_index.dec();
                    if self.modal_active
                        && self.tab_index == self.hamburger_state.tab_index
                    {
                        // We need to skip over the hamburger
                        self.tab_index.dec();
                    }
                    self.set_tabbed();
                }
            }
            KeyCode::Esc => {
                // Clear the top object
                if self.help_menu_open() {
                    self.close_help_menu();
                } else if self.modal_active {
                    // Close the modal on the next draw
                    self.modal_active = false;
                } else {
                    self.clear_tabbed();
                    self.tab_index.clear();
                }
            }
            KeyCode::Enter => {
                // We allow the hamburger menu on top of the modal
                if self.tab_index == self.hamburger_state.tab_index {
                    self.open_help_menu()
                } else if !self.modal_active && self.tab_index.is_set() {
                    // Open the modal on the next draw, but only if the help menu is not open already
                    if !self.help_menu_open() {
                        self.modal_active = true;
                    }
                }
            }
            _ => (),
        }
        vec![Action::Redraw]
    }

    fn open_help_menu(&mut self) {
        self.hamburger_state
            .help_menu
            .get_or_insert(AnimationState::Opening { frame: 0, frame_max: 8 });
    }

    fn close_help_menu(&mut self) {
        let state = self.hamburger_state.help_menu.take();
        match state {
            None => {
                // Already closed
                ()
            }
            Some(AnimationState::Opening { frame, frame_max }) => {
                // Transition to closing at the same position in the animation
                self.hamburger_state.help_menu =
                    Some(AnimationState::Closing { frame, frame_max });
            }
            Some(s) => {
                // Already closing. Maintain same state
                self.hamburger_state.help_menu = Some(s);
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

    fn handle_mouse_click(&mut self, state: &State) -> Vec<Action> {
        // Do nothing if we're inside the modal
        //
        // TODO: Unfortunately this means the user can't click the help
        // button. Fix this.
        //
        // The problem is that we conflate being tabbed as also allowing
        // entry to the object, in this case the hamburger. But when in the modal
        // we also limit tabs to not go to the hamburger ever, because we
        // want to allow only cycling through the sleds.
        //
        // One option to fix this would be to just go ahead and remove the
        // hamburger from the tab index, and allow access with <CMD-h> or
        // a mouse click only. This is probably the easiest, but not being
        // able to tab access to a control seems wrong. However, with the modal
        // we really only allow conditional tabbing.
        //
        // Another option is to separate the notion of being tab selected from
        // being clicked and make clicking not change the tab index.
        //
        // A final option is just to add more special case code. This really
        // seems terrible.
        if self.modal_active {
            return vec![];
        }

        // Set the tab index to the hovered component Id if there is one.
        // Remove the old tab_index, and make it match the clicked one
        if let Some(hover_state) = self.hovered {
            match hover_state {
                HoverState::Hamburger => {
                    self.clear_tabbed();
                    self.tab_index = self.hamburger_state.tab_index;
                    self.set_tabbed();
                }
                HoverState::Rack(component_id) => {
                    self.clear_tabbed();
                    self.tab_index = self
                        .tab_index_by_component_id
                        .get(&component_id)
                        .unwrap()
                        .clone();
                    self.set_tabbed();
                }
            }

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
        // Are we currently hovering over the hamburger?
        let mouse_pointer = Rect { x, y, width: 1, height: 1 };
        if self.hamburger_state.rect.intersects(mouse_pointer) {
            let actions = match self.hovered {
                Some(HoverState::Hamburger) => {
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

            self.hamburger_state.hovered = true;
            self.hovered = Some(HoverState::Hamburger);
            return actions;
        }

        // Were we previously hovering over anything?
        let current_id = self.find_rack_rect_intersection(x, y);
        let actions = match self.hovered {
            Some(HoverState::Hamburger) => {
                self.hamburger_state.hovered = false;
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
        } else if self.tab_index == self.hamburger_state.tab_index {
            self.hamburger_state.tabbed = val;
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

    fn get_next_component_id(&self) -> ComponentId {
        // We skip over the hamburger
        let mut next = self.tab_index.next();
        if next == self.hamburger_state.tab_index {
            next.inc();
        }
        *self.component_id_by_tab_index.get(&next).unwrap()
    }

    // We skip over the hamburger
    fn get_prev_component_id(&self) -> ComponentId {
        // We skip over the hamburger
        let mut prev = self.tab_index.prev();
        if prev == self.hamburger_state.tab_index {
            prev.dec();
        }
        *self.component_id_by_tab_index.get(&prev).unwrap()
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
            if self.modal_active {
                self.draw_modal(state, f, MARGIN);
            } else {
                self.draw_rack(f, MARGIN);
                self.draw_watermark(f);
            }
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
                if self.hamburger_state.help_menu.is_some() {
                    let done =
                        self.hamburger_state.help_menu.as_mut().unwrap().step();
                    if done
                        && self
                            .hamburger_state
                            .help_menu
                            .as_ref()
                            .unwrap()
                            .is_closing()
                    {
                        self.hamburger_state.help_menu = None;
                    }
                    if !done {
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
