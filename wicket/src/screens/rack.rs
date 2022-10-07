// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Rack presentation [`Screen`]

use super::colors::*;
use super::Screen;
use super::ScreenId;
use super::{Height, Width};
use crate::inventory::ComponentId;
use crate::widgets::AnimationState;
use crate::widgets::Control;
use crate::widgets::{Banner, HelpButton, HelpButtonState, HelpMenu, Rack};
use crate::Action;
use crate::Frame;
use crate::ScreenEvent;
use crate::State;
use crossterm::event::Event as TermEvent;
use crossterm::event::{
    KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use slog::Logger;
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::style::{Color, Style};
use tui::widgets::{Block, Borders};

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
    hovered: Option<HoverState>,
    help_data: Vec<(&'static str, &'static str)>,
    help_button_state: HelpButtonState,
    help_menu_state: Option<AnimationState>,
}

impl RackScreen {
    pub fn new(log: &Logger) -> RackScreen {
        let help_data = vec![
            ("<TAB>", "Cycle forward through components"),
            ("<SHIFT>-<TAB>", "Cycle backwards through components"),
            ("<Enter> | left mouse click", "Select hovered object"),
            ("<ESC>", "Exit the current context"),
            ("<CTRL-C>", "Exit the program"),
        ];

        RackScreen {
            log: log.clone(),
            watermark: include_str!("../../banners/oxide.txt"),
            hovered: None,
            help_data,
            help_button_state: HelpButtonState::new(1, 0),
            help_menu_state: None,
        }
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
                help: &self.help_data,
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

    fn draw_watermark(&self, state: &State, f: &mut Frame) -> (Height, Width) {
        let style = Style::default().fg(OX_GRAY_DARK).bg(OX_GRAY);
        let banner = Banner::new(self.watermark).style(style);
        let height = banner.height();
        let width = banner.width();
        let mut rect = f.size();

        // Only draw the banner if there is enough horizontal whitespace to
        // make it look good.
        if state.rack_state.rect.width * 3 + width > rect.width {
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
    fn draw_rack(&mut self, state: &State, f: &mut Frame) {
        let rack = Rack {
            state: &state.rack_state,
            switch_style: Style::default().bg(OX_GRAY_DARK).fg(OX_WHITE),
            power_shelf_style: Style::default().bg(OX_GRAY).fg(OX_OFF_WHITE),
            sled_style: Style::default().bg(OX_GREEN_LIGHT).fg(Color::Black),
            sled_selected_style: Style::default()
                .fg(Color::Black)
                .bg(OX_GRAY_DARK),

            border_style: Style::default().fg(OX_GRAY).bg(Color::Black),
            border_selected_style: Style::default()
                .fg(OX_YELLOW)
                .bg(OX_GRAY_DARK),

            border_hover_style: Style::default().fg(OX_PINK).bg(OX_GRAY_DARK),
            switch_selected_style: Style::default().bg(OX_GRAY_DARK),
            power_shelf_selected_style: Style::default().bg(OX_GRAY),
        };

        let area = state.rack_state.rect.clone();
        f.render_widget(rack, area);
    }

    fn handle_key_event(
        &mut self,
        state: &mut State,
        event: KeyEvent,
    ) -> Vec<Action> {
        match event.code {
            KeyCode::Tab => {
                self.clear_tabbed(state);
                state.rack_state.tab_index.inc();
                self.set_tabbed(state);
            }
            KeyCode::BackTab => {
                self.clear_tabbed(state);
                state.rack_state.tab_index.dec();
                self.set_tabbed(state);
            }
            KeyCode::Esc => {
                if self.help_button_state.selected {
                    self.close_help_menu();
                } else {
                    self.clear_tabbed(state);
                    state.rack_state.tab_index.clear();
                }
            }
            KeyCode::Enter => {
                return vec![Action::SwitchScreen(ScreenId::Component)];
            }
            KeyCode::Char('h') => {
                if event.modifiers.contains(KeyModifiers::CONTROL) {
                    self.open_help_menu();
                }
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
        state: &mut State,
        event: MouseEvent,
    ) -> Vec<Action> {
        match event.kind {
            MouseEventKind::Moved => {
                self.set_hover_state(state, event.column, event.row)
            }
            MouseEventKind::Down(MouseButton::Left) => {
                self.handle_mouse_click(state)
            }
            _ => vec![],
        }
    }

    fn handle_mouse_click(&mut self, state: &mut State) -> Vec<Action> {
        // Set the tab index to the hovered component Id if there is one.
        // Remove the old tab_index, and make it match the clicked one
        if let Some(hover_state) = self.hovered {
            match hover_state {
                HoverState::HelpButton => {
                    self.open_help_menu();
                    vec![]
                }
                HoverState::Rack(component_id) => {
                    state.rack_state.set_tab(component_id);
                    vec![Action::SwitchScreen(ScreenId::Component)]
                }
            }
        } else {
            vec![]
        }
    }

    // Discover which rect the mouse is hovering over, remove any previous
    // hover state, and set any new state.
    fn set_hover_state(
        &mut self,
        state: &mut State,
        x: u16,
        y: u16,
    ) -> Vec<Action> {
        // Are we currently hovering over the Help Button?
        if self.help_button_state.intersects_point(x, y) {
            let actions = match self.hovered {
                Some(HoverState::HelpButton) => {
                    // No change
                    vec![]
                }
                Some(HoverState::Rack(id)) => {
                    // Clear the old hover state
                    state
                        .rack_state
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
        let current_id = self.find_rack_rect_intersection(state, x, y);
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
                    state
                        .rack_state
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
                state
                    .rack_state
                    .component_rects
                    .get_mut(&id)
                    .unwrap()
                    .hovered = true;
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
        state: &State,
        x: u16,
        y: u16,
    ) -> Option<ComponentId> {
        let mouse_pointer = Rect { x, y, width: 1, height: 1 };
        if !state.rack_state.rect.intersects(mouse_pointer) {
            return None;
        }

        // Find the interesecting component.
        // I'm sure there's a faster way to do this with percentages, etc..,
        // but this works for now.
        for (id, rect_state) in state.rack_state.component_rects.iter() {
            if rect_state.rect.intersects(mouse_pointer) {
                return Some(*id);
            }
        }
        None
    }

    // Set the tabbed boolean to `true` for the current tab indexed rect
    fn set_tabbed(&mut self, state: &mut State) {
        state.rack_state.update_tabbed(true);
    }

    // Set the tabbed boolean to `false` for the current tab indexed rect
    fn clear_tabbed(&mut self, state: &mut State) {
        state.rack_state.update_tabbed(false);
    }
}

impl Screen for RackScreen {
    fn draw(
        &mut self,
        state: &State,
        terminal: &mut crate::Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.draw_background(f);
            self.draw_rack(state, f);
            self.draw_watermark(state, f);
            self.draw_menubar(f);
        })?;
        Ok(())
    }

    fn on(&mut self, state: &mut State, event: ScreenEvent) -> Vec<Action> {
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
