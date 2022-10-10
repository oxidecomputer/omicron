// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The Rack presentation [`Screen`]

use super::colors::*;
use super::Screen;
use super::ScreenId;
use super::{Height, Width};
use crate::widgets::AnimationState;
use crate::widgets::Control;
use crate::widgets::ControlId;
use crate::widgets::KnightRiderMode;
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
use tui::style::{Color, Style};
use tui::widgets::{Block, Borders};

/// Show the rack view
pub struct RackScreen {
    log: Logger,
    watermark: &'static str,
    hovered: Option<ControlId>,
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
            let border_style =
                if self.hovered == Some(self.help_button_state.id()) {
                    hovered_style
                } else {
                    button_style
                };
            let button = HelpButton::new(
                &self.help_button_state,
                button_style,
                border_style,
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
                state.rack_state.inc_tab_index();
            }
            KeyCode::BackTab => {
                state.rack_state.dec_tab_index();
            }
            KeyCode::Esc => {
                if self.help_button_state.selected {
                    self.close_help_menu();
                } else {
                    state.rack_state.clear_tab_index();
                }
            }
            KeyCode::Enter => {
                if state.rack_state.tab_index.is_set() {
                    return vec![Action::SwitchScreen(ScreenId::Component)];
                }
            }
            KeyCode::Char('h') => {
                if event.modifiers.contains(KeyModifiers::CONTROL) {
                    self.open_help_menu();
                }
            }
            KeyCode::Char('k') => {
                if event.modifiers.contains(KeyModifiers::CONTROL) {
                    self.toggle_knight_rider_mode(state);
                }
            }
            _ => (),
        }
        vec![Action::Redraw]
    }

    fn toggle_knight_rider_mode(&self, state: &mut State) {
        if state.rack_state.knight_rider_mode.is_some() {
            state.rack_state.knight_rider_mode = None;
        } else {
            state.rack_state.knight_rider_mode =
                Some(KnightRiderMode::default());
        }
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
        match self.hovered {
            Some(control_id) if control_id == self.help_button_state.id() => {
                self.open_help_menu();
                vec![]
            }
            Some(control_id) if control_id == state.rack_state.id() => {
                state.rack_state.set_tab_from_hovered();
                vec![Action::SwitchScreen(ScreenId::Component)]
            }
            _ => vec![],
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
        let current_id = self.find_intersection(state, x, y);
        if current_id == self.hovered
            && self.hovered != Some(state.rack_state.id())
        {
            // No change
            vec![]
        } else {
            self.hovered = current_id;
            if self.hovered == Some(state.rack_state.id()) {
                // Update the specific component being hovered over
                if !state.rack_state.set_hover_state(x, y) {
                    // No need to redraw, as the component is the same as before
                    vec![]
                } else {
                    vec![Action::Redraw]
                }
            } else {
                state.rack_state.hovered = None;
                vec![Action::Redraw]
            }
        }
    }

    // Return if the coordinates interesct a given control.
    // This assumes disjoint control rectangles.
    fn find_intersection(
        &self,
        state: &State,
        x: u16,
        y: u16,
    ) -> Option<ControlId> {
        if self.help_button_state.intersects_point(x, y) {
            Some(self.help_button_state.id())
        } else if state.rack_state.intersects_point(x, y) {
            Some(state.rack_state.id())
        } else {
            None
        }
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
                let width = state.rack_state.rect.width / 2 - 2;
                let left = state.rack_state.rect.x + 1;
                let right = left + width;

                state
                    .rack_state
                    .knight_rider_mode
                    .as_mut()
                    .map(|k| k.inc(left, right));
                if self.help_button_state.selected {
                    let done = self.help_menu_state.as_mut().unwrap().step();
                    if done
                        && self.help_menu_state.as_ref().unwrap().is_closing()
                        && self.help_button_state.selected
                    {
                        self.help_menu_state = None;
                        self.help_button_state.selected = false;
                        return vec![Action::Redraw];
                    } else if !done {
                        return vec![Action::Redraw];
                    }
                }
                if state.rack_state.knight_rider_mode.is_some() {
                    vec![Action::Redraw]
                } else {
                    vec![]
                }
            }
            _ => vec![],
        }
    }
}
