// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code that is shared among screens, although not necessarily all screens.
//! In particular, the splash and rack screens don't use much of this.

use super::ScreenId;
use crate::defaults::colors::*;
use crate::defaults::dimensions::MENUBAR_HEIGHT;
use crate::defaults::style;
use crate::widgets::{
    Control, ControlId, HelpButton, HelpButtonState, HelpMenu, HelpMenuState,
};
use crate::wizard::{Action, Frame};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tui::layout::Rect;
use tui::style::{Color, Style};
use tui::text::Text;
use tui::widgets::{Block, Paragraph};

// State shared between all screens except the splash and rack screens
//
// Each screen will maintain its own instance of this state.
pub struct CommonScreenState {
    pub help_menu_state: HelpMenuState,
    pub help_button_state: HelpButtonState,
    pub hovered: Option<ControlId>,
    pub next_screen: ScreenId,
    pub prev_screen: Option<ScreenId>,
}

impl CommonScreenState {
    pub fn resize(&mut self, _width: u16, _height: u16) {}

    pub fn handle_mouse_click(&mut self) -> Vec<Action> {
        match self.hovered {
            Some(control_id) if control_id == self.help_button_state.id() => {
                self.help_menu_state.open();
                vec![]
            }
            _ => vec![],
        }
    }

    pub fn handle_key_event(&mut self, event: KeyEvent) -> Vec<Action> {
        match event.code {
            KeyCode::Char('r') => {
                if event.modifiers.contains(KeyModifiers::CONTROL) {
                    return vec![Action::SwitchScreen(ScreenId::Rack)];
                }
            }
            KeyCode::Char('h') => {
                if event.modifiers.contains(KeyModifiers::CONTROL) {
                    self.help_menu_state.toggle();
                    return vec![Action::Redraw];
                }
            }
            KeyCode::Char('n') => {
                if event.modifiers.contains(KeyModifiers::CONTROL) {
                    return vec![Action::SwitchScreen(self.next_screen)];
                }
            }
            KeyCode::Char('p') => {
                if event.modifiers.contains(KeyModifiers::CONTROL) {
                    if let Some(prev) = self.prev_screen {
                        return vec![Action::SwitchScreen(prev)];
                    }
                }
            }
            _ => (),
        }
        vec![]
    }

    // Handle a tick event. Return true if a redraw is required, false
    // otherwise.
    pub fn tick(&mut self) -> bool {
        if !self.help_menu_state.is_closed() {
            self.help_menu_state.step();
            true
        } else {
            false
        }
    }

    /// Return if the coordinates interesct a given control.
    /// This assumes disjoint control rectangles.
    pub fn find_intersection(&self, x: u16, y: u16) -> Option<ControlId> {
        if self.help_button_state.intersects_point(x, y) {
            Some(self.help_button_state.id())
        } else {
            None
        }
    }

    pub fn draw_background(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_GREEN_DARK).bg(Color::Black);
        let block = Block::default().style(style);
        f.render_widget(block, f.size());
    }

    pub fn draw_menubar(&self, f: &mut Frame) {
        let mut rect = f.size();
        rect.height = MENUBAR_HEIGHT;

        let bar_block = Block::default().style(style::menu_bar());
        f.render_widget(bar_block, rect);
    }

    pub fn draw_help_menu(&self, f: &mut Frame) {
        // Draw the help button if the help menu is closed, otherwise draw the
        // help menu
        if !self.help_menu_state.is_closed() {
            let menu = HelpMenu {
                help: self.help_menu_state.help_text(),
                style: style::help_menu(),
                command_style: style::help_menu_command(),
                // Unwrap is safe because we check that the menu is open (and
                // thus has an AnimationState).
                state: self.help_menu_state.get_animation_state().unwrap(),
            };
            f.render_widget(menu, f.size());
        } else {
            let border_style =
                if self.hovered == Some(self.help_button_state.id()) {
                    style::button_hovered()
                } else {
                    style::button()
                };
            let button = HelpButton::new(
                &self.help_button_state,
                style::button(),
                border_style,
            );

            f.render_widget(button, f.size());
        }
    }

    pub fn draw_screen_navigation_instructions(&self, f: &mut Frame) {
        let prev = self.prev_screen_label();
        let next = self.next_screen_label();
        if let Some(p) = prev {
            let width = p.width() as u16;
            let prev =
                Paragraph::new(p).style(Style::default().fg(OX_OFF_WHITE));
            let rect = Rect {
                x: f.size().width - width - next.width() as u16 - 3,
                y: 1,
                width,
                height: 3,
            };
            f.render_widget(prev, rect);
        }

        let width = next.width() as u16;
        let next =
            Paragraph::new(next).style(Style::default().fg(OX_OFF_WHITE));
        let rect =
            Rect { x: f.size().width - width - 1, y: 1, width, height: 3 };
        f.render_widget(next, rect);
    }

    fn prev_screen_label(&self) -> Option<Text> {
        self.prev_screen.map(|s| {
            let name = s.name();
            let width = usize::max(6, name.len());
            Text::from(format!(
                "{:<width$}\n{:━^width$}\n{:<width$}",
                name,
                "━",
                "ctrl-p",
                width = width,
            ))
        })
    }

    fn next_screen_label(&self) -> Text {
        let name = self.next_screen.name();
        let width = usize::max(6, name.len());
        Text::from(format!(
            "{:<width$}\n{:━^width$}\n{:<width$}",
            name,
            "━",
            "ctrl-n",
            width = width,
        ))
    }
}
