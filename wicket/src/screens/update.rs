// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manage updates for rack components

use super::common::CommonScreenState;
use super::{Screen, ScreenId};
use crate::defaults::style;
use crate::widgets::{HelpButtonState, HelpMenuState};
use crate::wizard::Frame;
use crate::wizard::{Action, ScreenEvent, State, Term};
use crate::{BOTTOM_MARGIN, TOP_MARGIN};
use crossterm::event::Event as TermEvent;
use crossterm::event::{
    KeyCode, KeyEvent, MouseButton, MouseEvent, MouseEventKind,
};
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::text::Spans;
use tui::widgets::Block;

pub struct UpdateScreen {
    common: CommonScreenState,
}

impl UpdateScreen {
    pub fn new() -> UpdateScreen {
        let help_text = vec![
            ("<ARROWS> | Mouse scroll", "Cycle through components"),
            ("<CTRL-h>", "Toggle this help menu"),
            ("<CTRL-n>", "Goto the next screen"),
            ("<CTRL-p>", "Goto the previous screen"),
            ("<CTRL-c>", "Exit the program"),
        ];

        UpdateScreen {
            common: CommonScreenState {
                hovered: None,
                help_button_state: HelpButtonState::new(1, 0),
                help_menu_state: HelpMenuState::new(help_text),
                prev_screen: Some(ScreenId::Component),
                next_screen: ScreenId::Rack,
            },
        }
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
            KeyCode::Up => {
                state.updates.scroll_up();
            }
            KeyCode::Down => {
                state.updates.scroll_down();
            }
            // Delegate to common handler
            _ => return self.common.handle_key_event(event),
        }
        vec![Action::Redraw]
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
                let mut actions = self.common.handle_mouse_click();
                if state.updates.on_mouse_click() {
                    actions.push(Action::Redraw);
                }
                actions
            }
            MouseEventKind::ScrollDown => {
                state.updates.scroll_down();
                // We need to check the hover state, because scrolling can move the list.
                let _ =
                    self.set_hover_state(state, state.mouse.x, state.mouse.y);
                vec![Action::Redraw]
            }
            MouseEventKind::ScrollUp => {
                state.updates.scroll_up();
                // We need to check the hover state, because scrolling can move the list.
                let _ =
                    self.set_hover_state(state, state.mouse.x, state.mouse.y);
                vec![Action::Redraw]
            }
            _ => vec![],
        }
    }

    fn set_hover_state(
        &mut self,
        state: &mut State,
        x: u16,
        y: u16,
    ) -> Vec<Action> {
        let update_hover = state.updates.on_mouse_move(x, y);
        if update_hover.hovered {
            self.common.hovered = None;
        }
        let mut redraw = update_hover.redraw;

        let current_id = self.common.find_intersection(x, y);
        redraw |= if current_id == self.common.hovered {
            // No change
            false
        } else {
            self.common.hovered = current_id;
            true
        };

        if redraw {
            vec![Action::Redraw]
        } else {
            vec![]
        }
    }

    fn draw_title(&self, f: &mut Frame) {
        let title = Spans::from("RACK UPDATE");
        let mut rect = f.size();
        rect.height = 1;
        rect.y = 2;
        let title_block = Block::default()
            .style(style::menu_bar())
            .title(title)
            .title_alignment(Alignment::Center);
        f.render_widget(title_block, rect);
    }
}

impl Screen for UpdateScreen {
    fn draw(&self, state: &State, terminal: &mut Term) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.common.draw_background(f);
            self.common.draw_menubar(f);
            self.draw_title(f);
            self.common.draw_help_menu(f);
            self.common.draw_screen_navigation_instructions(f);
            f.render_widget(state.updates.to_widget(), f.size());
            state.status_bar.draw(f);
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
            ScreenEvent::Term(TermEvent::Resize(width, height)) => {
                self.common.resize(width, height);
                state.updates.resize(Rect {
                    x: 0,
                    y: TOP_MARGIN.0,
                    width,
                    height: height - TOP_MARGIN.0 - BOTTOM_MARGIN.0,
                });
                vec![Action::Redraw]
            }
            ScreenEvent::Tick => {
                let mut redraw = self.common.tick();
                redraw |= state.status_bar.should_redraw();

                if redraw {
                    vec![Action::Redraw]
                } else {
                    vec![]
                }
            }
            _ => vec![],
        }
    }
}
