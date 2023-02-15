// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory and control of individual rack components: sled,switch,psc

use super::common::CommonScreenState;
use super::Screen;
use crate::defaults::colors::*;
use crate::defaults::dimensions::RectExt;
use crate::defaults::dimensions::MENUBAR_HEIGHT;
use crate::defaults::style;
use crate::screens::ScreenId;
use crate::widgets::HelpButtonState;
use crate::widgets::HelpMenuState;
use crate::widgets::ScreenButtonState;
use crate::wizard::{Action, Frame, ScreenEvent, State, Term};
use crossterm::event::Event as TermEvent;
use crossterm::event::{
    KeyCode, KeyEvent, MouseButton, MouseEvent, MouseEventKind,
};
use tui::layout::Alignment;
use tui::style::Modifier;
use tui::style::Style;
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, Paragraph};

pub struct ComponentScreen {
    common: CommonScreenState,
}

impl ComponentScreen {
    pub fn new() -> ComponentScreen {
        let help_text = vec![
            ("<TAB>", "Cycle forward through components"),
            ("<SHIFT>-<TAB>", "Cycle backwards through components"),
            ("<ESC>", "Exit help menu | Go back to the rack screen"),
            ("<CTRL-r>", "Go back to the rack screen"),
            ("<CTRL-h>", "Toggle this help menu"),
            ("<CTRL-c>", "Exit the program"),
        ];
        ComponentScreen {
            common: CommonScreenState {
                hovered: None,
                help_button_state: HelpButtonState::new(1, 0),
                help_menu_state: HelpMenuState::new(help_text),
                rack_screen_button_state: ScreenButtonState::new(
                    ScreenId::Rack,
                ),
            },
        }
    }

    fn draw_component_list(&self, f: &mut Frame, state: &State) {
        let current = state.rack_state.get_current_component_id();
        let menu_bar_style = style::menu_bar();

        // Draw the components list
        // TODO: Some sliding style animation?
        let title = Spans::from(vec![
            Span::styled(
                state.rack_state.get_prev_component_id().name(),
                menu_bar_style,
            ),
            Span::raw("   "),
            Span::styled(current.name(), style::menu_bar_selected()),
            Span::raw("   "),
            Span::styled(
                state.rack_state.get_next_component_id().name(),
                menu_bar_style,
            ),
        ]);

        let mut rect = f.size();
        rect.height = 1;
        rect.y = 1;
        let title_block = Block::default()
            .style(menu_bar_style)
            .title(title)
            .title_alignment(Alignment::Center);
        f.render_widget(title_block, rect);
    }

    fn draw_power_state(&self, f: &mut Frame, state: &State) {
        let current = state.rack_state.get_current_component_id();
        let title = match state.inventory.get_power_state(&current) {
            Some(s) => {
                format!(
                    "⌁ Power State: {}",
                    s.description().to_ascii_uppercase()
                )
            }
            None => "⌁ Power State: UNKNOWN".to_string(),
        };

        let mut rect = f.size();
        rect.height = 1;
        rect.y = 3;
        let power_state_block = Block::default()
            .style(style::menu_bar_selected())
            .title(title)
            .title_alignment(Alignment::Center);
        f.render_widget(power_state_block, rect);
    }

    fn draw_inventory(&self, f: &mut Frame, state: &State) {
        // Draw the header
        let inventory_style = Style::default().fg(OX_YELLOW_DIM);

        let mut header_style = style::menu_bar_selected();
        header_style =
            header_style.add_modifier(Modifier::UNDERLINED | Modifier::BOLD);

        let text = Text::styled("INVENTORY\n\n", header_style);
        let rect = f
            .size()
            .move_down_within_bounds(MENUBAR_HEIGHT + 1)
            .center_horizontally(text.width() as u16);
        let header = Paragraph::new(text);
        f.render_widget(header, rect);

        // Draw the contents
        let text = match state
            .inventory
            .get_inventory(&state.rack_state.get_current_component_id())
        {
            Some(inventory) => {
                Text::styled(format!("{:#?}", inventory), inventory_style)
            }
            None => Text::styled("UNKNOWN", inventory_style),
        };

        let rect = f
            .size()
            .move_down_within_bounds(MENUBAR_HEIGHT + 4)
            .center_horizontally(text.width() as u16);

        let inventory = Paragraph::new(text);
        f.render_widget(inventory, rect);
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
                if self.common.help_menu_state.is_closed() {
                    return vec![Action::SwitchScreen(ScreenId::Rack)];
                } else {
                    self.common.help_menu_state.close();
                }
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
                self.common.handle_mouse_click()
            }
            _ => vec![],
        }
    }

    fn set_hover_state(
        &mut self,
        _state: &mut State,
        x: u16,
        y: u16,
    ) -> Vec<Action> {
        let current_id = self.common.find_intersection(x, y);
        if current_id == self.common.hovered {
            // No change
            vec![]
        } else {
            self.common.hovered = current_id;
            vec![Action::Redraw]
        }
    }
}

impl Screen for ComponentScreen {
    fn draw(&self, state: &State, terminal: &mut Term) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.common.draw_background(f);
            self.common.draw_menubar(f, state);
            self.draw_component_list(f, state);
            self.draw_power_state(f, state);
            self.common.draw_help_menu(f);
            self.common.draw_screen_selection_buttons(f);
            self.draw_inventory(f, state);
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
                // A redraw always occurs in the wizard
                vec![]
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
