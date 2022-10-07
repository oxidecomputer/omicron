// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory and control of individual rack components: sled,switch,psc

use super::colors::*;
use super::Screen;
use super::TabIndex;
use super::{Height, Width};
use crate::inventory::ComponentId;
use crate::widgets::clear_buf;
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
use tui::buffer::Buffer;
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::style::Modifier;
use tui::style::{Color, Style};
use tui::text::{Span, Spans, Text};
use tui::widgets::Paragraph;
use tui::widgets::Widget;
use tui::widgets::{Block, Borders};

#[derive(Debug, Clone, Copy)]
pub enum HoverState {
    HelpButton,
}

pub struct ComponentScreen {
    hovered: Option<HoverState>,
    help_data: Vec<(&'static str, &'static str)>,
    help_button_state: HelpButtonState,
    help_menu_state: Option<AnimationState>,
}

impl ComponentScreen {
    pub fn new() -> ComponentScreen {
        let help_data = vec![
            ("<TAB>", "Cycle forward through components"),
            ("<SHIFT>-<TAB>", "Cycle backwards through components"),
            ("<ESC>", "Exit the current context"),
            ("<CTRL-C>", "Exit the program"),
        ];
        ComponentScreen {
            hovered: None,
            help_data,
            help_button_state: HelpButtonState::new(1, 0),
            help_menu_state: None,
        }
    }

    fn draw_background(&self, f: &mut Frame) {
        let style = Style::default().fg(OX_GREEN_DARK).bg(Color::Black);
        let block = Block::default().style(style);
        f.render_widget(block, f.size());
    }

    fn draw_status_bar(&self, f: &mut Frame, state: &State) {
        let mut rect = f.size();
        rect.height = 5;

        let style = Style::default().bg(OX_GREEN_DARK).fg(OX_GRAY);
        let selected_style = Style::default().fg(OX_GREEN_LIGHT);
        let help_menu_style =
            Style::default().fg(OX_OFF_WHITE).bg(OX_GREEN_DARK);
        let help_menu_command_style =
            Style::default().fg(OX_GREEN_LIGHT).bg(OX_GREEN_DARK);
        let button_style = Style::default().fg(OX_OFF_WHITE).bg(OX_GREEN_DARK);
        let hovered_style = Style::default().fg(OX_PINK).bg(OX_GREEN_DARK);

        let status_bar_block = Block::default().style(style);
        f.render_widget(status_bar_block, rect);

        let current = state.rack_state.get_current_component_id();

        // Draw the components list
        // TODO: Some sliding style animation?
        let title = Spans::from(vec![
            Span::styled(
                state.rack_state.get_next_component_id().name(),
                style,
            ),
            Span::raw("   "),
            Span::styled(current.name(), selected_style),
            Span::raw("   "),
            Span::styled(
                state.rack_state.get_next_component_id().name(),
                style,
            ),
        ]);

        let mut rect = f.size();
        rect.height = 1;
        rect.y = 1;
        let title_block = Block::default()
            .style(style)
            .title(title)
            .title_alignment(Alignment::Center);
        f.render_widget(title_block, rect);

        // Draw the power state
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
            .style(selected_style)
            .title(title)
            .title_alignment(Alignment::Center);
        f.render_widget(power_state_block, rect);

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
            );

            f.render_widget(button, f.size());
        }
    }

    fn draw_inventory(&self, f: &mut Frame, state: &State) {
        // Draw the header
        let selected_style = Style::default().fg(OX_GREEN_LIGHT);
        let inventory_style = Style::default().fg(OX_YELLOW_DIM);

        let mut header_style = selected_style;
        header_style =
            header_style.add_modifier(Modifier::UNDERLINED | Modifier::BOLD);

        let text = Text::styled("INVENTORY\n\n", header_style);
        let mut rect = f.size();
        rect.y = 6;
        rect.height = rect.height - 6;
        let center = (rect.width - text.width() as u16) / 2;
        rect.x = rect.x + center;
        rect.width = rect.width - center;
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

        let mut rect = f.size();
        rect.y = 9;
        rect.height = rect.height - 9;

        let center = (rect.width - text.width() as u16) / 2;
        rect.x = rect.x + center;
        rect.width = rect.width - center;

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
                }
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
            }
        } else {
            vec![]
        }
    }

    fn set_hover_state(
        &mut self,
        _state: &mut State,
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
                None => vec![Action::Redraw],
            };

            self.help_button_state.hovered = true;
            self.hovered = Some(HoverState::HelpButton);
            actions
        } else {
            if self.hovered.is_none() {
                vec![]
            } else {
                self.hovered = None;
                self.help_button_state.hovered = false;
                vec![Action::Redraw]
            }
        }
    }

    // TODO: DEDUPE
    fn open_help_menu(&mut self) {
        self.help_button_state.selected = true;
        self.help_menu_state
            .get_or_insert(AnimationState::Opening { frame: 0, frame_max: 8 });
    }

    // TODO: DEDUPE
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

    // Set the tabbed boolean to `true` for the current tab indexed rect
    // TODO: DEDUPE
    fn set_tabbed(&mut self, state: &mut State) {
        state.rack_state.update_tabbed(true);
    }

    // Set the tabbed boolean to `false` for the current tab indexed rect
    // TODO: DEDUPE
    fn clear_tabbed(&mut self, state: &mut State) {
        state.rack_state.update_tabbed(false);
    }
}

impl Screen for ComponentScreen {
    fn draw(
        &mut self,
        state: &State,
        terminal: &mut crate::Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.draw_background(f);
            self.draw_status_bar(f, state);
            self.draw_inventory(f, state);
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
                // TODO: DEDUPE w/ RackScreen
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
