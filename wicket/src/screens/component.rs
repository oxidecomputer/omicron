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
use tui::style::{Color, Style};
use tui::text::{Span, Spans, Text};
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
            ("<Enter> | left mouse click", "Select highlighted objects"),
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
        let style = Style::default().fg(OX_GREEN_DARK).bg(OX_GRAY);
        let block = Block::default().style(style);
        f.render_widget(block, f.size());
    }

    fn draw_status_bar(&self, f: &mut Frame, state: &State) {
        let mut rect = f.size();
        rect.height = 5;

        let style = Style::default().bg(OX_GREEN_DARK).fg(OX_GRAY);
        let selected_style = Style::default().fg(OX_GREEN_LIGHT);

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
        })?;
        Ok(())
    }

    fn on(&mut self, state: &mut State, event: ScreenEvent) -> Vec<Action> {
        match event {
            ScreenEvent::Term(TermEvent::Key(key_event)) => {
                //self.handle_key_event(state, key_event)
                vec![]
            }
            ScreenEvent::Term(TermEvent::Mouse(mouse_event)) => {
                //self.handle_mouse_event(state, mouse_event)
                vec![]
            }
            ScreenEvent::Tick => {
                vec![]
            }
            _ => vec![],
        }
    }
}
