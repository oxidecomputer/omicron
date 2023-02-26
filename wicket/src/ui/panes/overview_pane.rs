// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Control, Pane, Tab};
use crate::ui::defaults::colors::*;
use crate::ui::defaults::style;
use crate::ui::widgets::Rack;
use crate::{Action, Event, Frame, State};
use crossterm::event::Event as TermEvent;
use crossterm::event::KeyCode;
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::style::{Color, Style};
use tui::text::Text;
use tui::widgets::{Block, BorderType, Borders, Paragraph, Widget};

/// The OverviewPane shows a rendering of the rack.
///
/// This is useful for getting a quick view of the state of the rack.
pub struct OverviewPane {
    // We want the controls in a specific order accessible by index
    tabs: Vec<Tab>,
    selected: usize,
}

impl OverviewPane {
    pub fn new() -> OverviewPane {
        OverviewPane {
            tabs: vec![
                Tab { title: "OXIDE RACK", control: Box::new(RackTab {}) },
                Tab { title: "INVENTORY", control: Box::new(InventoryTab {}) },
            ],
            selected: 0,
        }
    }
}

impl Pane for OverviewPane {
    fn tab_titles(&self) -> Vec<&'static str> {
        self.tabs.iter().map(|t| t.title).collect()
    }

    fn selected_tab(&self) -> usize {
        self.selected
    }
}

impl Control for OverviewPane {
    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Tab => {
                    self.selected = (self.selected + 1) % self.tabs.len();
                    Some(Action::Redraw)
                }
                _ => self.tabs[self.selected].control.on(state, event),
            },
            _ => self.tabs[self.selected].control.on(state, event),
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: tui::layout::Rect,
        active: bool,
    ) {
        self.tabs[self.selected].control.draw(state, frame, rect, active)
    }
}

pub struct RackTab {}

impl Control for RackTab {
    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Up => {
                    state.rack_state.up();
                    Some(Action::Redraw)
                }
                KeyCode::Down => {
                    state.rack_state.down();
                    Some(Action::Redraw)
                }
                KeyCode::Char('k') => {
                    state.rack_state.toggle_knight_rider_mode();
                    Some(Action::Redraw)
                }
                KeyCode::Left | KeyCode::Right => {
                    state.rack_state.left_or_right();
                    Some(Action::Redraw)
                }
                _ => None,
            },
            Event::Tick => {
                // TODO: This only animates when the pane is active. Should we move the
                // tick into the wizard instead?
                if let Some(k) = state.rack_state.knight_rider_mode.as_mut() {
                    k.step();
                    Some(Action::Redraw)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: tui::layout::Rect,
        active: bool,
    ) {
        let border_style =
            if active { style::selected_line() } else { style::deselected() };

        // Draw the pane border
        let border = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);
        let inner = border.inner(rect);
        frame.render_widget(border, rect);

        // Draw the rack
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

            switch_selected_style: Style::default().bg(OX_GRAY_DARK),
            power_shelf_selected_style: Style::default().bg(OX_GRAY),
        };

        frame.render_widget(rack, inner);
    }
}

pub struct InventoryTab {}

impl Control for InventoryTab {
    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: tui::layout::Rect,
        active: bool,
    ) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Length(3),
                    Constraint::Min(0),
                    Constraint::Length(3),
                ]
                .as_ref(),
            )
            .split(rect);

        let (border_style, component_style) = if active {
            (style::selected_line(), style::selected())
        } else {
            (style::deselected(), style::deselected())
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);

        // Draw the current component
        // Make the borders touch (no gaps)
        let component = Paragraph::new(Text::from(format!(
            "{}",
            state.rack_state.selected
        )))
        .block(block.clone())
        .style(component_style)
        .alignment(Alignment::Center);
        frame.render_widget(component, chunks[0]);

        // Draw the contents
        let contents_block =
            block.clone().borders(Borders::LEFT | Borders::RIGHT);
        let mut rect = chunks[1];
        let inventory_style = Style::default().fg(OX_YELLOW_DIM);
        let text =
            match state.inventory.get_inventory(&state.rack_state.selected) {
                Some(inventory) => {
                    Text::styled(format!("{:#?}", inventory), inventory_style)
                }
                None => Text::styled("Inventory Unavailable", inventory_style),
            };

        let inventory = Paragraph::new(text).block(contents_block.clone());
        frame.render_widget(inventory, rect);

        // Draw the help bar
        let help = Paragraph::new("some help here | more help | yet more help")
            .block(block.clone());
        frame.render_widget(help, chunks[2]);

        // Make sure the top and bottom bars connect
        frame.render_widget(BoxConnector {}, chunks[1]);
    }

    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        None
    }
}

// Connect the top and bottom borders of contents in between two bars
struct BoxConnector {}

impl Widget for BoxConnector {
    fn render(self, rect: Rect, buf: &mut tui::buffer::Buffer) {
        buf.get_mut(rect.x, rect.y - 1).set_symbol("├");
        buf.get_mut(rect.x + rect.width - 1, rect.y - 1).set_symbol("┤");
        buf.get_mut(rect.x, rect.y + rect.height).set_symbol("├");
        buf.get_mut(rect.x + rect.width - 1, rect.y + rect.height)
            .set_symbol("┤");
    }
}
