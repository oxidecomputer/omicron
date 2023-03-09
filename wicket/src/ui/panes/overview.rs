// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use super::help_text;
use super::Control;
use crate::state::{ComponentId, ALL_COMPONENT_IDS};
use crate::ui::defaults::colors::*;
use crate::ui::defaults::style;
use crate::ui::panes::compute_scroll_offset;
use crate::ui::widgets::{BoxConnector, BoxConnectorKind, Rack};
use crate::{Action, Event, Frame, State};
use crossterm::event::Event as TermEvent;
use crossterm::event::KeyCode;
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::style::Style;
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, BorderType, Borders, Paragraph};

/// The OverviewPane shows a rendering of the rack.
///
/// This is useful for getting a quick view of the state of the rack.
pub struct OverviewPane {
    rack_view: RackView,
    inventory_view: InventoryView,
    rack_view_selected: bool,
}

impl OverviewPane {
    pub fn new() -> OverviewPane {
        OverviewPane {
            rack_view: RackView::default(),
            inventory_view: InventoryView::new(),
            rack_view_selected: true,
        }
    }

    pub fn dispatch(
        &mut self,
        state: &mut State,
        event: Event,
    ) -> Option<Action> {
        if self.rack_view_selected {
            self.rack_view.on(state, event)
        } else {
            self.inventory_view.on(state, event)
        }
    }
}

impl Control for OverviewPane {
    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Enter => {
                    // Switch between rack and inventory view
                    self.rack_view_selected = !self.rack_view_selected;
                    Some(Action::Redraw)
                }
                _ => self.dispatch(state, event),
            },
            _ => self.dispatch(state, event),
        }
    }

    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
        active: bool,
    ) {
        if self.rack_view_selected {
            self.rack_view.draw(state, frame, rect, active);
        } else {
            self.inventory_view.draw(state, frame, rect, active);
        }
    }
}

#[derive(Default)]
pub struct RackView {}

impl Control for RackView {
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
        rect: Rect,
        active: bool,
    ) {
        let (border_style, component_style) = if active {
            (style::selected_line(), style::selected())
        } else {
            (style::deselected(), style::deselected())
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(0)].as_ref())
            .split(rect);

        let border = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);

        // Draw the sled title (subview look)
        let title_bar = Paragraph::new(Spans::from(vec![Span::styled(
            "OXIDE RACK",
            component_style,
        )]))
        .block(border.clone().title("<ENTER>"));
        frame.render_widget(title_bar, chunks[0]);

        // Draw the pane border
        let border = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(border_style);
        let inner = border.inner(chunks[1]);
        frame.render_widget(border, chunks[1]);

        // Draw the rack
        let rack = Rack {
            state: &state.rack_state,
            switch_style: Style::default().bg(OX_GRAY_DARK).fg(OX_WHITE),
            power_shelf_style: Style::default().bg(OX_GRAY).fg(OX_OFF_WHITE),
            sled_style: Style::default().bg(OX_GREEN_LIGHT).fg(TUI_BLACK),
            sled_selected_style: Style::default()
                .fg(TUI_BLACK)
                .bg(TUI_PURPLE_DIM),

            border_style: Style::default().fg(OX_GRAY).bg(TUI_BLACK),
            border_selected_style: Style::default()
                .fg(TUI_BLACK)
                .bg(TUI_PURPLE),

            switch_selected_style: Style::default()
                .bg(TUI_PURPLE_DIM)
                .fg(TUI_PURPLE),
            power_shelf_selected_style: Style::default()
                .bg(TUI_PURPLE_DIM)
                .fg(TUI_PURPLE),
        };

        frame.render_widget(rack, inner);
    }
}

pub struct InventoryView {
    help: Vec<(&'static str, &'static str)>,
    // Vertical offset used for scrolling
    scroll_offsets: BTreeMap<ComponentId, usize>,
}

impl InventoryView {
    pub fn new() -> InventoryView {
        InventoryView {
            help: vec![
                ("RACK VIEW", "<ENTER>"),
                ("SWITCH COMPONENT", "<LEFT/RIGHT>"),
                ("SCROLL", "<UP/DOWN>"),
            ],
            scroll_offsets: ALL_COMPONENT_IDS
                .iter()
                .map(|id| (*id, 0))
                .collect(),
        }
    }
}

impl Control for InventoryView {
    fn draw(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
        rect: Rect,
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

        // Draw the sled title (subview look)
        let title_bar = Paragraph::new(Spans::from(vec![
            Span::styled("OXIDE RACK / ", border_style),
            Span::styled(
                state.rack_state.selected.to_string(),
                component_style,
            ),
        ]))
        .block(block.clone().title("<ENTER>"));
        frame.render_widget(title_bar, chunks[0]);

        // Draw the contents
        let contents_block = block
            .clone()
            .borders(Borders::LEFT | Borders::RIGHT | Borders::TOP);
        let inventory_style = Style::default().fg(OX_OFF_WHITE);
        let component_id = state.rack_state.selected;
        let text = match state.inventory.get_inventory(&component_id) {
            Some(inventory) => {
                Text::styled(format!("{:#?}", inventory), inventory_style)
            }
            None => Text::styled("Inventory Unavailable", inventory_style),
        };

        let scroll_offset = self.scroll_offsets.get_mut(&component_id).unwrap();
        let y_offset = compute_scroll_offset(
            *scroll_offset,
            text.height(),
            chunks[1].height as usize,
        );
        *scroll_offset = y_offset as usize;

        let inventory = Paragraph::new(text)
            .block(contents_block.clone())
            .scroll((y_offset, 0));
        frame.render_widget(inventory, chunks[1]);

        // Draw the help bar
        let help = help_text(&self.help).block(block.clone());
        frame.render_widget(help, chunks[2]);

        // Make sure the top and bottom bars connect
        frame.render_widget(
            BoxConnector::new(BoxConnectorKind::Bottom),
            chunks[1],
        );
    }

    fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        match event {
            Event::Term(TermEvent::Key(e)) => match e.code {
                KeyCode::Left => {
                    state.rack_state.prev();
                    Some(Action::Redraw)
                }
                KeyCode::Right => {
                    state.rack_state.next();
                    Some(Action::Redraw)
                }
                KeyCode::Down => {
                    let component_id = state.rack_state.selected;

                    // We currently debug print inventory on each call to
                    // `draw`, so we don't know  how many lines the total text is.
                    // We also don't know the Rect containing this Control, since
                    // we don't keep track of them anymore, and only know during
                    // rendering. Therefore, we just increment and correct
                    // for more incrments than lines exist during render, since
                    // `self` is passed mutably.
                    //
                    // It's also worth noting that the inventory may update
                    // before rendering, and so this is a somewhat sensible
                    // strategy.
                    *self.scroll_offsets.get_mut(&component_id).unwrap() += 1;
                    Some(Action::Redraw)
                }
                KeyCode::Up => {
                    let component_id = state.rack_state.selected;
                    let offset =
                        self.scroll_offsets.get_mut(&component_id).unwrap();
                    *offset = offset.saturating_sub(1);
                    Some(Action::Redraw)
                }
                _ => None,
            },
            Event::Tick => {
                // TODO: This only animates when the pane is active. Should we move the
                // tick into the [`Runner`] instead?
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
}
