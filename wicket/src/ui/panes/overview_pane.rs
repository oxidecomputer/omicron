// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use super::help_text;
use super::{Control, Pane, Tab};
use crate::state::{ComponentId, ALL_COMPONENT_IDS};
use crate::ui::defaults::colors::*;
use crate::ui::defaults::style;
use crate::ui::widgets::{BoxConnector, Rack};
use crate::{Action, Event, Frame, State};
use crossterm::event::Event as TermEvent;
use crossterm::event::KeyCode;
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::style::Style;
use tui::text::Text;
use tui::widgets::{Block, BorderType, Borders, Paragraph};

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
                Tab {
                    title: "INVENTORY",
                    control: Box::new(InventoryTab::new()),
                },
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
        rect: Rect,
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
        rect: Rect,
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

pub struct InventoryTab {
    help: Vec<(&'static str, &'static str)>,
    // Vertical offset used for scrolling
    scroll_offsets: BTreeMap<ComponentId, usize>,
}

impl InventoryTab {
    pub fn new() -> InventoryTab {
        InventoryTab {
            help: vec![("SELECT", "<LEFT/RIGHT>"), ("SCROLL", "<UP/DOWN>")],
            scroll_offsets: ALL_COMPONENT_IDS
                .iter()
                .map(|id| (*id, 0))
                .collect(),
        }
    }
}

impl Control for InventoryTab {
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
        let inventory_style = Style::default().fg(OX_OFF_WHITE);
        let component_id = state.rack_state.selected;
        let text = match state.inventory.get_inventory(&component_id) {
            Some(inventory) => {
                Text::styled(format!("{:#?}", inventory), inventory_style)
            }
            None => Text::styled("Inventory Unavailable", inventory_style),
        };

        // Scroll offset is in terms of lines of text.
        // We must compute it to be in terms of the number of terminal rows.
        //
        // XXX: This whole paragraph scroll only allows length of less than
        // 64k rows even though it shouldn't be limited. We can do our own
        // scrolling instead to obviate this limit. we may want to anyway, as
        // it's less data to be formatted for the Paragraph.
        let scroll_offset = self.scroll_offsets.get_mut(&component_id).unwrap();

        // Note that this check doesn't actually work with line wraps.
        // See https://github.com/tui-rs-revival/ratatui/pull/7
        if *scroll_offset > text.height() {
            *scroll_offset = text.height();
        }

        let num_lines = chunks[1].height as usize;
        if text.height() <= num_lines {
            *scroll_offset = 0;
        } else {
            if text.height() - *scroll_offset < num_lines {
                // Don't allow scrolling past bottom of content
                //
                // Reset the scroll_offset, so that an up arrow
                // will scroll up on the next try.
                *scroll_offset = text.height() - num_lines;
            }
        }
        // This doesn't allow data more than 64k rows. We shouldn't need
        // more than that for wicket, but who knows!
        let y_offset = u16::try_from(*scroll_offset).unwrap();

        let inventory = Paragraph::new(text)
            .block(contents_block.clone())
            .scroll((y_offset, 0));
        frame.render_widget(inventory, chunks[1]);

        // Draw the help bar
        let help = help_text(&self.help).block(block.clone());
        frame.render_widget(help, chunks[2]);

        // Make sure the top and bottom bars connect
        frame.render_widget(BoxConnector {}, chunks[1]);
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
}
