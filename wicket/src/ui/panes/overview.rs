// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use super::help_text;
use super::ComputedScrollOffset;
use super::Control;
use crate::state::{ComponentId, ALL_COMPONENT_IDS};
use crate::ui::defaults::colors::*;
use crate::ui::defaults::style;
use crate::ui::widgets::IgnitionPopup;
use crate::ui::widgets::PopupScrollKind;
use crate::ui::widgets::{BoxConnector, BoxConnectorKind, Rack};
use crate::{Action, Cmd, Frame, State};
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::style::Style;
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, BorderType, Borders, Paragraph};

enum PopupKind {
    Ignition,
}

/// The OverviewPane shows a rendering of the rack.
///
/// This is useful for getting a quick view of the state of the rack.
///
// TODO: Move this state into global `State` so `Recorder` snapshots work
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

    pub fn dispatch(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        if self.rack_view_selected {
            self.rack_view.on(state, cmd)
        } else {
            self.inventory_view.on(state, cmd)
        }
    }
}

impl Control for OverviewPane {
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        match cmd {
            Cmd::Enter => {
                // Transition to the inventory view `Enter` makes sense here
                // because we are entering a view of the given component.
                if self.rack_view_selected {
                    self.rack_view_selected = false;
                    Some(Action::Redraw)
                } else {
                    self.inventory_view.on(state, cmd)
                }
            }
            Cmd::Exit => {
                if self.inventory_view.popup.is_some() {
                    // If we're showing a popup, pass this event through so we
                    // can close it.
                    self.inventory_view.on(state, cmd)
                } else if !self.rack_view_selected {
                    // Otherwise, transition to the rack view. `Exit` makes
                    // sense here because we are exiting a subview of the rack.
                    self.rack_view_selected = true;
                    Some(Action::Redraw)
                } else {
                    // We're already on the rack view - there's nowhere to exit
                    // to, so this is a no-op.
                    None
                }
            }
            _ => self.dispatch(state, cmd),
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
    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        match cmd {
            Cmd::Up => {
                state.rack_state.up();
                Some(Action::Redraw)
            }
            Cmd::Down => {
                state.rack_state.down();
                Some(Action::Redraw)
            }
            Cmd::KnightRiderMode => {
                state.rack_state.toggle_knight_rider_mode();
                Some(Action::Redraw)
            }
            Cmd::Left | Cmd::Right => {
                state.rack_state.left_or_right();
                Some(Action::Redraw)
            }
            Cmd::Tick => {
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
        .block(border.clone());
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
            inventory: &state.inventory,
            state: &state.rack_state,
            not_present_style: Style::default()
                .bg(OX_GRAY_DARK)
                .fg(OX_OFF_WHITE),
            suspicious_style: Style::default().bg(OX_RED).fg(OX_WHITE),
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
    ignition: IgnitionPopup,
    popup: Option<PopupKind>,
}

impl InventoryView {
    pub fn new() -> InventoryView {
        InventoryView {
            help: vec![
                ("Rack View", "<ESC>"),
                ("Switch Component", "<LEFT/RIGHT>"),
                ("Scroll", "<UP/DOWN>"),
                ("Ignition", "<I>"),
            ],
            scroll_offsets: ALL_COMPONENT_IDS
                .iter()
                .map(|id| (*id, 0))
                .collect(),
            ignition: IgnitionPopup::default(),
            popup: None,
        }
    }

    pub fn draw_ignition_popup(
        &mut self,
        state: &State,
        frame: &mut Frame<'_>,
    ) {
        let full_screen = Rect {
            width: state.screen_width,
            height: state.screen_height,
            x: 0,
            y: 0,
        };

        let popup_builder =
            self.ignition.to_popup_builder(state.rack_state.selected);
        // Scrolling in the ignition popup is always disabled.
        let popup = popup_builder.build(full_screen, PopupScrollKind::Disabled);
        frame.render_widget(popup, full_screen);
    }

    fn handle_cmd_in_popup(
        &mut self,
        state: &mut State,
        cmd: Cmd,
    ) -> Option<Action> {
        if cmd == Cmd::Exit {
            self.popup = None;
            return Some(Action::Redraw);
        }
        match self.popup.as_ref().unwrap() {
            PopupKind::Ignition => match cmd {
                Cmd::Up => {
                    self.ignition.key_up();
                    Some(Action::Redraw)
                }
                Cmd::Down => {
                    self.ignition.key_down();
                    Some(Action::Redraw)
                }
                Cmd::Enter => {
                    // Note: If making changes here, consider making them to the
                    // same arm of `UpdatePane::handle_cmd_in_popup()` in the
                    // `update` pane.
                    let command = self.ignition.selected_command();
                    let selected = state.rack_state.selected;
                    self.popup = None;
                    Some(Action::Ignition(selected, command))
                }
                _ => None,
            },
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
        .block(block.clone());
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
        let y_offset = ComputedScrollOffset::new(
            *scroll_offset,
            text.height(),
            chunks[1].height as usize,
        )
        .into_offset();
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

        match self.popup {
            Some(PopupKind::Ignition) => self.draw_ignition_popup(state, frame),
            None => (),
        }
    }

    fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        if self.popup.is_some() {
            return self.handle_cmd_in_popup(state, cmd);
        }
        match cmd {
            Cmd::Left => {
                state.rack_state.prev();
                Some(Action::Redraw)
            }
            Cmd::Right => {
                state.rack_state.next();
                Some(Action::Redraw)
            }
            Cmd::Down => {
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
            Cmd::Up => {
                let component_id = state.rack_state.selected;
                let offset =
                    self.scroll_offsets.get_mut(&component_id).unwrap();
                *offset = offset.saturating_sub(1);
                Some(Action::Redraw)
            }
            Cmd::GotoTop => {
                let component_id = state.rack_state.selected;
                *self.scroll_offsets.get_mut(&component_id).unwrap() = 0;
                Some(Action::Redraw)
            }
            Cmd::GotoBottom => {
                let component_id = state.rack_state.selected;
                // This will get corrected to be the bottom line during `draw`
                *self.scroll_offsets.get_mut(&component_id).unwrap() =
                    usize::MAX;
                Some(Action::Redraw)
            }
            Cmd::Tick => {
                // TODO: This only animates when the pane is active. Should we move the
                // tick into the [`Runner`] instead?
                if let Some(k) = state.rack_state.knight_rider_mode.as_mut() {
                    k.step();
                    Some(Action::Redraw)
                } else {
                    None
                }
            }
            Cmd::Ignition => {
                self.ignition.reset();
                self.popup = Some(PopupKind::Ignition);
                Some(Action::Redraw)
            }
            _ => None,
        }
    }
}
