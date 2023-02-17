// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Widgets related to the Update screen

use super::{HoverResult, Indicator, List, ListEntry, ListState};
use crate::defaults::colors::*;
use crate::inventory::ComponentId;
use crate::inventory::Inventory;
use std::collections::BTreeMap;
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::style::{Color, Style};
use tui::text::Text;
use tui::widgets::{Block, Borders, Paragraph, Widget};
use wicketd_client::types::{SpType, UpdateLog, UpdateLogAll};

#[derive(Debug)]
pub struct UpdateState {
    components: ListState<ComponentId>,
    pub status: BTreeMap<ComponentId, UpdateLog>,
    status_rect: Rect,
}

impl UpdateState {
    pub fn new() -> UpdateState {
        UpdateState {
            components: ListState::new(),
            status: BTreeMap::new(),
            status_rect: Rect::default(),
        }
    }

    pub fn init(&mut self, inventory: &Inventory) {
        self.fill_list(inventory);
    }

    pub fn resize(&mut self, bounding_box: Rect) {
        let v_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Ratio(1, 4),
                Constraint::Ratio(2, 4),
                Constraint::Ratio(1, 4),
            ])
            .split(bounding_box);

        let mid_h_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Ratio(1, 4),
                Constraint::Ratio(2, 4),
                Constraint::Ratio(1, 4),
            ])
            .split(v_chunks[1]);

        let mut list_rect = mid_h_chunks[1];
        list_rect.width = 14;

        self.status_rect = mid_h_chunks[1];
        self.status_rect.width -= 14;
        self.status_rect.x += 14;

        self.components.resize(list_rect);
    }

    fn fill_list(&mut self, _: &Inventory) {
        self.components.init(
            (0u8..36u8)
                .into_iter()
                .map(|i| {
                    let item = if i < 32 {
                        ComponentId::Sled(i)
                    } else if i < 34 {
                        ComponentId::Switch(i - 32)
                    } else {
                        ComponentId::Psc(i - 34)
                    };

                    let style = Style::default().fg(OX_GRAY);

                    ListEntry {
                        item,
                        indicator: Indicator { style, symbol: "⦿  " },
                    }
                })
                .collect(),
        );
    }

    pub fn on_mouse_move(&mut self, x: u16, y: u16) -> HoverResult {
        self.components.on_mouse_move(x, y)
    }

    pub fn on_mouse_click(&mut self) -> bool {
        self.components.on_mouse_click()
    }

    pub fn scroll_down(&mut self) {
        self.components.scroll_down()
    }

    pub fn scroll_up(&mut self) {
        self.components.scroll_up()
    }

    pub fn start_update(&mut self) {
        self.components.selection_mut().indicator.style =
            Style::default().fg(OX_YELLOW);
    }

    pub fn selected(&mut self) -> ComponentId {
        self.components.selection().item
    }

    pub fn to_widget(&self) -> Update<'_> {
        Update { state: self }
    }
}

pub struct Update<'a> {
    state: &'a UpdateState,
}

impl<'a> Widget for Update<'a> {
    fn render(self, rect: Rect, buf: &mut tui::buffer::Buffer) {
        // Draw list
        let list = List {
            state: &self.state.components,
            style: Style::default().bg(Color::Black).fg(OX_OFF_WHITE),
            hover_style: Style::default().bg(Color::Black).fg(OX_PINK),
            selection_style: Style::default()
                .bg(Color::Black)
                .fg(OX_GREEN_LIGHT),
            border_style: Style::default().bg(Color::Black).fg(OX_GRAY_DARK),
        };
        list.render(rect, buf);

        // Draw status
        let component_id = self.state.components.selection().item;
        let status = self
            .state
            .status
            .get(&component_id)
            .map_or("None".to_string(), |s| format!("{:#?}", s));

        let text = Text::from(status.as_str());
        let status = Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL))
            .style(Style::default().fg(OX_OFF_WHITE))
            .alignment(Alignment::Center);
        status.render(self.state.status_rect, buf);
    }
}
