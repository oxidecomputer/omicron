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

#[derive(Debug)]
pub struct UpdateState {
    components: ListState<ComponentId>,
    status: BTreeMap<ComponentId, String>,
    status_rect: Rect,
}

const LOREM1: &'static str =
    "Lorem ipsum dolor sit amet, consectetur adipiscing
elit. Proin nunc urna, volutpat et velit at, mattis dictum elit. Proin rutrum
lacus vitae ex placerat condimentum. Vestibulum ante ipsum primis in faucibus
orci luctus et ultrices posuere cubilia curae; Sed sed sodales tellus, sit amet
eleifend sem. Mauris sollicitudin consequat diam, fermentum vestibulum diam
bibendum ac. Vivamus condimentum aliquam neque ac ultrices. Fusce sollicitudin
risus vitae ex scelerisque, nec facilisis enim faucibus. Nullam est velit,
placerat quis venenatis eu, viverra ac dolor. Aliquam eget orci vulputate,
fermentum sem sit amet, accumsan ex. Phasellus ornare dolor non massa tristique
ornare.";

const LOREM2: &'static str = "
    Vivamus justo tortor, fermentum non enim consequat, ultrices sodales urna.
Morbi scelerisque nisl vel orci aliquam hendrerit. Praesent varius vel lectus
non finibus. Pellentesque habitant morbi tristique senectus et netus et
malesuada fames ac turpis egestas. Donec quis lorem quis dui cursus congue.
Phasellus sollicitudin urna ex, eu lacinia tortor convallis ut. Nunc sed eros
mollis, feugiat erat sed, pretium nibh.
    
    
Suspendisse libero augue, efficitur ac posuere non, convallis et metus. Duis
tempor sodales lacus quis pellentesque. Duis mattis magna et nunc tristique, in
efficitur ante commodo. Sed non porta velit, ac eleifend lectus. Pellentesque
sollicitudin posuere sem vitae aliquet. Vivamus condimentum placerat semper.
Vestibulum lacinia pulvinar congue. Praesent mollis, orci nec rhoncus
ullamcorper, purus lorem fermentum lectus, tristique accumsan ligula quam ut
quam. Sed nec elit magna. Etiam eget pulvinar quam. Aenean a velit vulputate
turpis commodo tempus.
";

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
        self.fill_status();
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

                    // TODO: Fix this hardcoding nonsense
                    let style = if i == 4 {
                        Style::default().fg(OX_GREEN_LIGHT)
                    } else if i == 1 {
                        Style::default().fg(OX_YELLOW)
                    } else {
                        Style::default().fg(OX_GRAY)
                    };

                    ListEntry {
                        item,
                        indicator: Indicator { style, symbol: "⦿  " },
                    }
                })
                .collect(),
        );
    }

    /// TODO: Fill with real data
    pub fn fill_status(&mut self) {
        for (i, id) in self.components.items().enumerate() {
            let lorem = if i % 2 == 0 { LOREM1 } else { LOREM2 };
            self.status.insert(*id, lorem.to_string());
        }
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
        let text =
            Text::from(self.state.status.get(&component_id).unwrap().as_str());
        let status = Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL))
            .style(Style::default().fg(OX_OFF_WHITE))
            .alignment(Alignment::Center);
        status.render(self.state.status_rect, buf);
    }
}
