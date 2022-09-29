// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A Modal for displaying rack inventory

use crate::inventory::{Component, FakePsc, FakeSled, FakeSwitch, Inventory};
use crate::screens::make_even;
use crate::screens::Height;
use crate::screens::RectState;
use slog::debug;
use slog::Logger;
use std::marker::PhantomData;
use tui::buffer::Buffer;
use tui::buffer::Cell;
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::style::Style;
use tui::text::{Span, Spans};
use tui::widgets::Block;
use tui::widgets::Borders;
use tui::widgets::StatefulWidget;
use tui::widgets::Widget;

pub struct ComponentModalState<'a> {
    pub prev_name: String,
    pub next_name: String,
    pub current_name: String,
    pub current_component: Option<Component>,
    pub inventory: &'a Inventory,
}

#[derive(Debug, Clone, Default)]
pub struct ComponentModal<'a> {
    style: Style,
    status_bar_style: Style,
    status_bar_selected_style: Style,
    phantom: PhantomData<&'a usize>,
}

impl<'a> ComponentModal<'a> {
    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }

    pub fn status_bar_style(mut self, style: Style) -> Self {
        self.status_bar_style = style;
        self
    }

    pub fn status_bar_selected_style(mut self, style: Style) -> Self {
        self.status_bar_selected_style = style;
        self
    }
}

impl<'a> ComponentModal<'a> {
    fn draw_status_bar(
        &self,
        area: Rect,
        buf: &mut Buffer,
        state: &mut ComponentModalState,
    ) {
        let mut rect = area.clone();
        rect.height = 5;

        let status_bar_block = Block::default().style(self.status_bar_style);

        status_bar_block.render(rect, buf);

        // The title is the current component in the middle with the previous
        // on the left and next on the right.
        //
        // TODO: Some sliding style animation?
        let title = Spans::from(vec![
            Span::styled(&state.prev_name, self.status_bar_style),
            Span::raw("   "),
            Span::styled(&state.current_name, self.status_bar_selected_style),
            Span::raw("   "),
            Span::styled(&state.next_name, self.status_bar_style),
        ]);

        let mut rect = area.clone();
        rect.height = 1;
        rect.y = area.y + 1;
        let title_block = Block::default()
            .style(self.status_bar_style)
            .title(title)
            .title_alignment(Alignment::Center);
        title_block.render(rect, buf);
    }

    fn draw_background(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::default().style(self.style);
        clear_buf(area.clone(), buf, self.style);
        block.render(area, buf);
    }
}

impl<'a> StatefulWidget for ComponentModal<'a> {
    type State = ComponentModalState<'a>;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        self.draw_background(area, buf);
        self.draw_status_bar(area, buf, state);
    }
}

// Set the buf area to the bg color
fn clear_buf(area: Rect, buf: &mut Buffer, style: Style) {
    for x in area.left()..area.right() {
        for y in area.top()..area.bottom() {
            buf.get_mut(x, y).set_style(style).set_symbol(" ");
        }
    }
}
