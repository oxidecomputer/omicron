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
    phantom: PhantomData<&'a usize>,
}

impl<'a> ComponentModal<'a> {
    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }
}

impl<'a> StatefulWidget for ComponentModal<'a> {
    type State = ComponentModalState<'a>;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let block = Block::default()
            .style(self.style)
            .title(state.current_name.clone())
            .title_alignment(Alignment::Center);
        clear_buf(area.clone(), buf, self.style);
        block.render(area, buf);
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
