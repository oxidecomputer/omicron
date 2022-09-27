// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A Modal for displaying rack inventory

use crate::screens::make_even;
use crate::screens::Height;
use crate::screens::RectState;
use crate::Inventory;
use slog::debug;
use slog::Logger;
use std::marker::PhantomData;
use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::Style;
use tui::widgets::Block;
use tui::widgets::Borders;
use tui::widgets::StatefulWidget;
use tui::widgets::Widget;

#[derive(Debug, Clone, Default)]
pub struct InventoryModal<'a> {
    style: Style,
    phantom: PhantomData<&'a usize>,
}

impl<'a> InventoryModal<'a> {
    pub fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }
}

pub struct InventoryModalState<'a> {
    inventory: &'a Inventory,
}

impl<'a> StatefulWidget for InventoryModal<'a> {
    type State = InventoryModalState<'a>;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        unimplemented!()
    }
}
