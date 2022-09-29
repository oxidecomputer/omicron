// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A rendering of the Oxide rack

use crate::inventory::ComponentId;
use crate::screens::make_even;
use crate::screens::Height;
use crate::screens::RectState;
use slog::Logger;
use std::collections::BTreeMap;
use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::Style;
use tui::widgets::Block;
use tui::widgets::Borders;
use tui::widgets::StatefulWidget;
use tui::widgets::Widget;

#[derive(Debug, Clone, Default)]
pub struct Rack {
    sled_style: Style,
    sled_selected_style: Style,
    switch_style: Style,
    switch_selected_style: Style,
    power_shelf_style: Style,
    power_shelf_selected_style: Style,
    border_style: Style,
    border_selected_style: Style,
    border_hover_style: Style,
}

impl Rack {
    pub fn sled_style(mut self, style: Style) -> Self {
        self.sled_style = style;
        self
    }
    pub fn sled_selected_style(mut self, style: Style) -> Self {
        self.sled_selected_style = style;
        self
    }
    pub fn switch_style(mut self, style: Style) -> Self {
        self.switch_style = style;
        self
    }
    pub fn switch_selected_style(mut self, style: Style) -> Self {
        self.switch_selected_style = style;
        self
    }
    pub fn power_shelf_style(mut self, style: Style) -> Self {
        self.power_shelf_style = style;
        self
    }
    pub fn power_shelf_selected_style(mut self, style: Style) -> Self {
        self.power_shelf_selected_style = style;
        self
    }
    pub fn border_style(mut self, style: Style) -> Self {
        self.border_style = style;
        self
    }
    pub fn border_selected_style(mut self, style: Style) -> Self {
        self.border_selected_style = style;
        self
    }

    pub fn border_hover_style(mut self, style: Style) -> Self {
        self.border_hover_style = style;
        self
    }

    pub fn draw_sled(&self, buf: &mut Buffer, sled: &RectState, i: u8) {
        let mut block =
            Block::default().title(format!("sled {}", i)).borders(Borders::ALL);
        if sled.tabbed {
            block = block
                .style(self.sled_selected_style)
                .border_style(self.border_selected_style);
        } else {
            block =
                block.style(self.sled_style).border_style(self.border_style);

            if sled.hovered {
                block = block.border_style(self.border_hover_style)
            }
        }

        let inner = block.inner(sled.rect);
        block.render(sled.rect, buf);

        // Draw some U.2 bays
        // TODO: Draw 10 only? - That may not scale down as well
        for x in inner.left()..inner.right() {
            for y in inner.top()..inner.bottom() {
                let cell = buf.get_mut(x, y).set_symbol("▕");
                if sled.tabbed {
                    if let Some(color) = self.sled_selected_style.fg {
                        cell.set_fg(color);
                    }
                } else {
                    if let Some(color) = self.sled_style.fg {
                        cell.set_fg(color);
                    }
                }
            }
        }
    }

    pub fn draw_switch(&self, buf: &mut Buffer, switch: &RectState, i: u8) {
        let mut block = Block::default()
            .title(format!("switch {}", i))
            .borders(Borders::ALL);
        if switch.tabbed {
            block = block
                .style(self.switch_selected_style)
                .border_style(self.border_selected_style);
        } else {
            block =
                block.style(self.switch_style).border_style(self.border_style);
            if switch.hovered {
                block = block.border_style(self.border_hover_style)
            }
        }

        let inner = block.inner(switch.rect);
        block.render(switch.rect, buf);

        for x in inner.left()..inner.right() {
            for y in inner.top()..inner.bottom() {
                let cell = buf.get_mut(x, y).set_symbol("❒");
            }
        }
    }

    pub fn draw_power_shelf(
        &self,
        buf: &mut Buffer,
        power_shelf: &RectState,
        i: u8,
        log: &Logger,
    ) {
        let mut block = Block::default()
            .title(format!("power {}", i))
            .borders(Borders::ALL);
        if power_shelf.tabbed {
            block = block
                .style(self.power_shelf_selected_style)
                .border_style(self.border_selected_style);
        } else {
            block = block
                .style(self.power_shelf_style)
                .border_style(self.border_style);
            if power_shelf.hovered {
                block = block.border_style(self.border_hover_style)
            }
        }

        let inner = block.inner(power_shelf.rect);
        block.render(power_shelf.rect, buf);

        let width = inner.right() - inner.left();
        let step = width / 6;
        let border = (width - step * 6) / 2;

        for x in inner.left() + border..inner.right() - border {
            for y in inner.top()..inner.bottom() {
                if x % step != 0 {
                    buf.get_mut(x, y).set_symbol("█");
                }
            }
        }
    }
}

impl StatefulWidget for Rack {
    type State = RackState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        for (id, rect) in state.component_rects.iter() {
            match id {
                ComponentId::Sled(i) => self.draw_sled(buf, rect, *i),
                ComponentId::Switch(i) => self.draw_switch(buf, rect, *i),
                ComponentId::Psc(i) => self.draw_power_shelf(
                    buf,
                    rect,
                    *i,
                    state.log.as_ref().unwrap(),
                ),
            }
        }
    }
}

// The visual state of the rack
#[derive(Debug, Default)]
pub struct RackState {
    pub log: Option<Logger>,
    // The starting TabIndex for the Rack.
    pub tab_start: u16,
    pub rect: Rect,
    pub component_rects: BTreeMap<ComponentId, RectState>,
}

impl RackState {
    pub fn new() -> RackState {
        let mut state = RackState::default();
        for i in 0..32 {
            state
                .component_rects
                .insert(ComponentId::Sled(i), RectState::default());
        }

        for i in 0..2 {
            state
                .component_rects
                .insert(ComponentId::Switch(i), RectState::default());
            state
                .component_rects
                .insert(ComponentId::Psc(i), RectState::default());
        }
        state
    }
    pub fn set_logger(&mut self, log: Logger) {
        self.log = Some(log);
    }

    // Split the rect into 20 vertical chunks. 1 for each sled bay, 1 per
    // switch, 1 per power shelf.
    // Divide each rack chunk into two horizontal chunks, one per sled.
    pub fn resize(&mut self, rect: &Rect, watermark_height: &Height) {
        let width = rect.width;
        let max_height = rect.height;
        let mut rack = rect.clone();

        // Scale proportionally and center the rack horizontally
        rack.height = make_even(rack.height - (watermark_height.0 * 2) - 2);
        rack.width = make_even(rack.height * 2 / 3);
        rack.x = width / 2 - rack.width / 2;

        // Make the max_height divisible by 20
        let actual_height = rack.height / 20 * 20;
        rack.height = actual_height;

        // Center the rack vertically
        rack.y = (max_height - actual_height) / 2 - 1;

        self.rect = rack.clone();

        let sled_height = rack.height / 20;
        let sled_width = rack.width / 2;

        // Top Sleds
        for i in 0..16u8 {
            self.size_sled(i, &rack, sled_height, sled_width);
        }

        // Top Switch
        let switch =
            self.component_rects.get_mut(&ComponentId::Switch(0)).unwrap();
        switch.rect.y = rack.y + sled_height * 8;
        switch.rect.x = rack.x;
        switch.rect.height = sled_height;
        switch.rect.width = sled_width * 2;

        // Power Shelves
        for i in [17, 18] {
            let shelf = self
                .component_rects
                .get_mut(&ComponentId::Psc(i - 17))
                .unwrap();
            shelf.rect.y = rack.y + sled_height * (i as u16 / 2 + 1);
            shelf.rect.x = rack.x;
            shelf.rect.height = sled_height;
            shelf.rect.width = sled_width * 2;
        }

        // Bottom Switch
        let switch =
            self.component_rects.get_mut(&ComponentId::Switch(1)).unwrap();
        switch.rect.y = rack.y + sled_height * (11);
        switch.rect.x = rack.x;
        switch.rect.height = sled_height;
        switch.rect.width = sled_width * 2;

        // Bottom Sleds
        // We treat each non-sled as 2 sleds for layout purposes
        for i in 24..40 {
            self.size_sled(i, &rack, sled_height, sled_width);
        }
    }

    fn size_sled(
        &mut self,
        i: u8,
        rack: &Rect,
        sled_height: u16,
        sled_width: u16,
    ) {
        // The power shelves and switches are in between in the layout
        let index = if i < 16 { i } else { i - 8 };
        let sled =
            self.component_rects.get_mut(&ComponentId::Sled(index)).unwrap();
        sled.rect.y = rack.y + sled_height * (i as u16 / 2);
        if i % 2 == 0 {
            // left sled
            sled.rect.x = rack.x
        } else {
            // right sled
            sled.rect.x = rack.x + sled_width;
        }
        sled.rect.height = sled_height;
        sled.rect.width = sled_width;
    }
}
