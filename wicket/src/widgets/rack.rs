// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A rendering of the Oxide rack

use crate::inventory::ComponentId;
use crate::screens::make_even;
use crate::screens::Height;
use crate::screens::RectState;
use crate::screens::TabIndex;
use slog::Logger;
use std::collections::BTreeMap;
use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::Style;
use tui::widgets::Block;
use tui::widgets::Borders;
use tui::widgets::Widget;

#[derive(Debug, Clone)]
pub struct Rack<'a> {
    pub state: &'a RackState,
    pub sled_style: Style,
    pub sled_selected_style: Style,
    pub switch_style: Style,
    pub switch_selected_style: Style,
    pub power_shelf_style: Style,
    pub power_shelf_selected_style: Style,
    pub border_style: Style,
    pub border_selected_style: Style,
    pub border_hover_style: Style,
}

impl<'a> Rack<'a> {
    fn draw_sled(&self, buf: &mut Buffer, sled: &RectState, i: u8) {
        let mut block = Block::default()
            .title(format!("sled {}", i))
            .borders(borders(sled.rect.height));
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

    fn draw_switch(&self, buf: &mut Buffer, switch: &RectState, i: u8) {
        let mut block = Block::default()
            .title(format!("switch {}", i))
            .borders(borders(switch.rect.height));
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
                buf.get_mut(x, y).set_symbol("❒");
            }
        }
    }

    fn draw_power_shelf(
        &self,
        buf: &mut Buffer,
        power_shelf: &RectState,
        i: u8,
    ) {
        let mut block = Block::default()
            .title(format!("power {}", i))
            .borders(borders(power_shelf.rect.height));
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

// Each of the top and bottom borders take one line. The rendering looks
// better with all borders, but to save space, we don't draw the bottom
// border if we don't have 3 lines available.
fn borders(height: u16) -> Borders {
    if height < 3 {
        Borders::TOP | Borders::LEFT | Borders::RIGHT
    } else {
        Borders::ALL
    }
}

impl<'a> Widget for Rack<'a> {
    fn render(self, _area: Rect, buf: &mut Buffer) {
        for (id, rect) in self.state.component_rects.iter() {
            match id {
                ComponentId::Sled(i) => self.draw_sled(buf, rect, *i),
                ComponentId::Switch(i) => self.draw_switch(buf, rect, *i),
                ComponentId::Psc(i) => self.draw_power_shelf(buf, rect, *i),
            }
        }
    }
}

// Currently we only allow tabbing through the rack
const MAX_TAB_INDEX: u16 = 35;

// The visual state of the rack
#[derive(Debug)]
pub struct RackState {
    pub log: Option<Logger>,
    pub rect: Rect,
    pub component_rects: BTreeMap<ComponentId, RectState>,
    pub tab_index: TabIndex,
    pub tab_index_by_component_id: BTreeMap<ComponentId, TabIndex>,
    pub component_id_by_tab_index: BTreeMap<TabIndex, ComponentId>,
}

impl RackState {
    pub fn new() -> RackState {
        let mut state = RackState {
            log: None,
            rect: Rect::default(),
            component_rects: BTreeMap::new(),
            tab_index: TabIndex::new_unset(MAX_TAB_INDEX),
            tab_index_by_component_id: BTreeMap::new(),
            component_id_by_tab_index: BTreeMap::new(),
        };

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

        state.init_tab_index();
        state
    }

    pub fn update_tabbed(&mut self, val: bool) {
        if let Some(id) = self.component_id_by_tab_index.get(&self.tab_index) {
            self.component_rects.get_mut(&id).unwrap().tabbed = val;
        }
    }

    pub fn set_tab(&mut self, id: ComponentId) {
        self.update_tabbed(false);
        if let Some(tab_index) = self.tab_index_by_component_id.get(&id) {
            self.tab_index = *tab_index;
            self.update_tabbed(true);
        }
    }

    pub fn set_logger(&mut self, log: Logger) {
        self.log = Some(log);
    }

    pub fn get_current_component_id(&self) -> ComponentId {
        *self.component_id_by_tab_index.get(&self.tab_index).unwrap()
    }

    pub fn get_next_component_id(&self) -> ComponentId {
        let mut next = self.tab_index.next();
        *self.component_id_by_tab_index.get(&next).unwrap()
    }

    pub fn get_prev_component_id(&self) -> ComponentId {
        let mut prev = self.tab_index.prev();
        *self.component_id_by_tab_index.get(&prev).unwrap()
    }

    // We need to size the rack for large and small terminals
    // Width is not the issue, but height is.
    // We have the following constraints:
    //
    //  * Sleds, switches, and power shelves must be at least 2 lines high
    //    so they have a top border with label and black margin and one line for
    //    content drawing.
    //  * The top borders give a bottom border automatically to each component
    //    above them, and the top of the rack. However we also need a bottom
    //    border, so that's one more line.
    //
    // Therefore the minimum height of the rack is 2*16 + 2*2 + 2*2 + 1 = 41 lines.
    //
    // With a 5 line margin at the top, the minimum size of the terminal is 46
    // lines.
    //
    // If the terminal is smaller than 46 lines, the artistic rendering will
    // get progressively worse. XXX: We may want to bail on this instead.
    //
    // As much as possible we would like to also have a bottom margin, but we
    // don't sweat it.
    //
    // The calculations below follow from this logic
    pub fn resize(&mut self, rect: &Rect, margin: &Height) {
        // Give ourself room for a top margin
        let max_height = rect.height - margin.0;

        // Let's size our components
        let (rack_height, sled_height, other_height): (u16, u16, u16) =
            if max_height < 20 {
                panic!(
                    "Terminal size must be at least {} lines long",
                    20 + margin.0
                );
            } else if max_height < 37 {
                (21, 1, 1)
            } else if max_height < 41 {
                (37, 2, 1)
            } else if max_height < 56 {
                (41, 2, 2)
            } else if max_height < 60 {
                (56, 3, 2)
            } else if max_height < 80 {
                // 80 = 4*16 (sled bays) + 4*2 (power shelves) + 4*2 (switches)
                (60, 3, 3)
            } else {
                // Just divide evenly by 20 (16 sleds + 2 power shelves + 2 switches)
                let component_height = max_height / 20;
                (component_height * 20, component_height, component_height)
            };

        let mut rect = rect.clone();
        rect.height = rack_height;

        // Center the rack vertically as much as possible
        let extra_margin = (max_height - rack_height) / 2;
        rect.y = margin.0 + extra_margin;

        // Scale proportionally and center the rack horizontally
        let width = rect.width;
        rect.width = make_even(rack_height * 2 / 3);
        rect.x = width / 2 - rect.width / 2;

        let sled_width = rect.width / 2;

        // Save our rack rect for later
        self.rect = rect.clone();

        // Top Sleds
        for i in 0..16u8 {
            self.size_sled(i, &rect, sled_height, sled_width, other_height);
        }

        // Top Switch
        let switch =
            self.component_rects.get_mut(&ComponentId::Switch(0)).unwrap();
        switch.rect.y = rect.y + sled_height * 8;
        switch.rect.x = rect.x;
        switch.rect.height = other_height;
        switch.rect.width = sled_width * 2;

        // Power Shelves
        for i in [17, 18] {
            let shelf = self
                .component_rects
                .get_mut(&ComponentId::Psc(i - 17))
                .unwrap();
            shelf.rect.y =
                rect.y + sled_height * 8 + other_height * (i as u16 - 16);
            shelf.rect.x = rect.x;
            shelf.rect.height = other_height;
            shelf.rect.width = sled_width * 2;
        }

        // Bottom Switch
        let switch =
            self.component_rects.get_mut(&ComponentId::Switch(1)).unwrap();
        switch.rect.y = rect.y + sled_height * 8 + 3 * other_height;
        switch.rect.x = rect.x;
        switch.rect.height = other_height;
        switch.rect.width = sled_width * 2;

        // Bottom Sleds
        // We treat each non-sled as 2 sleds for layout purposes
        for i in 24..40 {
            self.size_sled(i, &rect, sled_height, sled_width, other_height);
        }
    }

    fn size_sled(
        &mut self,
        i: u8,
        rack: &Rect,
        sled_height: u16,
        sled_width: u16,
        other_height: u16,
    ) {
        // The power shelves and switches are in between in the layout
        let index = if i < 16 { i } else { i - 8 };
        let sled =
            self.component_rects.get_mut(&ComponentId::Sled(index)).unwrap();

        if index < 16 {
            sled.rect.y = rack.y + sled_height * (index as u16 / 2);
        } else {
            sled.rect.y =
                rack.y + sled_height * (index as u16 / 2) + other_height * 4;
        }

        if index % 2 == 0 {
            // left sled
            sled.rect.x = rack.x
        } else {
            // right sled
            sled.rect.x = rack.x + sled_width;
        }
        if (index == 30 || index == 31) && sled_height == 2 {
            // We saved space for a bottom border
            sled.rect.height = 3;
        } else {
            sled.rect.height = sled_height;
        }
        sled.rect.width = sled_width;
    }

    fn init_tab_index(&mut self) {
        for i in 0..MAX_TAB_INDEX {
            let tab_index = TabIndex::new(MAX_TAB_INDEX, i);
            let component_id = if i < 16 {
                ComponentId::Sled(i.try_into().unwrap())
            } else if i > 19 {
                ComponentId::Sled((i - 4).try_into().unwrap())
            } else if i == 16 {
                // Switches
                ComponentId::Switch(0)
            } else if i == 19 {
                ComponentId::Switch(1)
            } else if i == 17 || i == 18 {
                // Power Shelves
                // We actually want to return the active component here, so
                // we name it "psc X"
                ComponentId::Psc((i - 17).try_into().unwrap())
            } else {
                // If we add more items to tab through this will change
                unreachable!();
            };

            self.component_id_by_tab_index.insert(tab_index, component_id);
            self.tab_index_by_component_id.insert(component_id, tab_index);
        }
    }
}
