// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A rendering of the Oxide rack

use crate::inventory::ComponentId;
use crate::screens::TabIndex;
use crate::{Action, Event, Frame, State};
use crossterm::event::Event as TermEvent;
use slog::Logger;
use std::collections::BTreeMap;
use tui::buffer::Buffer;
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::style::Color;
use tui::style::Style;
use tui::text::Text;
use tui::widgets::Block;
use tui::widgets::Borders;
use tui::widgets::Paragraph;
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
}

impl<'a> Rack<'a> {
    fn draw_sled(&self, buf: &mut Buffer, sled: Rect, i: u8) {
        let component_id = ComponentId::Sled(i);
        let mut block = Block::default()
            .title(format!("sled {}", i))
            .borders(borders(sled.height));
        if self.state.tabbed == component_id {
            block = block
                .style(self.sled_selected_style)
                .border_style(self.border_selected_style);
        } else {
            block =
                block.style(self.sled_style).border_style(self.border_style);
        }

        let inner = block.inner(sled);
        block.render(sled, buf);

        // Draw some U.2 bays
        // TODO: Draw 10 only? - That may not scale down as well
        for x in inner.left()..inner.right() {
            for y in inner.top()..inner.bottom() {
                let cell = buf.get_mut(x, y).set_symbol("▕");
                if self.state.tabbed == component_id {
                    if let Some(KnightRiderMode { count }) =
                        self.state.knight_rider_mode
                    {
                        let go_right = (count / inner.width) % 2 == 0;
                        let offset = if go_right {
                            count % inner.width
                        } else {
                            inner.width - (count % inner.width)
                        };
                        if x == (inner.left() + offset) {
                            cell.set_bg(Color::Red);
                        }
                    }
                    if let Some(color) = self.sled_selected_style.fg {
                        cell.set_fg(color);
                    }
                } else if let Some(color) = self.sled_style.fg {
                    cell.set_fg(color);
                }
            }
        }
    }

    fn draw_switch(&self, buf: &mut Buffer, switch: Rect, i: u8) {
        let component_id = ComponentId::Switch(i);
        let mut block = Block::default()
            .title(format!("switch {}", i))
            .borders(borders(switch.height));
        if self.state.tabbed == component_id {
            block = block
                .style(self.switch_selected_style)
                .border_style(self.border_selected_style);
        } else {
            block =
                block.style(self.switch_style).border_style(self.border_style);
        }

        let inner = block.inner(switch);
        block.render(switch, buf);

        for x in inner.left()..inner.right() {
            for y in inner.top()..inner.bottom() {
                buf.get_mut(x, y).set_symbol("❒");
            }
        }
    }

    fn draw_power_shelf(&self, buf: &mut Buffer, power_shelf: Rect, i: u8) {
        let component_id = ComponentId::Psc(i);
        let mut block = Block::default()
            .title(format!("power {}", i))
            .borders(borders(power_shelf.height));
        if self.state.tabbed == component_id {
            block = block
                .style(self.power_shelf_selected_style)
                .border_style(self.border_selected_style);
        } else {
            block = block
                .style(self.power_shelf_style)
                .border_style(self.border_style);
        }

        let inner = block.inner(power_shelf);
        block.render(power_shelf, buf);

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
    fn render(self, rect: Rect, buf: &mut Buffer) {
        match self.state.resize(rect) {
            ComponentRects::Displayed { rects_map, .. } => {
                for (id, rect) in rects_map {
                    match id {
                        ComponentId::Sled(i) => self.draw_sled(buf, rect, i),
                        ComponentId::Switch(i) => {
                            self.draw_switch(buf, rect, i)
                        }
                        ComponentId::Psc(i) => {
                            self.draw_power_shelf(buf, rect, i)
                        }
                    }
                }
            }
            ComponentRects::WindowTooShort { text, text_rect } => {
                text.clone().render(text_rect, buf);
            }
        }
    }
}

// Currently we only allow tabbing through the rack
// There are 36 entries (0 - 35)
const MAX_TAB_INDEX: u16 = 35;

// Easter egg alert: Support for Knight Rider mode
#[derive(Debug, Default)]
pub struct KnightRiderMode {
    count: u16,
}

impl KnightRiderMode {
    pub fn step(&mut self) {
        self.count += 1;
    }
}

// The visual state of the rack
#[derive(Debug)]
pub struct RackState {
    pub log: Option<Logger>,
    pub tabbed: ComponentId,
    pub tab_index: TabIndex,
    pub tab_index_by_component_id: BTreeMap<ComponentId, TabIndex>,
    pub component_id_by_tab_index: BTreeMap<TabIndex, ComponentId>,
    pub knight_rider_mode: Option<KnightRiderMode>,

    // Useful for arrow based navigation. When we cross the switches going up
    // or down the rack we want to stay in the same column. This allows a user
    // to repeatedly hit up or down arrow and stay in the same column, as they
    // would expect.
    //
    // This is the only part of the state that is "historical", and doesn't
    // rely on the current TabIndex/ComponentId at all times.
    left_column: bool,
}

#[allow(clippy::new_without_default)]
impl RackState {
    pub fn new() -> RackState {
        let mut state = RackState {
            log: None,
            tabbed: ComponentId::Sled(0),
            tab_index: TabIndex::new(MAX_TAB_INDEX, 0),
            tab_index_by_component_id: BTreeMap::new(),
            component_id_by_tab_index: BTreeMap::new(),
            knight_rider_mode: None,

            // Default to the left column, where sled 0 lives
            left_column: true,
        };

        state.init_tab_index();
        state
    }

    pub fn toggle_knight_rider_mode(&mut self) {
        if self.knight_rider_mode.is_none() {
            self.knight_rider_mode = Some(KnightRiderMode::default());
        } else {
            self.knight_rider_mode = None;
        }
    }

    pub fn up(&mut self) {
        self.set_column();

        match self.get_current_component_id() {
            // Up the left side
            ComponentId::Switch(1) if self.left_column => self.inc_tab_index(),

            // Up the middle (right side if sled 15)
            ComponentId::Sled(15)
            | ComponentId::Psc(_)
            | ComponentId::Switch(0) => {
                self.inc_tab_index();
            }

            // Up the current side (always right side if switch 1)
            ComponentId::Sled(_) | ComponentId::Switch(_) => {
                self.inc_tab_index();
                self.inc_tab_index();
            }
        }
    }

    pub fn down(&mut self) {
        self.set_column();

        match self.get_current_component_id() {
            // Down the left side
            ComponentId::Switch(0) if !self.left_column => self.dec_tab_index(),

            // Down the middle (right side if sled 16)
            ComponentId::Sled(16)
            | ComponentId::Psc(_)
            | ComponentId::Switch(1) => {
                self.dec_tab_index();
            }

            // Up the current side (always right side if switch 0)
            ComponentId::Sled(_) | ComponentId::Switch(_) => {
                self.dec_tab_index();
                self.dec_tab_index();
            }
        }
    }

    pub fn left_or_right_arrow(&mut self) {
        self.set_column();

        match self.get_current_component_id() {
            ComponentId::Sled(_) => {
                if self.left_column {
                    self.inc_tab_index();
                } else {
                    self.dec_tab_index();
                }
            }
            _ => (),
        }
    }

    fn set_column(&mut self) {
        match self.tabbed {
            ComponentId::Sled(i) => self.left_column = i % 2 == 0,
            _ => (),
        }
    }

    pub fn inc_tab_index(&mut self) {
        self.tab_index.inc();
        self.tabbed = self.get_current_component_id();
    }

    pub fn dec_tab_index(&mut self) {
        self.tab_index.dec();
        self.tabbed = self.get_current_component_id();
    }

    pub fn set_logger(&mut self, log: Logger) {
        self.log = Some(log);
    }

    pub fn get_current_component_id(&self) -> ComponentId {
        *self.component_id_by_tab_index.get(&self.tab_index).unwrap()
    }

    pub fn get_next_component_id(&self) -> ComponentId {
        let next = self.tab_index.next();
        *self.component_id_by_tab_index.get(&next).unwrap()
    }

    pub fn get_prev_component_id(&self) -> ComponentId {
        let prev = self.tab_index.prev();
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
    // With a 5 line margin at the top and a 2 line margin at the bottom, the
    // minimum size of the terminal is 48 lines.
    //
    // If the terminal is smaller than 46 lines, the artistic rendering will
    // get progressively worse. XXX: We may want to bail on this instead.
    //
    // As much as possible we would like to also have a bottom margin, but we
    // don't sweat it.
    //
    // The calculations below follow from this logic
    fn resize(&self, rect: Rect) -> ComponentRects {
        let max_height = rect.height;

        // Let's size our components:
        let (rack_height, sled_height, other_height): (u16, u16, u16) =
            if max_height < 20 {
                // The window is too short.
                let text = Paragraph::new(Text::raw(format!(
                    "[window too short: resize to at least {} lines high]",
                    20
                )))
                .alignment(Alignment::Center);
                // Center vertically.
                let text_rect =
                    Rect { y: rect.y + max_height / 2, height: 1, ..rect };
                return ComponentRects::WindowTooShort { text, text_rect };
            } else if max_height < 37 {
                (20, 1, 1)
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

        let mut rack_rect = Rect { height: rack_height, ..rect };

        // Center the rack vertically as much as possible
        let extra_margin = (max_height - rack_height) / 2;
        rack_rect.y += extra_margin;

        // Scale proportionally and center the rack horizontally
        rack_rect.width = make_even(rack_height * 2 / 3);
        rack_rect.x += rect.width / 2 - rack_rect.width / 2;

        let sled_width = rack_rect.width / 2;

        let mut rects_map = ComponentRectsMap::new();

        // Top Sleds
        for i in 0..16u8 {
            Self::size_sled(
                &mut rects_map,
                i,
                &rack_rect,
                sled_height,
                sled_width,
                other_height,
            );
        }

        // Top Switch
        let top_switch_rect = Rect::new(
            rack_rect.x,
            rack_rect.y + sled_height * 8,
            sled_width * 2,
            other_height,
        );
        rects_map.insert(ComponentId::Switch(1), top_switch_rect);

        // Power Shelves
        for i in [17, 18] {
            let shelf_rect = Rect {
                x: rack_rect.x,
                y: rack_rect.y
                    + sled_height * 8
                    + other_height * (i as u16 - 16),
                width: sled_width * 2,
                height: other_height,
            };
            rects_map.insert(ComponentId::Psc(18 - i), shelf_rect);
        }

        // Bottom Switch
        let bottom_switch_rect = Rect {
            x: rack_rect.x,
            y: rack_rect.y + sled_height * 8 + 3 * other_height,
            width: sled_width * 2,
            height: other_height,
        };

        rects_map.insert(ComponentId::Switch(0), bottom_switch_rect);

        // Bottom Sleds
        // We treat each non-sled as 2 sleds for layout purposes
        for i in 24..40 {
            Self::size_sled(
                &mut rects_map,
                i,
                &rack_rect,
                sled_height,
                sled_width,
                other_height,
            );
        }

        return ComponentRects::Displayed { rack_rect, rects_map };
    }

    // Sleds are numbered bottom-to-top and left-to-rigth
    // See https://rfd.shared.oxide.computer/rfd/0200#_number_ordering
    fn size_sled(
        rects_map: &mut ComponentRectsMap,
        i: u8,
        rack: &Rect,
        sled_height: u16,
        sled_width: u16,
        other_height: u16,
    ) {
        // The power shelves and switches are in between in the layout
        let index = if i < 16 { i } else { i - 8 };
        let mut sled_index = 31 - index;
        // Swap left and right
        if sled_index & 1 == 1 {
            sled_index -= 1;
        } else {
            sled_index += 1;
        }

        let x = if index % 2 == 0 {
            // left sled
            rack.x
        } else {
            // right sled
            rack.x + sled_width
        };
        let y = if index < 16 {
            rack.y + sled_height * (index as u16 / 2)
        } else {
            rack.y + sled_height * (index as u16 / 2) + other_height * 4
        };
        let height = if (index == 30 || index == 31) && sled_height == 2 {
            // We saved space for a bottom border
            3
        } else {
            sled_height
        };

        let sled_rect = Rect { x, y, width: sled_width, height };
        rects_map.insert(ComponentId::Sled(sled_index), sled_rect);
    }

    // We tab bottom-to-top, left-to-right, same as component numbering
    fn init_tab_index(&mut self) {
        for i in 0..=MAX_TAB_INDEX as u8 {
            let tab_index = TabIndex::new(MAX_TAB_INDEX, i as u16);
            let component_id = if i < 16 {
                ComponentId::Sled(i)
            } else if i > 19 {
                ComponentId::Sled(i - 4)
            } else if i == 16 {
                // Switches
                ComponentId::Switch(0)
            } else if i == 19 {
                ComponentId::Switch(1)
            } else if i == 17 || i == 18 {
                // Power Shelves
                // We actually want to return the active component here, so
                // we name it "psc X"
                ComponentId::Psc(i - 17)
            } else {
                // If we add more items to tab through this will change
                unreachable!();
            };

            self.component_id_by_tab_index.insert(tab_index, component_id);
            self.tab_index_by_component_id.insert(component_id, tab_index);
        }
    }
}

#[derive(Clone, Debug)]
enum ComponentRects {
    Displayed {
        /// The enclosing rect for the rack.
        rack_rect: Rect,
        /// The individual rects.
        rects_map: ComponentRectsMap,
    },

    /// The window is too short and needs to be resized.
    WindowTooShort { text: Paragraph<'static>, text_rect: Rect },
}

type ComponentRectsMap = BTreeMap<ComponentId, Rect>;

/// Ensure that a u16 is an even number by adding 1 if necessary.
pub fn make_even(val: u16) -> u16 {
    if val % 2 == 0 {
        val
    } else {
        val + 1
    }
}
