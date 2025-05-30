// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A rendering of the Oxide rack

use crate::state::Inventory;
use crate::state::{ComponentId, KnightRiderMode, RackState};
use ratatui::buffer::Buffer;
use ratatui::layout::Alignment;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Style;
use ratatui::text::Text;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Widget;
use std::collections::BTreeMap;
use wicket_common::inventory::SpIgnition;

#[derive(Debug, Clone)]
pub struct Rack<'a> {
    pub inventory: &'a Inventory,
    pub state: &'a RackState,
    pub suspicious_style: Style,
    pub not_present_style: Style,
    pub sled_style: Style,
    pub sled_selected_style: Style,
    pub switch_style: Style,
    pub switch_selected_style: Style,
    pub power_shelf_style: Style,
    pub power_shelf_selected_style: Style,
    pub border_style: Style,
    pub border_selected_style: Style,
}

impl Rack<'_> {
    fn draw_sled(&self, buf: &mut Buffer, sled: Rect, i: u8) {
        let component_id = ComponentId::Sled(i);
        let presence =
            ComponentPresence::for_component(self.inventory, &component_id);
        let mut block = Block::default()
            .title(format!("SLD{}", i))
            .borders(borders(sled.height));
        if self.state.selected == component_id {
            block = block
                .style(self.sled_selected_style)
                .border_style(self.border_selected_style);
        } else {
            let style = match presence {
                ComponentPresence::Present => self.sled_style,
                ComponentPresence::NotPresent => self.not_present_style,
                ComponentPresence::Suspicious => self.suspicious_style,
            };

            block = block.style(style).border_style(self.border_style);
        }

        let inner = block.inner(sled);
        block.render(sled, buf);

        if presence == ComponentPresence::Present {
            // Draw some U.2 bays
            // TODO: Draw 10 only? - That may not scale down as well
            for x in inner.left()..inner.right() {
                for y in inner.top()..inner.bottom() {
                    let cell = buf[(x, y)].set_symbol("▕");
                    if self.state.selected == component_id {
                        if let Some(KnightRiderMode { count }) =
                            self.state.knight_rider_mode
                        {
                            let width = inner.width as usize;
                            let go_right = (count / width) % 2 == 0;
                            let offset = if go_right {
                                count % width
                            } else {
                                width - (count % width)
                            } as u16;
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
    }

    fn draw_switch(&self, buf: &mut Buffer, switch: Rect, i: u8) {
        let component_id = ComponentId::Switch(i);
        let presence =
            ComponentPresence::for_component(self.inventory, &component_id);
        let mut block = Block::default()
            .title(format!("SW{}", i))
            .borders(borders(switch.height));
        if self.state.selected == component_id {
            block = block
                .style(self.switch_selected_style)
                .border_style(self.border_selected_style);
        } else {
            let style = match presence {
                ComponentPresence::Present => self.switch_style,
                ComponentPresence::NotPresent => self.not_present_style,
                ComponentPresence::Suspicious => self.suspicious_style,
            };
            block = block.style(style).border_style(self.border_style);
        }

        let inner = block.inner(switch);
        block.render(switch, buf);

        if presence == ComponentPresence::Present {
            for x in inner.left()..inner.right() {
                for y in inner.top()..inner.bottom() {
                    buf[(x, y)].set_symbol("❒");
                }
            }
        }
    }

    fn draw_power_shelf(&self, buf: &mut Buffer, power_shelf: Rect, i: u8) {
        let component_id = ComponentId::Psc(i);
        let presence =
            ComponentPresence::for_component(self.inventory, &component_id);
        let mut block = Block::default()
            .title(format!("PWR{}", i))
            .borders(borders(power_shelf.height));
        if self.state.selected == component_id {
            block = block
                .style(self.power_shelf_selected_style)
                .border_style(self.border_selected_style);
        } else {
            let style = match presence {
                ComponentPresence::Present => self.power_shelf_style,
                ComponentPresence::NotPresent => self.not_present_style,
                ComponentPresence::Suspicious => self.suspicious_style,
            };
            block = block.style(style).border_style(self.border_style);
        }

        let inner = block.inner(power_shelf);
        block.render(power_shelf, buf);

        if presence == ComponentPresence::Present {
            let width = inner.right() - inner.left();
            let step = width / 6;
            let border = (width - step * 6) / 2;

            for x in inner.left() + border..inner.right() - border {
                for y in inner.top()..inner.bottom() {
                    if x % step != 0 {
                        buf[(x, y)].set_symbol("█");
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComponentPresence {
    // Ignition says the device is present, and we're able to talk to it.
    Present,
    // Ignition says the device is not present, and we're not able to talk to
    // it.
    NotPresent,
    // Something is wrong: ignition says it is present but we can't talk to it,
    // or (less likely) the opposite.
    Suspicious,
}

impl ComponentPresence {
    fn for_component(inventory: &Inventory, component: &ComponentId) -> Self {
        let sp = match inventory.get_inventory(component) {
            Some(component) => component.sp(),
            None => return Self::NotPresent,
        };

        match (sp.ignition(), sp.state().is_some()) {
            // No ignition and no state = no sled.
            (None, false) => Self::NotPresent,
            // No ignition but we have state - suspect!
            (None, true) => Self::Suspicious,
            (Some(ignition), false) => match ignition {
                // No ignition and no state = no sled.
                SpIgnition::No => Self::NotPresent,
                // Ignition says it's present but we have no state - suspect!
                SpIgnition::Yes { .. } => Self::Suspicious,
            },
            (Some(ignition), true) => match ignition {
                // No ignition but we have state - suspect!
                SpIgnition::No => Self::Suspicious,
                // Ignition and state = sled present.
                SpIgnition::Yes { .. } => Self::Present,
            },
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

impl Widget for Rack<'_> {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        match resize(rect) {
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

// We need to size the rack for large and small terminals
// Width is not the issue, but height is.
//
// See inline comments for calculations.
fn resize(rect: Rect) -> ComponentRects {
    let max_height = rect.height;

    // Let's size our components:
    let (rack_height, sled_height, other_height): (u16, u16, u16) =
        if max_height < 20 {
            // The window is too short.
            let text = Box::new(
                Paragraph::new(Text::raw("[window too short]"))
                    .alignment(Alignment::Center),
            );
            // Center vertically.
            let text_rect =
                Rect { y: rect.y + max_height / 2, height: 1, ..rect };
            return ComponentRects::WindowTooShort { text, text_rect };
        } else if max_height < 37 {
            // 20 lines is just about enough to show a (very degraded)
            // representation of the rack, since:
            // 20 = 1*16 (sled bays) + 2*1 (power shelves) + 1*2 (switches)
            (20, 1, 1)
        } else if max_height < 41 {
            // 37 = 2*16 + 1*2 + 1*2 + 1 line drawn below the bottom sled
            (37, 2, 1)
        } else if max_height < 56 {
            // 41 = 2*16 + 2*2 + 2*2 + 1 line drawn below the bottom sled
            (41, 2, 2)
        } else if max_height < 60 {
            // 56 = 3*16 + 2*2 + 2*2
            (56, 3, 2)
        } else if max_height < 80 {
            // 60 = 3*16 + 3*2 + 3*2
            (60, 3, 3)
        } else {
            // 80 = 4*16 + 4*2 + 4*2
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
        size_sled(
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
                + other_height * (u16::from(i) - 16),
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
        size_sled(
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
        rack.y + sled_height * (u16::from(index) / 2)
    } else {
        rack.y + sled_height * (u16::from(index) / 2) + other_height * 4
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

#[derive(Clone, Debug)]
enum ComponentRects {
    Displayed {
        #[allow(unused)]
        /// The enclosing rect for the rack.
        rack_rect: Rect,
        /// The individual rects.
        rects_map: ComponentRectsMap,
    },

    /// The window is too short and needs to be resized.
    WindowTooShort { text: Box<Paragraph<'static>>, text_rect: Rect },
}

type ComponentRectsMap = BTreeMap<ComponentId, Rect>;

/// Ensure that a u16 is an even number by adding 1 if necessary.
pub fn make_even(val: u16) -> u16 {
    if val % 2 == 0 { val } else { val + 1 }
}
