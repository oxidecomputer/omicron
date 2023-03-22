// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A rendering of the Oxide rack

use crate::state::{ComponentId, KnightRiderMode, RackState};
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
            .title(format!("SLD{}", i))
            .borders(borders(sled.height));
        if self.state.selected == component_id {
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

    fn draw_switch(&self, buf: &mut Buffer, switch: Rect, i: u8) {
        let component_id = ComponentId::Switch(i);
        let mut block = Block::default()
            .title(format!("SW{}", i))
            .borders(borders(switch.height));
        if self.state.selected == component_id {
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
        assert_eq!(i, 0, "current display only supports one power shelf");
        let component_id = ComponentId::Psc(i);
        let mut block =
            Block::default().title("PWR").borders(borders(power_shelf.height));
        if self.state.selected == component_id {
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
        if max_height < 19 {
            // The window is too short.
            let text = Paragraph::new(Text::raw("[window too short]"))
                .alignment(Alignment::Center);
            // Center vertically.
            let text_rect =
                Rect { y: rect.y + max_height / 2, height: 1, ..rect };
            return ComponentRects::WindowTooShort { text, text_rect };
        } else if max_height < 36 {
            // 19 lines is just about enough to show a (very degraded)
            // representation of the rack, since:
            // 19 = 1*16 (sled bays) + 1*1 (power shelf) + 1*2 (switches)
            (19, 1, 1)
        } else if max_height < 39 {
            // 36 = 2*16 + 1*1 + 1*2 + 1 line drawn below the bottom sled
            (36, 2, 1)
        } else if max_height < 54 {
            // 39 = 2*16 + 2*1 + 2*2 + 1 line drawn below the bottom sled
            (39, 2, 2)
        } else if max_height < 57 {
            // 54 = 3*16 + 2*1 + 2*2
            (54, 3, 2)
        } else if max_height < 76 {
            // 57 = 3*16 + 3*1 + 3*2
            (57, 3, 3)
        } else {
            // 76 = 4*16 + 4*1 + 4*2
            // Just divide evenly by 20 (16 sleds + 1 power shelf + 2 switches)
            let component_height = max_height / 19;
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

    // Power Shelves. Current Oxide designs only use one power shelf (item 17),
    // so skip the other one (18).
    //
    // When a second power shelf is added to the UI, all the y indexes below
    // this point will have to be increased by other_height.
    let shelf_rect = Rect {
        x: rack_rect.x,
        y: rack_rect.y + sled_height * 8 + other_height,
        width: sled_width * 2,
        height: other_height,
    };
    rects_map.insert(ComponentId::Psc(0), shelf_rect);

    // Bottom Switch
    let bottom_switch_rect = Rect {
        x: rack_rect.x,
        y: rack_rect.y + sled_height * 8 + 2 * other_height,
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
    // The power shelf and switches are in between in the layout
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
        rack.y + sled_height * (index as u16 / 2) + other_height * 3
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
