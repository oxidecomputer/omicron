// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Custom tui widgets

use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::Style;

mod animated_logo;
mod banner;
mod component;
mod help_menu;
mod menubar;
mod rack;

pub use animated_logo::{Logo, LogoState, LOGO_HEIGHT, LOGO_WIDTH};
pub use banner::Banner;
pub use component::{ComponentModal, ComponentModalState};
pub use help_menu::HelpMenu;
pub use menubar::{HamburgerState, MenuBar};
pub use rack::{Rack, RackState};

// Set the buf area to the bg color
pub fn clear_buf(area: Rect, buf: &mut Buffer, style: Style) {
    for x in area.left()..area.right() {
        for y in area.top()..area.bottom() {
            buf.get_mut(x, y).set_style(style).set_symbol(" ");
        }
    }
}
