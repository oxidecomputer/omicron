// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The menu bar at the top of each screen

use super::HelpMenu;
use crate::screens::TabIndex;
use tui::buffer::Buffer;
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::style::Style;
use tui::text::Text;
use tui::widgets::Block;
use tui::widgets::Paragraph;
use tui::widgets::Widget;

#[derive(Debug)]
pub struct HamburgerState {
    pub tab_index: TabIndex,
    pub rect: Rect,
    pub tabbed: bool,
    pub hovered: bool,
    pub selected: bool,
}

impl HamburgerState {
    pub fn new(tab_index: TabIndex) -> HamburgerState {
        HamburgerState {
            tab_index,
            rect: Rect { height: 3, width: 4, x: 1, y: 0 },
            tabbed: false,
            hovered: false,
            selected: false,
        }
    }
}

#[derive(Debug)]
pub struct MenuBar<'a> {
    pub hamburger_state: &'a HamburgerState,
    pub title: &'a str,
    pub style: Style,
    pub selected_style: Style,
    pub hovered_style: Style,
    pub help_menu_style: Style,
    pub help_menu_command_style: Style,
}

impl<'a> MenuBar<'a> {
    fn draw_title(&self, mut rect: Rect, buf: &mut Buffer) {
        rect.height = 1;
        rect.y = 1;
        let title_block = Block::default()
            .style(self.style)
            .title(self.title)
            .title_alignment(Alignment::Center);
        title_block.render(rect, buf);
    }

    fn draw_hamburger(&self, mut rect: Rect, buf: &mut Buffer) {
        if self.hamburger_state.selected {
            return;
        }
        rect.height = 3;
        rect.width = 4;
        rect.x = 1;

        let mut text = Text::from("▄▄▄▄\n▄▄▄▄\n▄▄▄▄\n");
        if self.hamburger_state.tabbed {
            text.patch_style(self.selected_style);
        } else if self.hamburger_state.hovered {
            text.patch_style(self.hovered_style);
        } else {
            text.patch_style(self.style);
        }

        let hamburger = Paragraph::new(text);
        hamburger.render(rect, buf);
    }

    fn draw_help_menu(&self, rect: Rect, buf: &mut Buffer) {
        let menu = HelpMenu {
            style: self.help_menu_style,
            command_style: self.help_menu_command_style,
        };
        menu.render(rect, buf);
    }
}

impl<'a> Widget for MenuBar<'a> {
    fn render(self, rect: Rect, buf: &mut Buffer) {
        self.draw_title(rect, buf);
        self.draw_hamburger(rect, buf);
        if self.hamburger_state.selected {
            self.draw_help_menu(rect, buf);
        }
    }
}
