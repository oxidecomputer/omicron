// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A help button that brings up a help menu when selected

use super::Control;
use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::Style;
use tui::text::Text;
use tui::widgets::Block;
use tui::widgets::BorderType;
use tui::widgets::Borders;
use tui::widgets::Paragraph;
use tui::widgets::Widget;

#[derive(Debug)]
pub struct HelpButtonState {
    pub rect: Rect,
    pub hovered: bool,
    pub selected: bool,
}

impl HelpButtonState {
    pub fn new(x: u16, y: u16) -> HelpButtonState {
        HelpButtonState {
            rect: Rect { height: 5, width: 12, x, y },
            hovered: false,
            selected: false,
        }
    }
}

impl Control for HelpButtonState {
    fn rect(&self) -> Rect {
        self.rect
    }
}

#[derive(Debug)]
pub struct HelpButton<'a> {
    pub state: &'a HelpButtonState,
    pub style: Style,
    pub hovered_style: Style,
    pub selected_style: Style,
}

impl<'a> HelpButton<'a> {
    pub fn new(
        state: &'a HelpButtonState,
        style: Style,
        hovered_style: Style,
        selected_style: Style,
    ) -> HelpButton<'a> {
        HelpButton { state, style, hovered_style, selected_style }
    }
}

impl<'a> Widget for HelpButton<'a> {
    fn render(self, _: Rect, buf: &mut Buffer) {
        let text = Text::from("  Help  \n ━━━━━━━\n  ctrl-h ");
        let block_style = if self.state.hovered {
            self.hovered_style
        } else if self.state.selected {
            self.selected_style
        } else {
            self.style
        };
        let button = Paragraph::new(text).style(self.style).block(
            Block::default()
                .style(block_style)
                .borders(Borders::ALL)
                .border_type(BorderType::Double),
        );
        button.render(self.state.rect, buf);
    }
}
