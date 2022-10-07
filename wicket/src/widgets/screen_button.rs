// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A help button that brings up a help menu when selected

use super::Control;
use crate::ScreenId;
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
pub struct ScreenButtonState {
    pub rect: Rect,
    pub hovered: bool,
    pub screen_id: ScreenId,
}

impl ScreenButtonState {
    pub fn new(screen_id: ScreenId, x: u16, y: u16) -> ScreenButtonState {
        ScreenButtonState {
            rect: Rect { height: 5, width: Self::width(), x, y },
            hovered: false,
            screen_id,
        }
    }

    pub fn width() -> u16 {
        ScreenId::width() + 5
    }
}

impl Control for ScreenButtonState {
    fn rect(&self) -> Rect {
        self.rect
    }
}

#[derive(Debug)]
pub struct ScreenButton<'a> {
    pub state: &'a ScreenButtonState,
    pub style: Style,
    pub hovered_style: Style,
}

impl<'a> ScreenButton<'a> {
    pub fn new(
        state: &'a ScreenButtonState,
        style: Style,
        hovered_style: Style,
    ) -> ScreenButton<'a> {
        ScreenButton { state, style, hovered_style }
    }
}

impl<'a> Widget for ScreenButton<'a> {
    fn render(self, _: Rect, buf: &mut Buffer) {
        let name = self.state.screen_id.name();
        //        let margin = (ScreenButtonState::width() - 2 - name.len()) / 2;
        // Subtract borders
        let width = ScreenButtonState::width() as usize - 2;
        let text = Text::from(format!(
            "   Screen  \n ━━━━━━━━━━\n{:^width$}",
            name,
            width = width
        ));
        let block_style =
            if self.state.hovered { self.hovered_style } else { self.style };
        let button = Paragraph::new(text).style(self.style).block(
            Block::default()
                .style(block_style)
                .borders(Borders::ALL)
                .border_type(BorderType::Double),
        );
        button.render(self.state.rect, buf);
    }
}
