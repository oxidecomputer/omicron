// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A help button that brings up a [`HelpMenu`] when selected

use super::get_control_id;
use super::Control;
use super::ControlId;
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
    control_id: ControlId,
    pub rect: Rect,
}

impl HelpButtonState {
    pub fn new(x: u16, y: u16) -> HelpButtonState {
        HelpButtonState {
            control_id: get_control_id(),
            rect: Rect { height: 5, width: 12, x, y },
        }
    }
}

impl Control for HelpButtonState {
    fn id(&self) -> ControlId {
        self.control_id
    }

    fn rect(&self) -> Rect {
        self.rect
    }
}

#[derive(Debug)]
pub struct HelpButton<'a> {
    pub state: &'a HelpButtonState,
    pub style: Style,
    pub border_style: Style,
}

impl<'a> HelpButton<'a> {
    pub fn new(
        state: &'a HelpButtonState,
        style: Style,
        border_style: Style,
    ) -> HelpButton<'a> {
        HelpButton { state, style, border_style }
    }
}

impl<'a> Widget for HelpButton<'a> {
    fn render(self, _: Rect, buf: &mut Buffer) {
        let text = Text::from("  Help  \n ━━━━━━━\n  ctrl-h ");
        let button = Paragraph::new(text).style(self.style).block(
            Block::default()
                .style(self.border_style)
                .borders(Borders::ALL)
                .border_type(BorderType::Double),
        );
        button.render(self.state.rect, buf);
    }
}
