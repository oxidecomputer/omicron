// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A help button that brings up a help menu when selected

use super::get_control_id;
use super::Control;
use super::ControlId;
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
    control_id: ControlId,
    pub rect: Rect,
    pub screen_id: ScreenId,
}

impl ScreenButtonState {
    pub fn new(screen_id: ScreenId) -> ScreenButtonState {
        // Location will get set via [`Screen::resize`]
        ScreenButtonState {
            control_id: get_control_id(),
            rect: Rect { height: 5, width: Self::width(), x: 0, y: 0 },
            screen_id,
        }
    }

    pub fn width() -> u16 {
        ScreenId::width() + 5
    }
}

impl Control for ScreenButtonState {
    fn id(&self) -> ControlId {
        self.control_id
    }

    fn rect(&self) -> Rect {
        self.rect
    }
}

#[derive(Debug)]
pub struct ScreenButton<'a> {
    pub state: &'a ScreenButtonState,
    pub style: Style,
    pub border_style: Style,
}

impl<'a> ScreenButton<'a> {
    pub fn new(
        state: &'a ScreenButtonState,
        style: Style,
        border_style: Style,
    ) -> ScreenButton<'a> {
        ScreenButton { state, style, border_style }
    }
}

impl<'a> Widget for ScreenButton<'a> {
    fn render(self, _: Rect, buf: &mut Buffer) {
        let name = self.state.screen_id.name();
        // Subtract borders
        let width = ScreenButtonState::width() as usize - 2;
        let text = Text::from(format!(
            "{:^width$}\n ━━━━━━━━━━\n{:^width$}",
            name,
            "ctrl-r",
            width = width,
        ));
        let button = Paragraph::new(text).style(self.style).block(
            Block::default()
                .style(self.border_style)
                .borders(Borders::ALL)
                .border_type(BorderType::Double),
        );
        button.render(self.state.rect, buf);
    }
}
