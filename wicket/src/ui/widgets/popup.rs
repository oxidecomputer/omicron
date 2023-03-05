// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A popup dialog box widget

use tui::{
    buffer::Buffer,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Span, Spans, Text},
    widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget, Wrap},
};

use std::cmp::{max, min};

use crate::ui::defaults::dimensions::RectExt;
use crate::ui::defaults::{colors::*, style};
use crate::ui::widgets::{BoxConnector, BoxConnectorKind, Fade};

pub struct ButtonText<'a> {
    instruction: &'a str,
    key: &'a str,
}

#[derive(Default)]
pub struct Popup<'a> {
    pub header: Text<'a>,
    pub body: Text<'a>,
    pub buttons: Vec<ButtonText<'a>>,
}

impl Popup<'_> {
    pub fn height(&self) -> u16 {
        let button_height: u16 = if self.buttons.is_empty() { 0 } else { 3 };
        let bottom_margin: u16 = 1;
        let borders: u16 = 3;
        u16::try_from(self.header.height()).unwrap()
            + u16::try_from(self.body.height()).unwrap()
            + button_height
            + bottom_margin
            + borders
    }

    pub fn width(&self) -> u16 {
        let borders: u16 = 2;
        let right_margin: u16 = 3;
        u16::try_from(self.body.width()).unwrap() + borders + right_margin
    }
}

impl Widget for Popup<'_> {
    fn render(self, full_screen: Rect, buf: &mut Buffer) {
        let fade = Fade {};
        fade.render(full_screen, buf);

        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .style(style::selected_line());

        let rect = full_screen
            .center_horizontally(self.width())
            .center_vertically(self.height());

        // Clear the popup
        Clear.render(rect, buf);
        Block::default()
            .style(Style::default().bg(TUI_BLACK).fg(TUI_BLACK))
            .render(rect, buf);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(0)].as_ref())
            .split(rect);

        let header = Paragraph::new(self.header).block(block.clone());
        header.render(chunks[0], buf);

        let body = Paragraph::new(self.body).block(
            block.borders(Borders::BOTTOM | Borders::LEFT | Borders::RIGHT),
        );
        body.render(chunks[1], buf);

        let connector = BoxConnector::new(BoxConnectorKind::Top);
        connector.render(chunks[1], buf);

        // TODO Layout and render buttons
    }
}
