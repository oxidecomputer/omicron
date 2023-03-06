// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A popup dialog box widget

use tui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Span, Spans, Text},
    widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget},
};

use crate::ui::defaults::dimensions::RectExt;
use crate::ui::defaults::{colors::*, style};
use crate::ui::widgets::{BoxConnector, BoxConnectorKind, Fade};
use std::iter;

pub struct ButtonText<'a> {
    pub instruction: &'a str,
    pub key: &'a str,
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

        draw_buttons(self.buttons, chunks[1], buf);
    }
}
pub fn draw_buttons(
    buttons: Vec<ButtonText<'_>>,
    body_rect: Rect,
    buf: &mut Buffer,
) {
    // Enough space at the bottom for buttons and margin
    let button_and_margin_height = 4;
    let mut rect = body_rect;
    rect.y = (body_rect.y + body_rect.height)
        .saturating_sub(button_and_margin_height);
    rect.height = 3;

    let brackets = 2;
    let margin = 2;
    let borders = 2;

    // The first constraint right aligns the buttons
    // The buttons themselves are sized according to their contents
    let constraints: Vec<_> = iter::once(Constraint::Min(0))
        .chain(buttons.iter().map(|b| {
            Constraint::Length(
                u16::try_from(
                    b.instruction.len()
                        + b.key.len()
                        + brackets
                        + margin
                        + borders
                        + 1,
                )
                .unwrap(),
            )
        }))
        .collect();

    let button_rects = Layout::default()
        .direction(Direction::Horizontal)
        .horizontal_margin(2)
        .constraints(constraints.as_ref())
        .split(rect);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .style(style::selected_line());

    for (i, button) in buttons.into_iter().enumerate() {
        let b = Paragraph::new(Spans::from(vec![
            Span::raw(" "),
            Span::styled(button.instruction, style::selected()),
            Span::styled(format!(" <{}> ", button.key), style::selected_line()),
        ]))
        .block(block.clone());
        b.render(button_rects[i + 1], buf);
    }
}
