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

const BUTTON_HEIGHT: u16 = 3;

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
        let button_height: u16 =
            if self.buttons.is_empty() { 0 } else { BUTTON_HEIGHT };
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
        let body_width =
            u16::try_from(self.body.width()).unwrap() + borders + right_margin;
        let header_width = u16::try_from(self.header.width()).unwrap()
            + borders
            + right_margin;
        let width = u16::max(body_width, header_width);
        u16::max(width, self.button_width())
    }

    pub fn button_width(&self) -> u16 {
        let space_between_buttons = 1;
        let margins = 4;
        // Margin + space + angle brackets
        let button_extras = 6;
        let width = self.buttons.iter().fold(margins, |acc, text| {
            acc + text.instruction.len()
                + text.key.len()
                + button_extras
                + space_between_buttons
        });
        u16::try_from(width).unwrap()
    }

    /// Returns the maximum width that this popup can have, including outer
    /// borders.
    ///
    /// This is currently 80% of screen width.
    pub fn max_width(full_screen_width: u16) -> u16 {
        (full_screen_width as u32 * 4 / 5) as u16
    }

    /// Returns the maximum width that this popup can have, not including outer
    /// borders.
    pub fn max_content_width(full_screen_width: u16) -> u16 {
        Self::max_width(full_screen_width).saturating_sub(2)
    }

    /// Returns the maximum height that this popup can have, including outer
    /// borders.
    ///
    /// This is currently 80% of screen height.
    pub fn max_height(full_screen_height: u16) -> u16 {
        (full_screen_height as u32 * 4 / 5) as u16
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

        let width = u16::min(self.width(), Self::max_width(full_screen.width));
        let height =
            u16::min(self.height(), Self::max_height(full_screen.height));

        let rect =
            full_screen.center_horizontally(width).center_vertically(height);

        // Clear the popup
        Clear.render(rect, buf);
        Block::default()
            .style(Style::default().bg(TUI_BLACK).fg(TUI_BLACK))
            .render(rect, buf);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    // Top titlebar
                    Constraint::Length(3),
                    Constraint::Min(0),
                    // Buttons at the bottom will be accounted for while
                    // rendering the body
                ]
                .as_ref(),
            )
            .split(rect);

        let header = Paragraph::new(self.header).block(block.clone());
        header.render(chunks[0], buf);

        block
            .borders(Borders::BOTTOM | Borders::LEFT | Borders::RIGHT)
            .render(chunks[1], buf);

        // NOTE: wrapping should be performed externally, by e.g. wrap_text.
        let body = Paragraph::new(self.body);

        let mut body_rect = chunks[1];
        // Ensure we're inside the outer border.
        body_rect.x += 1;
        body_rect.width = body_rect.width.saturating_sub(2);
        body_rect.height = body_rect.height.saturating_sub(1);

        if !self.buttons.is_empty() {
            // Reduce the height so that the body text doesn't overflow into the
            // button area.
            body_rect.height = body_rect.height.saturating_sub(BUTTON_HEIGHT);
        }

        body.render(body_rect, buf);

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
    let mut rect = body_rect;
    // Enough space at the bottom for buttons and margin
    rect.y = body_rect.y + body_rect.height - 4;
    rect.height = 3;

    let brackets = 2;
    let margin = 2;
    let borders = 2;

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
