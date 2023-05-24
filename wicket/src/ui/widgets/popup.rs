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

use crate::ui::widgets::{BoxConnector, BoxConnectorKind, Fade};
use crate::ui::{
    defaults::dimensions::RectExt, panes::ComputedScrollOffset, wrap::wrap_text,
};
use crate::ui::{
    defaults::{colors::*, style},
    wrap::wrap_line,
};
use std::iter;

const BUTTON_HEIGHT: u16 = 3;

#[derive(Clone, Debug)]
pub struct ButtonText<'a> {
    pub instruction: Spans<'a>,
    pub key: Spans<'a>,
}

impl<'a> ButtonText<'a> {
    pub fn new(instruction: &'a str, key: &'a str) -> Self {
        Self {
            instruction: Spans::from(Span::styled(
                instruction,
                style::selected(),
            )),
            key: Spans::from(Span::styled(key, style::selected_line())),
        }
    }
}

#[derive(Default)]
pub struct PopupBuilder<'a> {
    pub header: Spans<'a>,
    pub body: Text<'a>,
    pub buttons: Vec<ButtonText<'a>>,
}

impl<'a> PopupBuilder<'a> {
    pub fn build(
        &self,
        full_screen: Rect,
        scroll_kind: PopupScrollKind,
    ) -> Popup<'_> {
        Popup::new(
            full_screen,
            &self.header,
            &self.body,
            self.buttons.clone(),
            scroll_kind,
        )
    }
}

#[derive(Default)]
pub struct Popup<'a> {
    data: PopupData<'a>,
    rect: Rect,
    chunks: Vec<Rect>,
    body_rect: Rect,
    actual_kind: PopupScrollKind,
}

impl<'a> Popup<'a> {
    fn new(
        full_screen: Rect,
        header: &'a Spans<'_>,
        body: &'a Text<'_>,
        buttons: Vec<ButtonText<'a>>,
        scroll_kind: PopupScrollKind,
    ) -> Self {
        let wrapped_header =
            wrap_line(header, Self::default_wrap_options(full_screen.width));
        let wrapped_body =
            wrap_text(body, Self::default_wrap_options(full_screen.width));

        let mut data = PopupData { wrapped_header, wrapped_body, buttons };

        // Compute the dimensions here so we can compute scroll positions more
        // effectively.
        let width = u16::min(data.width(), Self::max_width(full_screen.width));
        let height =
            u16::min(data.height(), Self::max_height(full_screen.height));

        let rect =
            full_screen.center_horizontally(width).center_vertically(height);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    // Top titlebar
                    Constraint::Length(data.wrapped_header.height() as u16 + 2),
                    Constraint::Min(0),
                    // Buttons at the bottom will be accounted for while
                    // rendering the body
                ]
                .as_ref(),
            )
            .split(rect);

        let mut body_rect = chunks[1];
        // Ensure we're inside the outer border.
        body_rect.x += 1;
        body_rect.width = body_rect.width.saturating_sub(2);
        body_rect.height = body_rect.height.saturating_sub(1);

        if !data.buttons.is_empty() {
            body_rect.height = body_rect.height.saturating_sub(BUTTON_HEIGHT);
        }

        enum ScrollKind {
            Disabled,
            NotRequired,
            Scrolling(u16),
        }

        let scroll_kind = match scroll_kind {
            PopupScrollKind::Disabled => ScrollKind::Disabled,
            PopupScrollKind::Enabled { offset } => {
                let height_exceeded =
                    data.wrapped_body.height() > body_rect.height as usize;
                match (data.buttons.is_empty(), height_exceeded) {
                    (true, true) => {
                        // Need to add scroll buttons, which necessitates reducing
                        // the size.
                        body_rect.height =
                            body_rect.height.saturating_sub(BUTTON_HEIGHT);
                        ScrollKind::Scrolling(offset)
                    }
                    (false, true) => ScrollKind::Scrolling(offset),
                    (_, false) => ScrollKind::NotRequired,
                }
            }
        };

        let actual_offset = match scroll_kind {
            ScrollKind::Disabled => PopupScrollKind::Disabled,
            ScrollKind::NotRequired => PopupScrollKind::Enabled { offset: 0 },
            ScrollKind::Scrolling(offset) => {
                // Add scroll buttons.
                let offset = ComputedScrollOffset::new(
                    offset as usize,
                    data.wrapped_body.height(),
                    body_rect.height as usize,
                );

                let up_style = if offset.can_scroll_up() {
                    style::selected()
                } else {
                    style::selected_line()
                };
                let down_style = if offset.can_scroll_down() {
                    style::selected()
                } else {
                    style::selected_line()
                };

                data.buttons.insert(
                    0,
                    ButtonText {
                        instruction: Span::raw("SCROLL").into(),
                        key: Spans::from(vec![
                            Span::styled("UP", up_style),
                            Span::raw("/"),
                            Span::styled("DOWN", down_style),
                        ]),
                    },
                );

                PopupScrollKind::Enabled { offset: offset.into_offset() }
            }
        };

        Self { data, rect, chunks, body_rect, actual_kind: actual_offset }
    }

    /// Returns the effective, or actual, scroll kind after the text is laid
    /// out.
    ///
    /// If this is a `PopupScrollKind::Enabled` popup, the offset is is capped
    /// to the maximum degree to which text can be scrolled.
    pub fn actual_kind(&self) -> PopupScrollKind {
        self.actual_kind
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
        // -2 for borders, -2 for padding
        Self::max_width(full_screen_width).saturating_sub(4)
    }

    /// Returns the maximum height that this popup can have, including outer
    /// borders.
    ///
    /// This is currently 80% of screen height.
    pub fn max_height(full_screen_height: u16) -> u16 {
        (full_screen_height as u32 * 4 / 5) as u16
    }

    /// Returns the wrap options that should be used in most cases for popups.
    fn default_wrap_options(
        full_screen_width: u16,
    ) -> crate::ui::wrap::Options<'static> {
        crate::ui::wrap::Options {
            width: Popup::max_content_width(full_screen_width) as usize,
            // The indent here is to add 1 character of padding.
            initial_indent: Span::raw(" "),
            subsequent_indent: Span::raw(" "),
            break_words: true,
        }
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

        // Clear the popup
        Clear.render(self.rect, buf);
        Block::default()
            .style(Style::default().bg(TUI_BLACK).fg(TUI_BLACK))
            .render(self.rect, buf);

        let header =
            Paragraph::new(self.data.wrapped_header).block(block.clone());
        header.render(self.chunks[0], buf);

        block
            .borders(Borders::BOTTOM | Borders::LEFT | Borders::RIGHT)
            .render(self.chunks[1], buf);

        let mut body = Paragraph::new(self.data.wrapped_body);
        match self.actual_kind {
            PopupScrollKind::Disabled => {}
            PopupScrollKind::Enabled { offset } => {
                body = body.scroll((offset, 0));
            }
        }

        body.render(self.body_rect, buf);

        let connector = BoxConnector::new(BoxConnectorKind::Top);

        connector.render(self.chunks[1], buf);

        draw_buttons(self.data.buttons, self.chunks[1], buf);
    }
}

/// Scroll kind for a popup.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PopupScrollKind {
    /// Scrolling is disabled.
    #[default]
    Disabled,

    /// Scrolling is enabled.
    Enabled {
        /// The offset.
        offset: u16,
    },
}

impl PopupScrollKind {
    /// Returns a new enabled scroll kind.
    pub fn enabled() -> Self {
        Self::Enabled { offset: 0 }
    }

    /// Returns true if scrolling is enabled.
    pub fn is_scrollable(&self) -> bool {
        match self {
            Self::Disabled => false,
            Self::Enabled { .. } => true,
        }
    }

    /// Scrolls up.
    pub fn scroll_up(&mut self) {
        match self {
            Self::Disabled => {}
            Self::Enabled { offset } => {
                *offset = offset.saturating_sub(1);
            }
        }
    }

    /// Scrolls down.
    pub fn scroll_down(&mut self) {
        match self {
            PopupScrollKind::Disabled => {}
            PopupScrollKind::Enabled { offset } => {
                *offset = offset.saturating_add(1);
            }
        }
    }
}

#[derive(Default)]
struct PopupData<'a> {
    // Fields are private because we always want users to go through the
    // constructor.

    // This is the header as passed in, except wrapped.
    wrapped_header: Text<'a>,

    // We store the *wrapped body* rather than the unwrapped body to make
    // `self.height()` and `self.width()` be computed correctly.
    wrapped_body: Text<'a>,

    buttons: Vec<ButtonText<'a>>,
}

impl<'a> PopupData<'a> {
    pub fn height(&self) -> u16 {
        let button_height: u16 =
            if self.buttons.is_empty() { 0 } else { BUTTON_HEIGHT };
        let bottom_margin: u16 = 1;
        let borders: u16 = 3;
        u16::try_from(self.wrapped_header.height()).unwrap()
            + u16::try_from(self.wrapped_body.height()).unwrap()
            + button_height
            + bottom_margin
            + borders
    }

    pub fn width(&self) -> u16 {
        let borders: u16 = 2;
        // Left padding is taken care of by prepending spaces to the body. Right
        // padding is added here.
        let right_padding: u16 = 1;
        let body_width = u16::try_from(self.wrapped_body.width()).unwrap()
            + borders
            + right_padding;
        let header_width = u16::try_from(self.wrapped_header.width()).unwrap()
            + borders
            + right_padding;
        let width = u16::max(body_width, header_width);
        u16::max(width, self.button_width())
    }

    pub fn button_width(&self) -> u16 {
        let space_between_buttons = 1;
        let margins = 4;
        // Margin + space + angle brackets
        let button_extras = 6;
        let width = self.buttons.iter().fold(margins, |acc, text| {
            acc + text.instruction.width()
                + text.key.width()
                + button_extras
                + space_between_buttons
        });
        u16::try_from(width).unwrap()
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
                    b.instruction.width()
                        + b.key.width()
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
        let mut spans = vec![Span::raw(" ")];
        spans.extend(button.instruction.0);
        spans.push(Span::styled(" <", style::selected_line()));
        spans.extend(button.key.0);
        spans.push(Span::styled(">", style::selected_line()));

        let b = Paragraph::new(Spans(spans)).block(block.clone());
        b.render(button_rects[i + 1], buf);
    }
}
