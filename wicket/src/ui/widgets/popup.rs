// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A popup dialog box widget

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget},
};

use crate::ui::{
    defaults::dimensions::RectExt, panes::ComputedScrollOffset, wrap::wrap_text,
};
use crate::ui::{
    defaults::{colors::*, style},
    wrap::wrap_line,
};
use crate::ui::{
    panes::PendingScroll,
    widgets::{BoxConnector, BoxConnectorKind, Fade},
};
use std::{iter, marker::PhantomData};

const BUTTON_HEIGHT: u16 = 3;

#[derive(Clone, Debug)]
pub struct ButtonText<'a> {
    pub instruction: Line<'a>,
    pub key: Line<'a>,
}

impl<'a> ButtonText<'a> {
    pub fn new(instruction: &'a str, key: &'a str) -> Self {
        Self {
            instruction: Line::from(Span::styled(
                instruction,
                Self::default_instruction_style(),
            )),
            key: Line::from(Span::styled(key, Self::default_key_style())),
        }
    }

    pub fn default_instruction_style() -> Style {
        style::help_function()
    }

    pub fn default_key_style() -> Style {
        style::help_keys()
    }
}

#[derive(Default)]
pub struct PopupBuilder<'a> {
    pub header: Line<'a>,
    pub body: Text<'a>,
    pub buttons: Vec<ButtonText<'a>>,
}

impl<'a> PopupBuilder<'a> {
    /// Builds a non-scrollable popup.
    pub fn build(&self, full_screen: Rect) -> Popup<'_, NonScrollable> {
        Popup::new(
            full_screen,
            &self.header,
            &self.body,
            self.buttons.clone(),
            NonScrollable {},
        )
    }

    pub fn build_scrollable(
        &self,
        full_screen: Rect,
        scroll_offset: PopupScrollOffset,
    ) -> Popup<'_, Scrollable> {
        Popup::new(
            full_screen,
            &self.header,
            &self.body,
            self.buttons.clone(),
            Scrollable { scroll_offset },
        )
    }
}

/// Tracks state regarding scrollable and non-scrollable popups.
///
/// This is a trait to ensure a compile-time separation between scrollable and
/// non-scrollable popups -- otherwise it was too easy to make a mistake
/// handling scroll offsets.
pub trait PopupScrollability {
    /// Apply the state present in `self` to the data and body rectangle,
    /// returning the actual scroll offset if any.
    fn actualize(
        self,
        data: &mut PopupData<'_>,
        body_rect: &mut Rect,
    ) -> Option<u16>;
}

/// Type parameter for a non-scrollable popup.
pub struct NonScrollable {}

impl PopupScrollability for NonScrollable {
    fn actualize(
        self,
        _data: &mut PopupData<'_>,
        _body_rect: &mut Rect,
    ) -> Option<u16> {
        None
    }
}

/// Type parameter for a scrollable popup.
pub struct Scrollable {
    scroll_offset: PopupScrollOffset,
}

impl PopupScrollability for Scrollable {
    fn actualize(
        self,
        data: &mut PopupData<'_>,
        body_rect: &mut Rect,
    ) -> Option<u16> {
        enum ScrollKind {
            NotRequired,
            Scrolling(PopupScrollOffset),
        }

        let height_exceeded =
            data.wrapped_body.height() > body_rect.height as usize;
        let scroll_kind = match (data.buttons.is_empty(), height_exceeded) {
            (true, true) => {
                // Need to add scroll buttons, which necessitates reducing
                // the size.
                body_rect.height =
                    body_rect.height.saturating_sub(BUTTON_HEIGHT);
                ScrollKind::Scrolling(self.scroll_offset)
            }
            (false, true) => ScrollKind::Scrolling(self.scroll_offset),
            (_, false) => ScrollKind::NotRequired,
        };

        let offset = match scroll_kind {
            ScrollKind::NotRequired => 0,
            ScrollKind::Scrolling(offset) => {
                // Add scroll buttons.
                let offset = ComputedScrollOffset::new(
                    offset.offset as usize,
                    data.wrapped_body.height(),
                    body_rect.height as usize,
                    offset.pending_scroll,
                );

                let up_style = if offset.can_scroll_up() {
                    style::selected()
                } else {
                    ButtonText::default_key_style()
                };
                let down_style = if offset.can_scroll_down() {
                    style::selected()
                } else {
                    ButtonText::default_key_style()
                };

                data.buttons.insert(
                    0,
                    ButtonText {
                        instruction: Span::styled(
                            "Scroll",
                            ButtonText::default_instruction_style(),
                        )
                        .into(),
                        key: Line::from(vec![
                            Span::styled("Up", up_style),
                            Span::styled("/", ButtonText::default_key_style()),
                            Span::styled("Down", down_style),
                        ]),
                    },
                );

                offset.into_offset()
            }
        };
        Some(offset)
    }
}

/// Returns the maximum width that this popup can have, including outer
/// borders.
///
/// This is currently 80% of screen width.
pub fn popup_max_width(full_screen_width: u16) -> u16 {
    (u32::from(full_screen_width) * 4 / 5) as u16
}

/// Returns the maximum width that this popup can have, not including outer
/// borders.
pub fn popup_max_content_width(full_screen_width: u16) -> u16 {
    // -2 for borders, -2 for padding
    popup_max_width(full_screen_width).saturating_sub(4)
}

/// Returns the maximum height that this popup can have, including outer
/// borders.
///
/// This is currently 80% of screen height.
pub fn popup_max_height(full_screen_height: u16) -> u16 {
    (u32::from(full_screen_height) * 4 / 5) as u16
}

/// Returns the wrap options that should be used in most cases for popups.
fn default_wrap_options(
    full_screen_width: u16,
) -> crate::ui::wrap::Options<'static> {
    crate::ui::wrap::Options {
        width: popup_max_content_width(full_screen_width) as usize,
        // The indent here is to add 1 character of padding.
        initial_indent: Span::raw(" "),
        subsequent_indent: Span::raw(" "),
        break_words: true,
    }
}

#[derive(Default)]
pub struct Popup<'a, S: PopupScrollability> {
    data: PopupData<'a>,
    rect: Rect,
    chunks: Vec<Rect>,
    body_rect: Rect,
    actual_scroll_offset: Option<u16>,
    _marker: PhantomData<S>,
}

impl<'a, S: PopupScrollability> Popup<'a, S> {
    fn new(
        full_screen: Rect,
        header: &'a Line<'_>,
        body: &'a Text<'_>,
        buttons: Vec<ButtonText<'a>>,
        scrollability: S,
    ) -> Self {
        let wrapped_header =
            wrap_line(header, default_wrap_options(full_screen.width));
        let wrapped_body =
            wrap_text(body, default_wrap_options(full_screen.width));

        let mut data = PopupData { wrapped_header, wrapped_body, buttons };

        // Compute the dimensions here so we can compute scroll positions more
        // effectively.
        let width = u16::min(data.width(), popup_max_width(full_screen.width));
        let height =
            u16::min(data.height(), popup_max_height(full_screen.height));

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

        let actual_scroll = scrollability.actualize(&mut data, &mut body_rect);

        Self {
            data,
            rect,
            chunks: chunks.to_vec(),
            body_rect,
            actual_scroll_offset: actual_scroll,
            _marker: PhantomData,
        }
    }
}

impl<'a> Popup<'a, Scrollable> {
    /// Returns the effective, or actual, scroll offset after the text is laid
    /// out, in the form of a `PopupScrollOffset`.
    ///
    /// The offset is is capped to the maximum degree to which text can be
    /// scrolled.
    pub fn actual_scroll_offset(&self) -> PopupScrollOffset {
        PopupScrollOffset::new(
            self.actual_scroll_offset
                .expect("scrollable popups always have actual_scroll_offset"),
        )
    }
}

impl<S: PopupScrollability> Widget for Popup<'_, S> {
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
        if let Some(offset) = self.actual_scroll_offset {
            body = body.scroll((offset, 0));
        }

        body.render(self.body_rect, buf);

        let connector = BoxConnector::new(BoxConnectorKind::Top);

        connector.render(self.chunks[1], buf);

        draw_buttons(self.data.buttons, self.chunks[1], buf);
    }
}

/// Scroll offset for a popup.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
#[must_use]
pub struct PopupScrollOffset {
    /// The offset.
    offset: u16,

    /// A pending scroll event.
    pending_scroll: Option<PendingScroll>,
}

impl PopupScrollOffset {
    /// Returns a new `PopupScrollOffset` at the specified offset.
    pub fn new(offset: u16) -> Self {
        Self { offset, pending_scroll: None }
    }

    /// Sets a pending scroll event.
    pub fn set_pending_scroll(&mut self, pending_scroll: PendingScroll) {
        self.pending_scroll = Some(pending_scroll);
    }
}

#[derive(Default)]
pub struct PopupData<'a> {
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
        .constraints(constraints)
        .split(rect);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .style(style::selected_line());

    for (i, button) in buttons.into_iter().enumerate() {
        let mut spans = vec![Span::raw(" ")];
        spans.extend(button.instruction.spans);
        spans.push(Span::styled(" <", ButtonText::default_key_style()));
        spans.extend(button.key.spans);
        spans.push(Span::styled(">", ButtonText::default_key_style()));

        let b = Paragraph::new(Line::from(spans)).block(block.clone());
        b.render(button_rects[i + 1], buf);
    }
}
