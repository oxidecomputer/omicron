// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod overview;
mod rack_setup;
mod update;

pub use super::Control;
use crate::ui::defaults::style;
use crate::Cmd;
pub use overview::OverviewPane;
pub use rack_setup::RackSetupPane;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Paragraph;
pub use update::UpdatePane;

/// Generate one line of text for the help bar in panes
pub fn help_text<'a>(data: &'a [(&'a str, &'a str)]) -> Paragraph<'a> {
    let mut text = vec![Span::raw(" ")];
    for (function, keys) in data {
        text.push(Span::styled(*function, style::help_function()));
        text.push(Span::raw(" "));
        text.push(Span::styled(*keys, style::help_keys()));
        text.push(Span::styled(" | ", style::divider()));
    }
    text.pop();
    Paragraph::new(Line::from(text))
}

/// Split up a text into lines and push them into a `Line` one at a time.
///
/// This makes text wrapping offsets work correctly.
pub fn push_text_lines<'a>(
    message: &str,
    prefix: Vec<Span<'a>>,
    spans: &mut Vec<Line<'a>>,
) {
    // If the message has multiple lines of text, split them
    // into separate spans. This makes text wrapping offsets
    // work correctly.
    let mut next_line = prefix;
    for line in message.lines() {
        next_line.push(Span::styled(line.to_owned(), style::plain_text()));
        spans.push(Line::from(next_line));
        next_line = Vec::new();
    }
}

/// Align a bunch of spans on a single line with with at most `column_width`
/// length, and being left-aligned by `left_margin`
pub fn align_by(
    left_margin: u16,
    column_width: u16,
    mut rect: Rect,
    spans: Vec<Span>,
) -> Text {
    rect.x += left_margin;
    rect.width -= left_margin;
    let constraints: Vec<_> =
        (0..spans.len()).map(|_| Constraint::Max(column_width)).collect();
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(rect);

    let mut text =
        vec![Span::raw(format!("{:width$}", "", width = left_margin as usize))];
    for (rect, span) in chunks.iter().zip(spans) {
        let spaces = rect.width.saturating_sub(span.width().try_into().unwrap())
            as usize;
        text.push(span);
        text.push(Span::raw(format!("{:spaces$}", "")));
    }
    Text::from(Line::from(text))
}

/// A pending scroll command.
///
/// This is used to communicate between the `on` and `draw` functions.
///
/// **NOTE:** `PendingScroll` deliberately does not implement `Copy` so that
/// users are forced to reset pending scrolls with `Option::take`, or at least
/// that there's some friction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingScroll {
    Up,
    Down,
    PageUp,
    PageDown,
    GotoTop,
    GotoBottom,
}

impl PendingScroll {
    /// Maps a [`Cmd`] to a `PendingScroll`, if possible.
    ///
    /// Use in [`Control::on`] to handle scroll events.
    pub fn from_cmd(cmd: &Cmd) -> Option<Self> {
        match cmd {
            Cmd::Up => Some(Self::Up),
            Cmd::Down => Some(Self::Down),
            Cmd::PageUp => Some(Self::PageUp),
            Cmd::PageDown => Some(Self::PageDown),
            Cmd::GotoTop => Some(Self::GotoTop),
            Cmd::GotoBottom => Some(Self::GotoBottom),
            _ => None,
        }
    }
}

/// A computed scroll offset.
///
/// This scroll offset is computed by [`Self::new`], and is capped so that we
/// don't allow scrolling past the bottom of content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComputedScrollOffset {
    Top,
    Middle(u16),
    Bottom(u16),
}

impl ComputedScrollOffset {
    /// Compute the scroll offset for a `Paragraph` widget
    ///
    /// This takes the text size and number of visible lines of the terminal
    /// `Rect` account such that it will render the whole paragraph and not
    /// scroll if there is enough room. Otherwise it will scroll as expected
    /// with the top of the terminal `Rect` set to current_offset. We don't
    /// allow scrolling beyond the bottom of the text content.
    ///
    /// XXX: This whole paragraph scroll only allows length of less than 64k
    /// rows even though it shouldn't be limited. We can do our own scrolling
    /// instead to obviate this limit. we may want to anyway, as it's less data
    /// to be formatted for the Paragraph.
    ///
    /// XXX: This algorithm doesn't work properly with line wraps, so we should
    /// ensure that the data we populate doesn't wrap or we have truncation
    /// turned on.
    pub fn new(
        current_offset: usize,
        text_height: usize,
        num_lines: usize,
        pending_scroll: Option<PendingScroll>,
    ) -> Self {
        let mut offset = if let Some(pending_scroll) = pending_scroll {
            // For page up and down, scroll by num_lines - 1 so at least one line is shared.
            let page_lines = num_lines.saturating_sub(1);
            match pending_scroll {
                PendingScroll::Up => current_offset.saturating_sub(1),
                PendingScroll::Down => current_offset + 1,
                PendingScroll::PageUp => {
                    current_offset.saturating_sub(page_lines)
                }
                PendingScroll::PageDown => current_offset + page_lines,
                PendingScroll::GotoTop => 0,
                // text.height() will get capped below.
                PendingScroll::GotoBottom => text_height,
            }
        } else {
            current_offset
        };

        if offset > text_height {
            offset = text_height;
        }

        if text_height <= num_lines {
            offset = 0;
        } else {
            if text_height - offset < num_lines {
                // Don't allow scrolling past bottom of content
                //
                // Reset the scroll offset, so that an up arrow will scroll up
                // on the next try.
                offset = text_height - num_lines;
            }
        }
        // This doesn't allow data more than 64k rows. We shouldn't need more
        // than that for wicket, but who knows!
        if offset == 0 {
            Self::Top
        } else if offset < text_height - num_lines {
            Self::Middle(u16::try_from(offset).unwrap())
        } else if offset == text_height - num_lines {
            Self::Bottom(u16::try_from(offset).unwrap())
        } else {
            panic!(
                "offset ({offset}) not capped to \
                 text_height ({text_height}) - num_lines ({num_lines})"
            )
        }
    }

    /// Returns true if scrolling up is possible.
    pub fn can_scroll_up(&self) -> bool {
        match self {
            Self::Top => false,
            Self::Middle(_) | Self::Bottom(_) => true,
        }
    }

    /// Returns true if scrolling down is possible.
    pub fn can_scroll_down(&self) -> bool {
        match self {
            Self::Top | Self::Middle(_) => true,
            Self::Bottom(_) => false,
        }
    }

    /// Returns the numerical offset.
    pub fn into_offset(self) -> u16 {
        match self {
            Self::Top => 0,
            Self::Middle(offset) | Self::Bottom(offset) => offset,
        }
    }
}
