// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod overview;
mod update;

pub use super::Control;
use crate::ui::defaults::style;
pub use overview::OverviewPane;
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::text::{Span, Spans, Text};
use tui::widgets::Paragraph;
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
    Paragraph::new(Spans::from(text))
}

/// Align a bunch of spans on a single line with `spaces` spaces between them,
/// with each having `max_width` length.
pub fn align_by<'a>(
    left_margin: u16,
    column_width: u16,
    mut rect: Rect,
    spans: Vec<Span<'a>>,
) -> Text<'a> {
    rect.x += left_margin;
    rect.width -= left_margin;
    let constraints: Vec<_> = (0..spans.len())
        .into_iter()
        .map(|_| Constraint::Max(column_width))
        .collect();
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(rect);

    let mut text =
        vec![Span::raw(format!("{:width$}", "", width = left_margin as usize))];
    for (rect, span) in chunks.iter().zip(spans).into_iter() {
        let spaces = rect.width.saturating_sub(span.width().try_into().unwrap())
            as usize;
        text.push(span);
        text.push(Span::raw(format!("{:spaces$}", "")));
    }
    Text::from(Spans::from(text))
}
