// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod overview;
mod update;

pub use super::Control;
use crate::ui::defaults::style;
pub use overview::OverviewPane;
use tui::text::{Span, Spans};
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
