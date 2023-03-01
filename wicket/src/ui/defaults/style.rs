// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default consistent styling for various widgets

use super::colors::*;
use tui::style::Color;
use tui::style::Style;

pub fn selected() -> Style {
    Style::default().fg(TUI_GREEN)
}

pub fn selected_line() -> Style {
    Style::default().fg(TUI_GREEN_DARK)
}

pub fn deselected() -> Style {
    Style::default().fg(TUI_GREY)
}

pub fn background() -> Style {
    Style::default().bg(TUI_BLACK)
}

pub fn help_function() -> Style {
    selected()
}

pub fn help_keys() -> Style {
    selected_line()
}

pub fn divider() -> Style {
    deselected()
}

pub fn connected() -> Style {
    selected()
}

pub fn service() -> Style {
    selected_line()
}

pub fn delayed() -> Style {
    Style::default().fg(OX_OFF_WHITE)
}

pub fn highlighted() -> Style {
    Style::default().bg(TUI_PURPLE).fg(TUI_BLACK)
}

pub fn plain_text() -> Style {
    Style::default().bg(TUI_BLACK).fg(OX_OFF_WHITE)
}
