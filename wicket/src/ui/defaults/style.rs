// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default consistent styling for various widgets
//! Screens can overried these where appropriate

use super::colors::*;
use tui::style::Color;
use tui::style::Style;

pub fn button() -> Style {
    Style::default().fg(OX_OFF_WHITE).bg(OX_GREEN_DARK)
}

pub fn button_hovered() -> Style {
    Style::default().fg(OX_PINK).bg(OX_GREEN_DARK)
}

pub fn help_menu() -> Style {
    Style::default().fg(OX_OFF_WHITE).bg(OX_GREEN_DARK)
}

pub fn help_menu_command() -> Style {
    Style::default().fg(OX_GREEN_LIGHT).bg(OX_GREEN_DARK)
}

pub fn menu_bar() -> Style {
    Style::default().bg(OX_GREEN_DARK).fg(OX_GRAY)
}

pub fn menu_bar_selected() -> Style {
    Style::default().fg(OX_GREEN_LIGHT)
}

pub fn screen_background() -> Style {
    Style::default().fg(OX_GREEN_DARK).bg(Color::Black)
}

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
