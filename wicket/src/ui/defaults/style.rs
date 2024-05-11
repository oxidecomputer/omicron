// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Default consistent styling for various widgets

use super::colors::*;
use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;

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

pub fn plain_text_bold() -> Style {
    plain_text().add_modifier(Modifier::BOLD)
}

pub fn successful_update() -> Style {
    selected()
}

pub fn successful_update_bold() -> Style {
    successful_update().add_modifier(Modifier::BOLD)
}

pub fn failed_update() -> Style {
    Style::default().fg(OX_RED)
}

pub fn failed_update_bold() -> Style {
    failed_update().add_modifier(Modifier::BOLD)
}

pub fn start_update() -> Style {
    Style::default().fg(OX_YELLOW)
}

pub fn warning_update() -> Style {
    Style::default().fg(OX_YELLOW)
}

pub fn warning_update_bold() -> Style {
    warning_update().add_modifier(Modifier::BOLD)
}

pub fn line(active: bool) -> Style {
    if active {
        selected_line()
    } else {
        deselected()
    }
}

pub fn warning() -> Style {
    Style::default().fg(OX_YELLOW)
}

pub fn header(active: bool) -> Style {
    if active {
        selected()
    } else {
        deselected()
    }
}

pub fn popup_highlight() -> Style {
    Style::default().fg(TUI_PURPLE)
}

pub fn bold() -> Style {
    Style::default().add_modifier(Modifier::BOLD)
}

pub fn faded_background() -> Style {
    Style::default().bg(TUI_BLACK).fg(TUI_GREY)
}

pub fn text_label() -> Style {
    Style::default().fg(OX_OFF_WHITE)
}

pub fn text_success() -> Style {
    Style::default().fg(OX_GREEN_LIGHT)
}

pub fn text_failure() -> Style {
    Style::default().fg(OX_RED)
}

pub fn text_warning() -> Style {
    Style::default().fg(OX_YELLOW)
}

pub const CHECK_ICON: char = '✓';
pub const CROSS_ICON: char = '✗';
pub const WARN_ICON: char = '⚠';
pub const BULLET_ICON: char = '•';
