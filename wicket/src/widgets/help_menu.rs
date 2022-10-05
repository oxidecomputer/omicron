// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The help menu for the system

use super::clear_buf;
use crate::screens::TabIndex;
use tui::buffer::Buffer;
use tui::layout::Alignment;
use tui::layout::Rect;
use tui::style::{Modifier, Style};
use tui::text::Text;
use tui::text::{Span, Spans};
use tui::widgets::Block;
use tui::widgets::Paragraph;
use tui::widgets::Widget;

#[derive(Debug)]
pub struct HelpMenu {
    pub style: Style,
    pub command_style: Style,
}

impl Widget for HelpMenu {
    fn render(self, mut rect: Rect, buf: &mut Buffer) {
        let help = [
            ("<TAB> | mouse over", "Highlight object"),
            ("<Enter> | left mouse click", "Select highlighted objects"),
            ("<ESC>", "Exit the current context"),
            ("<CTRL-C>", "Exit the program"),
        ];

        let mut text = Text::styled(
            "HELP\n\n",
            self.style
                .add_modifier(Modifier::BOLD)
                .add_modifier(Modifier::UNDERLINED),
        );

        text.extend(Text {
            lines: help
                .iter()
                .map(|(cmd, desc)| {
                    Spans::from(vec![
                        Span::styled(*cmd, self.command_style),
                        Span::raw("  "),
                        Span::styled(*desc, self.style),
                    ])
                })
                .collect(),
        });

        let text_width: u16 = text.width().try_into().unwrap();
        let text_height: u16 = text.height().try_into().unwrap();

        let xshift = 10;
        let yshift = 1;

        // Draw the background
        let mut bg = rect.clone();
        bg.width = text_width + xshift * 3 / 2;
        bg.height = text_height + yshift * 3 / 2 + 2;
        clear_buf(bg, buf, self.style);

        // Draw the menu text
        let menu = Paragraph::new(text);
        rect.x = xshift;
        rect.y = yshift;
        rect.width = text_width;
        rect.height = text_height;
        menu.render(rect, buf);
    }
}
