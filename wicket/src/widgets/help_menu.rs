// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The help menu for the system

use super::animate_clear_buf;
use super::AnimationState;
use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::style::{Modifier, Style};
use tui::text::Text;
use tui::widgets::Paragraph;
use tui::widgets::Widget;
use tui::widgets::{Block, Borders};

#[derive(Debug)]
pub struct HelpMenu {
    pub style: Style,
    pub command_style: Style,
    pub state: AnimationState,
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

        for (cmd, desc) in help {
            text.extend(Text::styled(cmd, self.command_style));
            text.extend(Text::styled(format!("    {desc}"), self.style));
            text.extend(Text::styled("\n", self.style));
        }

        let text_width: u16 = text.width().try_into().unwrap();
        let text_height: u16 = text.height().try_into().unwrap();

        let xshift = 5;
        let yshift = 1;

        // Draw the background and some borders
        let mut bg = rect.clone();
        bg.width = text_width + xshift * 2;
        bg.height = text_height + yshift * 3 / 2 + 2;
        let drawn_bg = animate_clear_buf(bg, buf, self.style, self.state);
        let bg_block =
            Block::default().border_style(self.style).borders(Borders::ALL);
        bg_block.render(drawn_bg, buf);

        // Draw the menu text if bg animation is done opening
        //
        // Note that render may be called during closing also, and so this will
        // not get called in that case
        if self.state.is_opening() && self.state.is_done() {
            let menu = Paragraph::new(text);
            rect.x = xshift;
            rect.y = yshift;
            rect.width = text_width;
            rect.height = text_height;
            menu.render(rect, buf);
        }
    }
}
