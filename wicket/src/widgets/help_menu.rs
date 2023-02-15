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

pub type HelpText = Vec<(&'static str, &'static str)>;

pub struct HelpMenuState {
    animation_state: Option<AnimationState>,
    help: HelpText,
}

impl HelpMenuState {
    pub fn new(help: HelpText) -> HelpMenuState {
        HelpMenuState { animation_state: None, help }
    }

    pub fn help_text(&self) -> &HelpText {
        &self.help
    }

    pub fn get_animation_state(&self) -> Option<AnimationState> {
        self.animation_state
    }

    // Return true if there is no [`AnimationState`]
    pub fn is_closed(&self) -> bool {
        self.animation_state.is_none()
    }

    // Toggle the display of the help menu
    pub fn toggle(&mut self) {
        if self.is_closed() {
            self.open()
        } else {
            self.close();
        }
    }

    // Perform an animation step
    pub fn step(&mut self) {
        let state = self.animation_state.as_mut().unwrap();
        let done = state.step();
        let is_closed = state.is_closing() && done;
        if is_closed {
            self.animation_state = None;
        }
    }

    pub fn open(&mut self) {
        self.animation_state
            .get_or_insert(AnimationState::Opening { frame: 0, frame_max: 8 });
    }

    pub fn close(&mut self) {
        let state = self.animation_state.take();
        match state {
            None => (), // Already closed
            Some(AnimationState::Opening { frame, frame_max }) => {
                // Transition to closing at the same position in the animation
                self.animation_state =
                    Some(AnimationState::Closing { frame, frame_max });
            }
            Some(s) => {
                // Already closing. Maintain same state
                self.animation_state = Some(s);
            }
        }
    }
}

#[derive(Debug)]
pub struct HelpMenu<'a> {
    pub help: &'a [(&'a str, &'a str)],
    pub style: Style,
    pub command_style: Style,
    pub state: AnimationState,
}

impl<'a> Widget for HelpMenu<'a> {
    fn render(self, mut rect: Rect, buf: &mut Buffer) {
        let mut text = Text::styled(
            "HELP\n\n",
            self.style
                .add_modifier(Modifier::BOLD)
                .add_modifier(Modifier::UNDERLINED),
        );

        for (cmd, desc) in self.help {
            text.extend(Text::styled(*cmd, self.command_style));
            text.extend(Text::styled(format!("    {desc}"), self.style));
            text.extend(Text::styled("\n", self.style));
        }

        let text_width: u16 = text.width().try_into().unwrap();
        let text_height: u16 = text.height().try_into().unwrap();

        let xshift = 5;
        let yshift = 1;

        // Draw the background and some borders
        let mut bg = rect;
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
