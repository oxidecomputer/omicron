// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This is the first screen the user sees

use super::defaults::colors::*;
use super::defaults::dimensions::RectExt;
use super::defaults::style;
use super::widgets::{Logo, LogoState, LOGO_HEIGHT, LOGO_WIDTH};
use crate::{Action, Cmd, Control, Frame};
use ratatui::style::Style;
use ratatui::widgets::Block;

const TOTAL_FRAMES: usize = 100;

pub struct SplashScreen {
    state: LogoState,
}

impl SplashScreen {
    pub fn new() -> SplashScreen {
        SplashScreen {
            state: LogoState {
                frame: 0,
                text: include_str!("../../banners/oxide.txt"),
            },
        }
    }

    fn draw_background(&self, f: &mut Frame) {
        let block = Block::default().style(style::background());
        f.render_widget(block, f.size());
    }

    // Sweep left to right, painting the banner white, with
    // the x painted green.
    fn animate_logo(&self, f: &mut Frame) {
        // Center the banner
        let rect = f
            .size()
            .center_horizontally(LOGO_WIDTH)
            .center_vertically(LOGO_HEIGHT);

        let stale_style = Style::default().fg(OX_GREEN_DARKEST);
        let style = Style::default().fg(OX_OFF_WHITE);
        let x_style = Style::default().fg(OX_GREEN_LIGHT);
        let logo = Logo::new(&self.state)
            .stale_style(stale_style)
            .style(style)
            .x_style(x_style);

        f.render_widget(logo, rect);
    }
}

impl Control for SplashScreen {
    fn draw(
        &mut self,
        _: &crate::State,
        frame: &mut Frame<'_>,
        _: ratatui::prelude::Rect,
        _: bool,
    ) {
        self.draw_background(frame);
        self.animate_logo(frame);
    }

    fn on(&mut self, _: &mut crate::State, cmd: Cmd) -> Option<crate::Action> {
        match cmd {
            Cmd::Tick => {
                self.state.frame += 1;
                if self.state.frame >= TOTAL_FRAMES {
                    Some(Action::SwitchScreen)
                } else {
                    Some(Action::Redraw)
                }
            }
            // Allow the user to skip the splash screen with any key press
            _ => Some(Action::SwitchScreen),
        }
    }
}
