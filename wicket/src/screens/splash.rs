// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The splash [`Screen']
//!
//! This is the first screen the user sees

use crate::defaults::colors::*;
use crate::defaults::dimensions::RectExt;
use crate::defaults::style;
use crate::widgets::{Logo, LogoState, LOGO_HEIGHT, LOGO_WIDTH};
use crate::{Event, Frame, Term};
use crossterm::event::Event as TermEvent;
use tui::style::{Color, Style};
use tui::widgets::Block;

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

impl SplashScreen {
    pub fn draw(&self, terminal: &mut Term) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.draw_background(f);
            self.animate_logo(f);
        })?;
        Ok(())
    }

    /// Return true if the splash screen should transition to the main screen, false
    /// if it should keep animating.
    pub fn on(&mut self, event: Event) -> bool {
        match event {
            Event::Tick => {
                self.state.frame += 1;
                if self.state.frame < TOTAL_FRAMES {
                    false
                } else {
                    true
                }
            }
            Event::Term(TermEvent::Key(_)) => {
                // Allow the user to skip the splash screen with any key press
                true
            }
            _ => false,
        }
    }
}
