// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The splash [`Screen']
//!
//! This is the first screen the user sees

use super::colors::*;
use super::{Screen, ScreenId};
use crate::widgets::{Logo, LogoState, LOGO_HEIGHT, LOGO_WIDTH};
use crate::Action;
use crate::Frame;
use crate::ScreenEvent;
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
        let style = Style::default().bg(Color::Black);
        let block = Block::default().style(style);
        f.render_widget(block, f.size());
    }

    // Sweep left to right, painting the banner white, with
    // the x painted green.
    fn animate_logo(&mut self, f: &mut Frame) {
        // Center the banner
        let mut rect = f.size();
        rect.x = rect.width / 2 - LOGO_WIDTH / 2;
        rect.y = rect.height / 2 - LOGO_HEIGHT / 2;
        rect.height = LOGO_HEIGHT;
        rect.width = LOGO_WIDTH;

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

impl Screen for SplashScreen {
    fn draw(
        &mut self,
        state: &crate::State,
        terminal: &mut crate::Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            self.draw_background(f);
            self.animate_logo(f);
        })?;
        Ok(())
    }

    fn on(&mut self, state: &crate::State, event: ScreenEvent) -> Vec<Action> {
        self.state.frame += 1;
        if self.state.frame < TOTAL_FRAMES {
            vec![Action::Redraw]
        } else {
            vec![Action::SwitchScreen(ScreenId::Inventory)]
        }
    }
}
