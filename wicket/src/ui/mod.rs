// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod controls;
pub mod defaults;
mod game;
mod main;
mod panes;
mod splash;
mod widgets;
mod wrap;

use crate::{Action, Cmd, State, Term};
use ratatui::prelude::Rect;
use ratatui::widgets::ListState;
use slog::{o, Logger};

use game::GameScreen;
use main::MainScreen;
use splash::SplashScreen;

pub use controls::Control;
pub use panes::OverviewPane;
pub use panes::RackSetupPane;
pub use panes::UpdatePane;

/// The primary display representation. It's sole purpose is to dispatch
/// [`Cmd`]s to the underlying splash and main screens.
///
/// We move controls out of their owning field's option and put them
/// in current as necessary. We start out in `splash` and never
/// transition back to it.
pub struct Screen {
    #[allow(unused)]
    log: slog::Logger,
    main: Option<Box<dyn Control>>,
    game: Option<Box<dyn Control>>,
    current: Box<dyn Control>,
}

impl Screen {
    pub fn new(log: &Logger) -> Screen {
        let log = log.new(o!("component" => "Screen"));
        let main = Some(Box::new(MainScreen::new(&log)) as Box<dyn Control>);
        let game = Some(Box::new(GameScreen::new()) as Box<dyn Control>);
        let current = Box::new(SplashScreen::new()) as Box<dyn Control>;
        Screen { log, main, game, current }
    }

    /// Compute the layout of the current screen
    ///
    // A draw is issued after every resize, so no need to return an Action
    pub fn resize(&mut self, state: &mut State, width: u16, height: u16) {
        state.screen_width = width;
        state.screen_height = height;
        self._resize(state);
    }

    pub fn on(&mut self, state: &mut State, cmd: Cmd) -> Option<Action> {
        self.current.on(state, cmd)
    }

    pub fn draw(
        &mut self,
        state: &State,
        terminal: &mut Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|f| {
            let active = true;
            self.current.draw(state, f, f.size(), active);
        })?;
        Ok(())
    }

    pub fn switch(&mut self, state: &mut State) {
        if self.main.is_some() {
            self.current = self.main.take().unwrap();
        } else {
            self.current = self.game.take().unwrap();
        }
        self._resize(state);
    }

    fn _resize(&mut self, state: &mut State) {
        let rect = Rect {
            x: 0,
            y: 0,
            width: state.screen_width,
            height: state.screen_height,
        };
        self.current.resize(state, rect);
    }
}

// Helper type to wrap a list
pub struct StatefulList<T> {
    pub state: ListState,
    pub items: Vec<T>,
}

impl<T> StatefulList<T> {
    pub fn new(items: Vec<T>) -> StatefulList<T> {
        StatefulList { state: ListState::default(), items }
    }

    pub fn next(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i >= self.items.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn previous(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i == 0 {
                    self.items.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }
}
