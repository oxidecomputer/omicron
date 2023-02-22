// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ControlId;
use crate::{Action, Event, Frame, State, Term};
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};

/// The [`MainScreen`] is the primary UI element of the terminal, covers the
/// entire terminal window/buffer and is visible for all interactions except
/// the initial splash screen animation.
///
/// This structure allows us to maintain similar styling and navigation
/// throughout wicket with a minimum of code.
///
/// Specific functionality is put inside [`Pane`]s, which can be customized
/// as needed.
pub struct MainScreen {
    //selected: ControlId,
}

impl MainScreen {
    pub fn new() -> MainScreen {
        MainScreen {}
    }

    /// Draw the [`MainScreen`]
    pub fn draw(
        &self,
        state: &State,
        terminal: &mut Term,
    ) -> anyhow::Result<()> {
        terminal.draw(|frame| {
            let mut rect = frame.size();
            rect.height -= 1;

            // Size the individual components of the screen
            let horizontal_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .margin(1)
                .constraints(
                    [Constraint::Length(20), Constraint::Max(1000)].as_ref(),
                )
                .split(rect);

            let vertical_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Max(1000),
                    Constraint::Length(3),
                ])
                .split(horizontal_chunks[1]);

            self.draw_sidebar(frame, state, horizontal_chunks[0]);
            self.draw_topbar(frame, state, vertical_chunks[0]);
            self.draw_pane(frame, state, vertical_chunks[1]);
            self.draw_bottombar(frame, state, vertical_chunks[2]);
        })?;
        Ok(())
    }

    /// Handle an [`Event`] to update state and output any necessary actions for the
    /// system to take.
    pub fn on(&mut self, state: &mut State, event: Event) -> Option<Action> {
        None
    }

    fn draw_sidebar(&self, frame: &Frame, state: &State, rect: Rect) {}
    fn draw_topbar(&self, frame: &Frame, state: &State, rect: Rect) {}
    fn draw_pane(&self, frame: &Frame, state: &State, rect: Rect) {}
    fn draw_bottombar(&self, frame: &Frame, state: &State, rect: Rect) {}
}
