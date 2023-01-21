// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use tokio::time::Instant;
use tui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    style::Style,
    text::{Span, Spans},
    widgets::{Row, Table, Widget},
};

use crate::{
    defaults::colors::{OX_GRAY, OX_GREEN_DARKEST},
    Frame,
};

use super::LivenessState;

/// A status bar shown at the bottom of the screen.
#[derive(Debug)]
pub struct StatusBar {
    wicketd_liveness: LivenessState,
    mgs_liveness: LivenessState,
    last_redraw_at: Option<Instant>,
}

impl StatusBar {
    pub fn new() -> Self {
        Self {
            // Wicketd is polled every 500ms by wicket. Setting a 1 second
            // liveness threshold means that under normal operation it should
            // never flip to delayed.
            wicketd_liveness: LivenessState::new(Duration::from_secs(1)),
            // MGS is polled every 10 seconds by wicketd. Set a 11 second
            // threshold to account for wicket -> wicketd and wicketd -> MGS
            // delay.
            mgs_liveness: LivenessState::new(Duration::from_secs(11)),
            last_redraw_at: None,
        }
    }

    pub fn reset_wicketd(&mut self, elapsed: Duration) {
        self.wicketd_liveness.reset(elapsed);
        // Force a redraw.
        self.last_redraw_at = None;
    }

    pub fn reset_mgs(&mut self, elapsed: Duration) {
        self.mgs_liveness.reset(elapsed);
        // Force a redraw.
        self.last_redraw_at = None;
    }

    /// Returns true if a redraw needs to happen, resetting the internal timer.
    pub fn should_redraw(&mut self) -> bool {
        if let Some(instant) = &mut self.last_redraw_at {
            let elapsed = instant.elapsed();
            if elapsed >= Duration::from_secs(1) {
                *instant = Instant::now();
                true
            } else {
                false
            }
        } else {
            // Initialize the last-redraw timer.
            self.last_redraw_at = Some(Instant::now());
            true
        }
    }

    /// Returns a widget which can be used to draw the status bar.
    pub fn to_widget(&self) -> StatusBarWidget<'_> {
        StatusBarWidget { bar: self }
    }

    /// Draws the status bar on the bottom of the screen.
    pub fn draw(&self, f: &mut Frame<'_>) {
        let widget = self.to_widget();
        let mut rect = f.size();

        // Draw in the bottom row.
        rect.y = rect.height - 1;
        rect.height = 1;

        f.render_widget(widget, rect);
    }
}

/// The status bar as a widget.
pub struct StatusBarWidget<'a> {
    bar: &'a StatusBar,
}

impl<'a> Widget for StatusBarWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // "delayed (30s)" is 13 characters
        const LIVENESS_CELL_WIDTH: u16 = 13;

        // Render the status bar as a table, using a single row.
        let row = Row::new(vec![
            "wicketd".into(),
            center_pad(
                self.bar.wicketd_liveness.compute().to_spans(),
                LIVENESS_CELL_WIDTH,
            ),
            "MGS".into(),
            center_pad(
                self.bar.mgs_liveness.compute().to_spans(),
                LIVENESS_CELL_WIDTH,
            ),
        ]);

        let table_style = Style::default().fg(OX_GRAY).bg(OX_GREEN_DARKEST);

        let table = Table::new(std::iter::once(row))
            .widths(&[
                Constraint::Min(7),
                Constraint::Min(LIVENESS_CELL_WIDTH),
                Constraint::Min(3),
                Constraint::Min(LIVENESS_CELL_WIDTH),
            ])
            .style(table_style)
            .column_spacing(1);

        table.render(area, buf);
    }
}

fn center_pad(mut spans: Spans<'_>, total_width: u16) -> Spans<'_> {
    let total_width = total_width as usize;
    if total_width > spans.width() {
        // Add left and right padding.
        let left_width = (total_width - spans.width()) / 2;
        let right_width = (total_width + 1 - spans.width()) / 2;
        spans.0.insert(0, Span::raw(format!("{:left_width$}", "")));
        spans.0.push(Span::raw(format!("{:right_width$}", "")));
    }
    spans
}
