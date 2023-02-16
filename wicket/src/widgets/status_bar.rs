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
    defaults::colors::{
        OX_GRAY, OX_GRAY_DARK, OX_GREEN_DARKEST, OX_GREEN_LIGHT, OX_YELLOW,
    },
    Frame,
};

use super::{liveness::LivenessStyles, LivenessState};

/// A status bar shown at the bottom of the screen.
#[derive(Debug)]
pub struct StatusBar {
    wicketd_liveness: LivenessState,
    mgs_liveness: LivenessState,
    last_redraw_at: Option<Instant>,
    styles: StatusBarStyles,
}

impl StatusBar {
    pub fn new() -> Self {
        let styles = StatusBarStyles {
            table: Style::default().fg(OX_GRAY).bg(OX_GREEN_DARKEST),
            liveness: LivenessStyles {
                live: Style::default().fg(OX_GREEN_LIGHT),
                delayed: Style::default().fg(OX_YELLOW),
                time: Style::default().fg(OX_GRAY_DARK),
            },
        };

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
            styles,
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

#[derive(Clone, Debug)]
pub struct StatusBarStyles {
    pub table: Style,
    pub liveness: LivenessStyles,
}

/// The status bar as a widget.
pub struct StatusBarWidget<'a> {
    bar: &'a StatusBar,
}

impl<'a> Widget for StatusBarWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // "delayed (30s)" is 13 characters
        const LIVENESS_CELL_WIDTH: u16 = 13;

        let (wicketd_spans, wicketd_spans_width) = center_pad(
            self.bar
                .wicketd_liveness
                .compute()
                .to_spans(&self.bar.styles.liveness),
            LIVENESS_CELL_WIDTH,
        );
        let (mgs_spans, mgs_spans_width) = center_pad(
            self.bar.mgs_liveness.compute().to_spans(&self.bar.styles.liveness),
            LIVENESS_CELL_WIDTH,
        );

        // Render the status bar as a table, using a single row.
        let row = Row::new(vec![
            "wicketd".into(),
            wicketd_spans,
            // A bit of spacing between wicketd and MGS.
            " ".into(),
            "MGS".into(),
            mgs_spans,
        ]);

        let table_widths = [
            Constraint::Min(7),
            Constraint::Min(wicketd_spans_width),
            Constraint::Min(1),
            Constraint::Min(3),
            Constraint::Min(mgs_spans_width),
        ];

        let table = Table::new(std::iter::once(row))
            .widths(&table_widths)
            .style(self.bar.styles.table)
            .column_spacing(1);

        table.render(area, buf);
    }
}

fn center_pad(mut spans: Spans<'_>, min_width: u16) -> (Spans<'_>, u16) {
    let cur_width = spans.width() as u16;
    if min_width > cur_width {
        // Add left and right padding.
        let left_width = ((min_width - cur_width) / 2) as usize;
        let right_width = ((min_width + 1 - cur_width) / 2) as usize;
        if left_width > 0 {
            spans.0.insert(0, Span::raw(format!("{:left_width$}", "")));
        }
        if right_width > 0 {
            spans.0.push(Span::raw(format!("{:right_width$}", "")));
        }
        (spans, min_width)
    } else {
        (spans, cur_width)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_center_pad() {
        let tests = [
            (Spans::from(""), 0, Spans::from(""), 0),
            (Spans::from("abc"), 3, Spans::from("abc"), 3),
            (Spans::from("abc"), 2, Spans::from("abc"), 3),
            (
                Spans::from("abc"),
                4,
                Spans::from(vec!["abc".into(), " ".into()]),
                4,
            ),
            (
                Spans::from("abc"),
                5,
                Spans::from(vec![" ".into(), "abc".into(), " ".into()]),
                5,
            ),
        ];

        for (input, min_width, output, output_width) in tests {
            assert_eq!(
                center_pad(input.clone(), min_width),
                (output, output_width),
                "for input {input:?}, actual output matches expected"
            );
        }
    }
}
