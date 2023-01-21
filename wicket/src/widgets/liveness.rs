// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use tui::{
    style::Style,
    text::{Span, Spans},
};

use crate::defaults::colors::{OX_GRAY_DARK, OX_GREEN_LIGHT, OX_YELLOW};

/// Tracker used by a single instance of liveness.
#[derive(Debug)]
pub struct LivenessState {
    // Some means that the stopwatch hasn't yet been initialized.
    stopwatch: Option<libsw::Stopwatch>,
    live_threshold: Duration,
}

impl LivenessState {
    pub fn new(live_threshold: Duration) -> Self {
        Self { stopwatch: None, live_threshold }
    }

    /// Resets or initializes the stopwatch state.
    pub fn reset(&mut self, elapsed: Duration) {
        self.stopwatch = Some(libsw::Stopwatch::with_elapsed_started(elapsed));
    }

    /// Compute the liveness for this state.
    pub fn compute(&self) -> ComputedLiveness {
        if let Some(stopwatch) = &self.stopwatch {
            let elapsed = stopwatch.elapsed();
            if elapsed > self.live_threshold {
                ComputedLiveness::Delayed(elapsed.as_secs())
            } else {
                ComputedLiveness::Live(elapsed.as_secs())
            }
        } else {
            ComputedLiveness::Never
        }
    }
}

/// Liveness that's been computed so far.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ComputedLiveness {
    /// The data is the number of seconds.
    Live(u64),

    /// The data is the number of seconds.
    Delayed(u64),

    Never,
}

impl ComputedLiveness {
    pub fn to_spans(&self) -> Spans<'static> {
        match self {
            ComputedLiveness::Live(secs) => Spans::from(vec![
                Span::styled("live", Style::default().fg(OX_GREEN_LIGHT)),
                Span::raw(" "),
                Self::secs_span(*secs),
            ]),
            ComputedLiveness::Delayed(secs) => Spans::from(vec![
                Span::styled("delayed", Style::default().fg(OX_YELLOW)),
                Span::raw(" "),
                Self::secs_span(*secs),
            ]),
            ComputedLiveness::Never => Spans::from(Span::styled(
                "never",
                Style::default().fg(OX_YELLOW),
            )),
        }
    }

    fn secs_span(secs: u64) -> Span<'static> {
        if secs < 1 {
            Span::styled("(<1s)", Style::default().fg(OX_GRAY_DARK))
        } else {
            Span::styled(
                format!("({secs}s)"),
                Style::default().fg(OX_GRAY_DARK),
            )
        }
    }
}
