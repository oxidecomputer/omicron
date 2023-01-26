// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use tui::{
    style::Style,
    text::{Span, Spans},
};

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
            ComputedLiveness::NoResponse
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

    NoResponse,
}

impl ComputedLiveness {
    pub fn to_spans(&self, styles: &LivenessStyles) -> Spans<'static> {
        match self {
            ComputedLiveness::Live(secs) => Spans::from(vec![
                Span::styled("live", styles.live),
                Span::raw(" "),
                Self::secs_span(*secs, styles.time),
            ]),
            ComputedLiveness::Delayed(secs) => Spans::from(vec![
                Span::styled("delayed", styles.delayed),
                Span::raw(" "),
                Self::secs_span(*secs, styles.time),
            ]),
            ComputedLiveness::NoResponse => {
                Spans::from(Span::styled("no response", styles.delayed))
            }
        }
    }

    fn secs_span(secs: u64, time_style: Style) -> Span<'static> {
        if secs < 1 {
            Span::styled("(<1s)", time_style)
        } else {
            Span::styled(format!("({secs}s)"), time_style)
        }
    }
}

#[derive(Clone, Debug)]
pub struct LivenessStyles {
    pub live: Style,
    pub delayed: Style,
    pub time: Style,
}
