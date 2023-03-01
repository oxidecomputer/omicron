// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State related to downstream service connectivity
//!
//! wicketd connectivity is measured directly, and MGS connectivity is proxied
//! through wicketd.

use crate::ui::defaults::style;
use std::time::{Duration, Instant};
use tui::style::Style;
use tui::text::Span;

/// A status bar shown at the bottom of the screen.
#[derive(Debug)]
pub struct ServiceStatus {
    pub wicketd_liveness: LivenessState,
    pub mgs_liveness: LivenessState,
    last_redraw_at: Option<Instant>,
}

impl ServiceStatus {
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
}

/// Tracker used by a single instance of liveness.
#[derive(Debug)]
pub struct LivenessState {
    // None means that the stopwatch hasn't yet been initialized.
    stopwatch: Option<libsw::Stopwatch>,

    #[allow(unused)]
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
    pub fn to_spans(&self) -> Vec<Span<'static>> {
        match self {
            ComputedLiveness::Live(secs) => vec![
                Span::styled("CONNECTED", style::connected()),
                Span::raw(" "),
                Self::secs_span(*secs, style::connected()),
            ],
            ComputedLiveness::Delayed(secs) => vec![
                Span::styled("DELAYED", style::delayed()),
                Span::raw(" "),
                Self::secs_span(*secs, style::delayed()),
            ],
            ComputedLiveness::NoResponse => {
                vec![Span::styled("NO RESPONSE", style::delayed())]
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
