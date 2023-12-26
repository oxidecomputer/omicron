// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! State related to downstream service connectivity
//!
//! wicketd connectivity is measured directly, and MGS connectivity is proxied
//! through wicketd.

use crate::ui::defaults::style;
use ratatui::style::Style;
use ratatui::text::Span;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// This should be greater than the highest poll value for each service
const LIVENESS_THRESHOLD: Duration = Duration::from_secs(30);

/// A status bar shown at the bottom of the screen.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceStatus {
    wicketd_last_seen: Option<Duration>,
    mgs_last_seen: Option<Duration>,
}

impl ServiceStatus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment all the status timers by `time` if they
    /// have ever been heard from.
    ///
    /// Return true if a second has rolled over and we should trigger a redraw.
    pub fn advance_all(&mut self, time: Duration) -> bool {
        let mut redraw = false;
        if let Some(d) = self.wicketd_last_seen.as_mut() {
            let prev = d.as_secs();
            *d += time;
            redraw |= d.as_secs() > prev;
        }
        if let Some(d) = self.mgs_last_seen.as_mut() {
            let prev = d.as_secs();
            *d += time;
            redraw |= d.as_secs() > prev;
        }

        redraw
    }

    pub fn reset_wicketd(&mut self, elapsed: Duration) {
        self.wicketd_last_seen = Some(elapsed);
    }

    pub fn reset_mgs(&mut self, elapsed: Duration) {
        self.mgs_last_seen = Some(elapsed);
    }

    pub fn mgs_liveness(&self) -> Liveness {
        Self::liveness(self.mgs_last_seen)
    }

    pub fn wicketd_liveness(&self) -> Liveness {
        Self::liveness(self.wicketd_last_seen)
    }

    fn liveness(elapsed: Option<Duration>) -> Liveness {
        elapsed.map_or(Liveness::NoResponse, |d| {
            if d > LIVENESS_THRESHOLD {
                Liveness::Delayed(d)
            } else {
                Liveness::Live(d)
            }
        })
    }
}

/// Liveness that's been computed so far.
///
/// `Duration`s map to time since last valid response
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Liveness {
    Live(Duration),
    Delayed(Duration),
    NoResponse,
}

impl Liveness {
    pub fn to_spans(&self) -> Vec<Span<'static>> {
        match self {
            Liveness::Live(duration) => vec![
                Span::styled("CONNECTED", style::connected()),
                Span::raw(" "),
                Self::secs_span(duration.as_secs(), style::connected()),
            ],
            Liveness::Delayed(duration) => vec![
                Span::styled("DELAYED", style::delayed()),
                Span::raw(" "),
                Self::secs_span(duration.as_secs(), style::delayed()),
            ],
            Liveness::NoResponse => {
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
