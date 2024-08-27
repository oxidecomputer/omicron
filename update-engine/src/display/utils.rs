// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility displayers.

use std::fmt;

/// Given an index and a count of total steps, displays `{current}/{total}`.
///
/// Here:
///
/// * `current` is `index + 1`.
/// * If `padded` is `true`, `current` is right-aligned and padded with spaces
///   to the width of `total`.
///
/// # Examples
///
/// ```
/// use update_engine::display::StepIndexDisplay;
///
/// let display = StepIndexDisplay::new(0, 8);
/// assert_eq!(display.to_string(), "1/8");
/// let display = StepIndexDisplay::new(82, 230);
/// assert_eq!(display.to_string(), "83/230");
/// let display = display.padded(true);
/// assert_eq!(display.to_string(), " 83/230");
/// ```
#[derive(Debug)]
pub struct StepIndexDisplay {
    index: usize,
    total: usize,
    padded: bool,
}

impl StepIndexDisplay {
    /// Create a new `StepIndexDisplay`.
    ///
    /// The index is 0-based (i.e. 1 is added to it when it is displayed).
    pub fn new(index: usize, total: usize) -> Self {
        Self { index, total, padded: false }
    }

    pub fn padded(self, padded: bool) -> Self {
        Self { padded, ..self }
    }
}

impl fmt::Display for StepIndexDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.padded {
            let width = self.total.to_string().len();
            write!(f, "{:>width$}/{}", self.index + 1, self.total)
        } else {
            write!(f, "{}/{}", self.index + 1, self.total)
        }
    }
}
