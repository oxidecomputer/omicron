// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility displayers.

use std::fmt;

/// Given current and total, displays `{current}/{total}`.
///
/// * If the `index_and_total` constructor is called, then `current` is `index
///   + 1`.
/// * If `padded` is `true`, `current` is right-aligned and padded with spaces
///   to the width of `total`.
///
/// # Examples
///
/// ```
/// use update_engine::display::ProgressRatioDisplay;
///
/// let display = ProgressRatioDisplay::new(0, 8);
/// assert_eq!(display.to_string(), "1/8");
/// let display = ProgressRatioDisplay::new(82, 230);
/// assert_eq!(display.to_string(), "83/230");
/// let display = display.padded(true);
/// assert_eq!(display.to_string(), " 83/230");
/// ```
#[derive(Debug)]
pub struct ProgressRatioDisplay {
    current: u64,
    total: u64,
    padded: bool,
}

impl ProgressRatioDisplay {
    /// Create a new `ProgressRatioDisplay` with current and total values.
    ///
    /// `current` is considered to be 1-based. For example, "1 job done of 8".
    pub fn current_and_total<T: ToU64>(current: T, total: T) -> Self {
        Self { current: current.to_u64(), total: total.to_u64(), padded: false }
    }

    /// Create a new `ProgressRatioDisplay` with index and total values.
    ///
    /// The index is 0-based (i.e. 1 is added to it). For example, step index 0
    /// out of 8 total steps.
    pub fn index_and_total<T: ToU64>(index: T, total: T) -> Self {
        Self {
            current: index.to_u64() + 1,
            total: total.to_u64(),
            padded: false,
        }
    }

    /// If set to true, the current value is padded to the same width as the
    /// total.
    pub fn padded(self, padded: bool) -> Self {
        Self { padded, ..self }
    }
}

impl fmt::Display for ProgressRatioDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.padded {
            let width = self.total.to_string().len();
            write!(f, "{:>width$}/{}", self.current + 1, self.total)
        } else {
            write!(f, "{}/{}", self.current + 1, self.total)
        }
    }
}

/// Trait that abstracts over `usize` and `u64`.
///
/// There are no `From` implementations between `usize` and `u64`, but we
/// assert below that all the architectures we support are 64-bit.
pub trait ToU64 {
    fn to_u64(self) -> u64;
}

const _: () = {
    assert!(
        std::mem::size_of::<usize>() == std::mem::size_of::<u64>(),
        "usize and u64 are the same size"
    );
};

impl ToU64 for usize {
    #[inline]
    fn to_u64(self) -> u64 {
        self as u64
    }
}

impl ToU64 for u64 {
    #[inline]
    fn to_u64(self) -> u64 {
        self
    }
}
