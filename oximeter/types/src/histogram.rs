// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # [`Histogram`] Examples
//!
//! ```rust
//! # // Rename the types crate so the doctests can refer to the public
//! # // `oximeter` crate, not the private impl.
//! # use oximeter_types as oximeter;
//! use oximeter::histogram::Histogram;
//!
//! let hist = Histogram::with_bins(&[(0..10).into(), (10..100).into()]).unwrap();
//! assert_eq!(hist.n_bins(), 4); // Added bins for ..0 on the left and 100.. on the right
//!
//! let hist = Histogram::with_bins(&[(..f64::NAN).into()]).is_err(); // No-no
//! ```
//!
//! ```rust
//! # // Rename the types crate so the doctests can refer to the public
//! # // `oximeter` crate, not the private impl.
//! # use oximeter_types as oximeter;
//! use oximeter::histogram::{Histogram, BinRange};
//! use std::ops::{RangeBounds, Bound};
//!
//! let hist: Histogram<f64> = Histogram::span_decades(-1, 1).unwrap();
//! let bins = hist.iter().collect::<Vec<_>>();
//!
//! // There are 9 bins per power of 10, plus 1 additional for everything
//! // below and above the power of 10.
//! assert_eq!(bins.len(), 2 * 9 + 2);
//!
//! // First bin is from the left support edge to the first bin
//! assert_eq!(bins[0].range.end_bound(), Bound::Excluded(&0.1));
//!
//! // First decade of bins is `[0.1, 0.2, ...)`.
//! assert_eq!(bins[1].range, BinRange::range(0.1, 0.2));
//!
//! // Note that these are floats, which are notoriously difficult to
//! // compare. The bin edges are not _exact_, but quite close.
//! let BinRange::Range { start, end } = bins[2].range else { unreachable!() };
//! let BinRange::Range {
//!     start: expected_start,
//!     end: expected_end,
//! } = BinRange::range(0.2, 0.3) else { unreachable!() };
//! assert_eq!(start, expected_start);
//! approx::assert_ulps_eq!(end, expected_end);
//!
//! // Second decade is `[1.0, 2.0, 3.0, ...]`
//! assert_eq!(bins[9].range, BinRange::range(0.9, 1.0));
//! assert_eq!(bins[10].range, BinRange::range(1.0, 2.0));
//! assert_eq!(bins[11].range, BinRange::range(2.0, 3.0));
//!
//! // Ends at the third decade, so the last bin is the remainder of the support
//! assert_eq!(bins[19].range, BinRange::from(10.0));
//! ```

pub use oximeter_types_versions::latest::histogram::*;
