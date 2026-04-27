// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Data structure for expressing quantile estimation.
//! This is based on the P² heuristic algorithm for dynamic
//! calculation of the median and other quantiles. The estimates
//! are produced dynamically as the observations are generated.
//! The observations are not stored; therefore, the algorithm has
//! a very small and fixed storage requirement regardless of the
//! number of observations.
//!
//! Read the [paper](https://www.cs.wustl.edu/~jain/papers/ftp/psqr.pdf)
//! for more specifics.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

pub(crate) const FILLED_MARKER_LEN: usize = 5;

/// Structure for estimating the p-quantile of a population.
///
/// This is based on the P² algorithm for estimating quantiles using
/// constant space.
///
/// The algorithm consists of maintaining five markers: the
/// minimum, the p/2-, p-, and (1 + p)/2 quantiles, and the maximum.
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Quantile {
    /// The p value for the quantile.
    pub(crate) p: f64,
    /// The heights of the markers.
    pub(crate) marker_heights: [f64; FILLED_MARKER_LEN],
    /// The positions of the markers.
    ///
    /// We track sample size in the 5th position, as useful observations won't
    /// start until we've filled the heights at the 6th sample anyway
    /// This does deviate from the paper, but it's a more useful representation
    /// that works according to the paper's algorithm.
    pub(crate) marker_positions: [u64; FILLED_MARKER_LEN],
    /// The desired marker positions.
    pub(crate) desired_marker_positions: [f64; FILLED_MARKER_LEN],
}
