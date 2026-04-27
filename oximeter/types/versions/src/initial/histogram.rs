// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for managing metrics that are histograms.

use super::quantile::Quantile;
use crate::impls::histogram::HistogramSupport;
use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// A type storing a range over `T`.
///
/// This type supports ranges similar to the `RangeTo`, `Range` and `RangeFrom` types in the
/// standard library. Those cover `(..end)`, `(start..end)`, and `(start..)` respectively.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(rename = "BinRange{T}")]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BinRange<T> {
    /// A range unbounded below and exclusively above, `..end`.
    RangeTo { end: T },

    /// A range bounded inclusively below and exclusively above, `start..end`.
    Range { start: T, end: T },

    /// A range bounded inclusively below and unbounded above, `start..`.
    RangeFrom { start: T },
}

/// Type storing bin edges and a count of samples within it.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(rename = "Bin{T}")]
pub struct Bin<T> {
    /// The range of the support covered by this bin.
    pub range: BinRange<T>,

    /// The total count of samples in this bin.
    pub count: u64,
}

/// Histogram metric
///
/// A histogram maintains the count of any number of samples, over a set of bins. Bins are
/// specified on construction via their _left_ edges, inclusive. There can't be any "gaps" in the
/// bins, and an additional bin may be added to the left, right, or both so that the bins extend to
/// the entire range of the support.
///
/// Note that any gaps, unsorted bins, or non-finite values will result in an error.
//
// Example
// -------
// ```rust
// use oximeter::histogram::{BinRange, Histogram};
//
// let edges = [0i64, 10, 20];
// let mut hist = Histogram::new(&edges).unwrap();
// assert_eq!(hist.n_bins(), 4); // One additional bin for the range (20..)
// assert_eq!(hist.n_samples(), 0);
// hist.sample(4);
// hist.sample(100);
// assert_eq!(hist.n_samples(), 2);
//
// let data = hist.iter().collect::<Vec<_>>();
// assert_eq!(data[0].range, BinRange::range(i64::MIN, 0)); // An additional bin for `..0`
// assert_eq!(data[0].count, 0); // Nothing is in this bin
//
// assert_eq!(data[1].range, BinRange::range(0, 10)); // The range `0..10`
// assert_eq!(data[1].count, 1); // 4 is sampled into this bin
// ```
//
// Notes
// -----
//
// Histograms may be constructed either from their left bin edges, or from a sequence of ranges.
// In either case, the left-most bin may be converted upon construction. In particular, if the
// left-most value is not equal to the minimum of the support, a new bin will be added from the
// minimum to that provided value. If the left-most value _is_ the support's minimum, because the
// provided bin was unbounded below, such as `(..0)`, then that bin will be converted into one
// bounded below, `(MIN..0)` in this case.
//
// The short of this is that, most of the time, it shouldn't matter. If one specifies the extremes
// of the support as their bins, be aware that the left-most may be converted from a
// `BinRange::RangeTo` into a `BinRange::Range`. In other words, the first bin of a histogram is
// _always_ a `Bin::Range` or a `Bin::RangeFrom` after construction. In fact, every bin is one of
// those variants, the `BinRange::RangeTo` is only provided as a convenience during construction.
//
// Floating point support
// ----------------------
//
// This type allows both integer and floating-point types as the support of the
// distribution. However, developers should be very aware of the difficulties
// around floating point comparisons. It's notoriously hard to understand,
// predict, and control floating point comparisons. Resolution changes with the
// magnitude of the values; cancellation can creep in in unexpected ways; and
// arithmetic operations often lead to rounding errors. In general, one should
// strongly prefer using an integer type for the histogram support, along with a
// well-understood unit / resolution. Developers are also encouraged to
// carefully check that the bins generated from methods like
// `Histogram::with_log_linear_bins()` are exactly the ones expected.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(rename = "Histogram{T}")]
pub struct Histogram<T>
where
    T: HistogramSupport,
{
    /// The start time of the histogram.
    pub(crate) start_time: DateTime<Utc>,
    /// The bins of the histogram.
    pub(crate) bins: Vec<Bin<T>>,
    /// The total number of samples in the histogram.
    pub(crate) n_samples: u64,
    /// The minimum value of all samples in the histogram.
    pub(crate) min: T,
    /// The maximum value of all samples in the histogram.
    pub(crate) max: T,
    /// The sum of all samples in the histogram.
    pub(crate) sum_of_samples: T::Width,
    /// M2 for Welford's algorithm for variance calculation.
    ///
    /// Read about [Welford's algorithm](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm)
    /// for more information on the algorithm.
    pub(crate) squared_mean: f64,
    /// p50 Quantile
    pub(crate) p50: Quantile,
    /// p95 Quantile
    pub(crate) p90: Quantile,
    /// p99 Quantile
    pub(crate) p99: Quantile,
}
