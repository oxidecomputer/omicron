// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for managing metrics that are histograms.

// Copyright 2024 Oxide Computer Company

use super::Quantile;
use super::QuantileError;
use chrono::DateTime;
use chrono::Utc;
use num::CheckedAdd;
use num::CheckedMul;
use num::Float;
use num::Integer;
use num::NumCast;
use num::traits::Bounded;
use num::traits::FromPrimitive;
use num::traits::Num;
use num::traits::ToPrimitive;
use num::traits::Zero;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Ordering;
use std::num::NonZeroUsize;
use std::ops::AddAssign;
use std::ops::Bound;
use std::ops::Range;
use std::ops::RangeBounds;
use std::ops::RangeFrom;
use std::ops::RangeTo;
use thiserror::Error;

/// A trait used to identify the data types that can be used as the support of a histogram.
pub trait HistogramSupport:
    std::fmt::Debug
    + Copy
    + Default
    + PartialOrd
    + Bounded
    + JsonSchema
    + Serialize
    + Clone
    + Num
    + Zero
    + FromPrimitive
    + ToPrimitive
    + AddAssign
    + NumCast
    + 'static
{
    type Power;
    type Width: HistogramAdditiveWidth;
    /// Return true if `self` is a finite number, not NAN or infinite.
    fn is_finite(&self) -> bool;
}

/// Used for designating the subset of types that can be used as the width for
/// summing up values in a histogram.
pub trait HistogramAdditiveWidth: HistogramSupport {}

impl HistogramAdditiveWidth for i64 {}
impl HistogramAdditiveWidth for f64 {}

macro_rules! impl_int_histogram_support {
    ($($type:ty),+) => {
        $(
            impl HistogramSupport for $type {
                type Power = u16;
                type Width = i64;
                fn is_finite(&self) -> bool {
                    true
                }
            }
        )+
    }
}

impl_int_histogram_support! { i8, u8, i16, u16, i32, u32, i64, u64 }

macro_rules! impl_float_histogram_support {
    ($($type:ty),+) => {
        $(
            impl HistogramSupport for $type {
                type Power = i16;
                type Width = f64;
                fn is_finite(&self) -> bool {
                    <$type>::is_finite(*self)
                }
            }
        )+
    }
}

impl_float_histogram_support! { f32, f64 }

/// Errors related to constructing histograms or adding samples into them.
#[derive(Debug, Clone, Error, JsonSchema, Serialize, Deserialize)]
#[serde(tag = "type", content = "content", rename_all = "snake_case")]
pub enum HistogramError {
    /// An attempt to construct a histogram with an empty set of bins.
    #[error("Bins may not be empty")]
    EmptyBins,

    /// An attempt to construct a histogram with non-monotonic bins.
    #[error("Bins must be monotonically increasing")]
    NonmonotonicBins,

    /// A non-finite was encountered, either as a bin edge or a sample.
    #[error("Bin edges and samples must be finite values, not Infinity or NaN")]
    NonFiniteValue,

    /// Error returned when two neighboring bins are not adjoining (there's space between them)
    #[error("Neigboring bins {left} and {right} are not adjoining")]
    NonAdjoiningBins { left: String, right: String },

    /// Bin and count arrays are of different sizes.
    #[error(
        "Bin and count arrays must have the same size, found {n_bins} and {n_counts}"
    )]
    ArraySizeMismatch { n_bins: usize, n_counts: usize },

    /// Error returned when a quantization error occurs.
    #[error("Quantization error")]
    Quantization(#[from] QuantizationError),

    /// Error returned when a quantile error occurs.
    #[error("Quantile error")]
    Quantile(#[from] QuantileError),
}

/// Errors occurring during quantizated bin generation.
#[derive(
    Clone, Debug, Deserialize, JsonSchema, Serialize, thiserror::Error,
)]
#[serde(tag = "type", content = "content", rename_all = "snake_case")]
pub enum QuantizationError {
    #[error("Overflow during bin generation")]
    Overflow,

    #[error("Precision error during bin generation")]
    Precision,

    #[error("Base must in the range [1, 32]")]
    InvalidBase,

    #[error("Number of steps must be > 1 and fit in the output type")]
    InvalidSteps,

    #[error(
        "Number of steps must be multiple of base and \
        evenly divide a power of the base"
    )]
    UnevenStepsForBase,

    #[error("Low power must be strictly less than high power")]
    PowersOutOfOrder,
}

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

impl<T> std::fmt::Display for BinRange<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BinRange::RangeTo { end } => write!(f, "< {end}"),
            BinRange::Range { start, end } => write!(f, "[{start}, {end})"),
            BinRange::RangeFrom { start } => write!(f, ">= {start}"),
        }
    }
}

impl<T> BinRange<T>
where
    T: HistogramSupport,
{
    /// Construct a range unbounded below and bounded exclusively from above.
    pub fn to(end: T) -> Self {
        BinRange::RangeTo { end }
    }

    /// Construct a range bounded inclusively from below and exclusively from above.
    pub fn range(start: T, end: T) -> Self {
        BinRange::Range { start, end }
    }

    /// Construct a range bounded inclusively from below and unbounded from above.
    pub fn from(start: T) -> Self {
        BinRange::RangeFrom { start }
    }

    /// Order the given *value* relative to the *bin*.
    ///
    /// Equal means the bin contains the value, Less means the value is less than the left edge of
    /// the bin, and Greater means the value is greater than the right edge of the bin.
    fn cmp(&self, value: &T) -> Ordering {
        if self.contains(value) {
            Ordering::Equal
        } else {
            match self {
                // If the bin doesn't contain the value but is unbounded below, the value must be
                // greater than the bin.
                BinRange::RangeTo { .. } => Ordering::Greater,
                // If the bin doesn't contain the value but is unbounded above, the value must be
                // less than the bin.
                BinRange::RangeFrom { .. } => Ordering::Less,
                BinRange::Range { start, .. } => {
                    if value < start {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
            }
        }
    }
}

impl<T> From<Range<T>> for BinRange<T>
where
    T: HistogramSupport,
{
    fn from(range: Range<T>) -> Self {
        BinRange::range(range.start, range.end)
    }
}

impl<T> From<RangeTo<T>> for BinRange<T>
where
    T: HistogramSupport,
{
    fn from(range: RangeTo<T>) -> Self {
        BinRange::RangeTo { end: range.end }
    }
}

impl<T> From<RangeFrom<T>> for BinRange<T>
where
    T: HistogramSupport,
{
    fn from(range: RangeFrom<T>) -> Self {
        BinRange::RangeFrom { start: range.start }
    }
}

impl<T> RangeBounds<T> for BinRange<T>
where
    T: HistogramSupport,
{
    fn start_bound(&self) -> Bound<&T> {
        match self {
            BinRange::RangeTo { .. } => Bound::Unbounded,
            BinRange::Range { start, .. } => Bound::Included(start),
            BinRange::RangeFrom { start } => Bound::Included(start),
        }
    }

    fn end_bound(&self) -> Bound<&T> {
        match self {
            BinRange::RangeTo { end } => Bound::Excluded(end),
            BinRange::Range { end, .. } => Bound::Excluded(end),
            BinRange::RangeFrom { .. } => Bound::Unbounded,
        }
    }
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

/// Internal, creation-specific newtype wrapper around `Vec<Bin<T>>` to
/// implement conversion(s).
struct Bins<T>(Vec<Bin<T>>);

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
    start_time: DateTime<Utc>,
    /// The bins of the histogram.
    bins: Vec<Bin<T>>,
    /// The total number of samples in the histogram.
    n_samples: u64,
    /// The minimum value of all samples in the histogram.
    min: T,
    /// The maximum value of all samples in the histogram.
    max: T,
    /// The sum of all samples in the histogram.
    sum_of_samples: T::Width,
    /// M2 for Welford's algorithm for variance calculation.
    ///
    /// Read about [Welford's algorithm](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm)
    /// for more information on the algorithm.
    squared_mean: f64,
    /// p50 Quantile
    p50: Quantile,
    /// p95 Quantile
    p90: Quantile,
    /// p99 Quantile
    p99: Quantile,
}

/// A trait for recording samples into a histogram.
pub trait Record<T: HistogramSupport> {
    /// Add a new sample into the histogram.
    ///
    /// This bumps the internal counter at the bin containing `value`. An `Err` is returned if the
    /// sample is not within the distribution's support (non-finite).
    fn sample(&mut self, value: T) -> Result<(), HistogramError>;
}

macro_rules! impl_int_sample {
    ($($type:ty),+) => {
        $(
            impl Record<$type> for Histogram<$type> where $type: HistogramSupport + Integer + CheckedAdd + CheckedMul {
                fn sample(&mut self, value: $type) -> Result<(), HistogramError> {
                    ensure_finite(value)?;

                    if self.n_samples == 0 {
                        self.min = <$type>::max_value();
                        self.max = <$type>::min_value();
                    }

                    // For squared mean (M2) calculation, before we update the
                    // count.
                    //
                    // Some of these casts are lossless (i8, u8 etc) and some
                    // are not (i64, u64). The point of the `cast_lossless`
                    // lint is to ensure that if the type changes out from
                    // underneath, this fails to compile. Since we explicitly
                    // know which types we're casting from, that concern is
                    // moot.
                    #[allow(clippy::cast_lossless)]
                    let value_f = value as f64;
                    let current_mean = self.mean();

                    let index = self
                        .bins
                        .binary_search_by(|bin| bin.range.cmp(&value).reverse())
                        .unwrap(); // The `ensure_finite` call above catches values that don't end up in a bin
                    self.bins[index].count += 1;
                    self.n_samples += 1;
                    self.min = self.min.min(value);
                    self.max = self.max.max(value);

                    // Like in the comment above, some of these casts are
                    // lossless and some are not.
                    #[allow(clippy::cast_lossless)]
                    {
                        self.sum_of_samples = self.sum_of_samples.saturating_add(value as i64);
                    }

                    let delta = value_f - current_mean;
                    let updated_mean = current_mean + delta / (self.n_samples as f64);
                    let delta2 = value_f - updated_mean;
                    self.squared_mean += (delta * delta2);

                    self.p50.append(value)?;
                    self.p90.append(value)?;
                    self.p99.append(value)?;
                    Ok(())
                }
            }
        )+
    }
}

impl_int_sample! { i8, u8, i16, u16, i32, u32, i64, u64 }

macro_rules! impl_float_sample {
    ($($type:ty),+) => {
        $(
            impl Record<$type> for Histogram<$type> where $type: HistogramSupport + Float {
                fn sample(&mut self, value: $type) -> Result<(), HistogramError> {
                    ensure_finite(value)?;

                    if self.n_samples == 0 {
                        self.min = <$type as num::Bounded>::max_value();
                        self.max = <$type as num::Bounded>::min_value();
                    }

                    // For squared mean (M2) calculation, before we update the
                    // count.
                    let value_f: f64 = value.into();
                    let current_mean = self.mean();

                    let index = self
                        .bins
                        .binary_search_by(|bin| bin.range.cmp(&value).reverse())
                        .unwrap(); // The `ensure_finite` call above catches values that don't end up in a bin
                    self.bins[index].count += 1;
                    self.n_samples += 1;

                    if value < self.min {
                        self.min = value;
                    }
                    if value > self.max {
                        self.max = value;
                    }

                    self.sum_of_samples += value_f;

                    let delta = value_f - current_mean;
                    let updated_mean = current_mean + delta / (self.n_samples as f64);
                    let delta2 = value_f - updated_mean;
                    self.squared_mean += (delta * delta2);

                    self.p50.append(value)?;
                    self.p90.append(value)?;
                    self.p99.append(value)?;

                    Ok(())
                }
            }
        )+
    }
}

impl_float_sample! { f32, f64 }

impl<T> Histogram<T>
where
    T: HistogramSupport,
{
    /// Construct a histogram with an explicit set of bins.
    ///
    /// The provided bins should be "back-to-back", so that the right edge of a bin and its
    /// rightward neighbor share a boundary. There should be no gaps and the bins must be strictly
    /// increasing. Only finite values are supported (i.e., not NaN or +/- infinity).
    ///
    /// Note that additional bins on the left and right may be added, to ensure that the bins
    /// extend over the entire support of the histogram.
    ///
    /// Example
    /// -------
    /// ```rust
    /// # // Rename the types crate so the doctests can refer to the public
    /// # // `oximeter` crate, not the private impl.
    /// # use oximeter_types as oximeter;
    /// use oximeter::histogram::Histogram;
    ///
    /// let hist = Histogram::with_bins(&[(0..10).into(), (10..100).into()]).unwrap();
    /// assert_eq!(hist.n_bins(), 4); // Added bins for ..0 on the left and 100.. on the right
    ///
    /// let hist = Histogram::with_bins(&[(..f64::NAN).into()]).is_err(); // No-no
    /// ```
    pub fn with_bins(bins: &[BinRange<T>]) -> Result<Self, HistogramError> {
        let mut bins_ = Vec::with_capacity(bins.len());
        let mut iter = bins.iter();
        let first = bins.first().ok_or(HistogramError::EmptyBins)?;

        let min = <T as Bounded>::min_value();
        if let Bound::Included(start) = first.start_bound() {
            // Prepend a range `MIN..start` if needed
            ensure_finite(*start)?;
            if min < *start {
                bins_.push(Bin {
                    range: BinRange::range(min, *start),
                    count: 0,
                });
            }
        } else if matches!(first.start_bound(), Bound::Unbounded) {
            // A range like `..end` was provided. _Transform_ this into `MIN..end`.
            if let Bound::Excluded(end) = first.end_bound() {
                bins_.push(Bin { range: BinRange::range(min, *end), count: 0 });
                let _ = iter.next().unwrap(); // Remove the transformed bin
            } else {
                unreachable!(
                    "Can't have an bin that is unbounded on both ends"
                );
            }
        }

        // Collect all bins
        bins_.extend(iter.map(|bin| Bin { range: *bin, count: 0 }));

        // Append a range end.. if needed.
        //
        // This seemingly-complicated construction is to avoid triggering the
        // `mutable_borrow_reservation_conflict` lint. See
        // https://github.com/rust-lang/rust/issues/59159 for details.
        let end = if let Bound::Excluded(end) =
            bins_.last().unwrap().range.end_bound()
        {
            if <T as Bounded>::max_value() >= *end {
                Some(Bin { range: BinRange::from(*end), count: 0 })
            } else {
                None
            }
        } else {
            None
        };
        if let Some(end) = end {
            bins_.push(end);
        }

        // Ensure there are no gaps, and each value is comparable
        let n_bins = bins_.len();
        for (first, second) in bins_[..n_bins - 1].iter().zip(&bins_[1..]) {
            if let Bound::Included(start) = first.range.start_bound() {
                ensure_finite(*start)?;
            }
            match (first.range.end_bound(), second.range.start_bound()) {
                (Bound::Excluded(end), Bound::Included(start)) => {
                    ensure_finite(*end).and(ensure_finite(*start))?;
                    if end != start {
                        return Err(HistogramError::NonAdjoiningBins {
                            left: format!("{:?}", first),
                            right: format!("{:?}", second),
                        });
                    }
                }
                _ => unreachable!(
                    "Bin ranges should always be excluded above and included below: {:#?}",
                    (first, second)
                ),
            }
        }
        if let Bound::Excluded(end) = bins_.last().unwrap().range.end_bound() {
            ensure_finite(*end)?;
        }
        Ok(Self {
            start_time: Utc::now(),
            bins: bins_,
            n_samples: 0,
            min: T::zero(),
            max: T::zero(),
            sum_of_samples: T::Width::zero(),
            squared_mean: 0.0,
            p50: Quantile::p50(),
            p90: Quantile::p90(),
            p99: Quantile::p99(),
        })
    }

    /// Construct a new histogram from left bin edges.
    ///
    /// The left edges of the bins must be specified as a non-empty,
    /// monotonically increasing slice. An `Err` is returned if either
    /// constraint is violated.
    pub fn new(left_edges: &[T]) -> Result<Self, HistogramError> {
        let bins = Bins::try_from(left_edges)?;
        Ok(Self {
            start_time: Utc::now(),
            bins: bins.0,
            n_samples: 0,
            min: T::zero(),
            max: T::zero(),
            sum_of_samples: T::Width::zero(),
            squared_mean: 0.0,
            p50: Quantile::p50(),
            p90: Quantile::p90(),
            p99: Quantile::p99(),
        })
    }

    /// Construct a new histogram with the given struct information, including
    /// bins, counts, and quantiles.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        start_time: DateTime<Utc>,
        bins: Vec<T>,
        counts: Vec<u64>,
        min: T,
        max: T,
        sum_of_samples: T::Width,
        squared_mean: f64,
        p50: Quantile,
        p90: Quantile,
        p99: Quantile,
    ) -> Result<Self, HistogramError> {
        if bins.len() != counts.len() {
            return Err(HistogramError::ArraySizeMismatch {
                n_bins: bins.len(),
                n_counts: counts.len(),
            });
        }

        let mut bins = Bins::try_from(bins.as_slice())?.0;
        let mut n_samples = 0;
        for (bin, count) in bins.iter_mut().zip(counts.into_iter()) {
            bin.count = count;
            n_samples += count;
        }

        Ok(Self {
            start_time,
            bins,
            n_samples,
            min,
            max,
            sum_of_samples,
            squared_mean,
            p50,
            p90,
            p99,
        })
    }

    /// Return the total number of samples contained in the histogram.
    pub fn n_samples(&self) -> u64 {
        self.n_samples
    }

    /// Return the number of bins in the histogram.
    pub fn n_bins(&self) -> usize {
        self.bins.len()
    }

    /// Return the bins of the histogram.
    pub fn bins_and_counts(&self) -> (Vec<T>, Vec<u64>) {
        let mut bins = Vec::with_capacity(self.n_bins());
        let mut counts = Vec::with_capacity(self.n_bins());
        for bin in self.bins.iter() {
            match bin.range {
                BinRange::Range { start, .. } => {
                    bins.push(start);
                }
                BinRange::RangeFrom { start } => {
                    bins.push(start);
                }
                _ => unreachable!(
                    "No bins in a constructed histogram should be of type RangeTo"
                ),
            }
            counts.push(bin.count);
        }
        (bins, counts)
    }

    /// Return the minimum value of inputs to the histogram.
    pub fn min(&self) -> T {
        self.min
    }

    /// Return the maximum value of all inputs to the histogram.
    pub fn max(&self) -> T {
        self.max
    }

    /// Return the sum of all inputs to the histogram.
    pub fn sum_of_samples(&self) -> T::Width {
        self.sum_of_samples
    }

    /// Return the squared mean (M2) of all inputs to the histogram.
    pub fn squared_mean(&self) -> f64 {
        self.squared_mean
    }

    /// Return the mean of all inputs/samples in the histogram.
    pub fn mean(&self) -> f64 {
        if self.n_samples() > 0 {
            self.sum_of_samples
                .to_f64()
                .map(|sum| sum / (self.n_samples() as f64))
                .unwrap()
        } else {
            0.
        }
    }

    /// Return the variance for inputs to the histogram based on the Welford's
    /// algorithm, using the squared mean (M2).
    ///
    /// Returns `None` if there are fewer than two samples.
    pub fn variance(&self) -> Option<f64> {
        (self.n_samples() > 1)
            .then(|| self.squared_mean / (self.n_samples() as f64))
    }

    /// Return the sample variance for inputs to the histogram based on the
    /// Welford's algorithm, using the squared mean (M2).
    ///
    /// Returns `None` if there are fewer than two samples.
    pub fn sample_variance(&self) -> Option<f64> {
        (self.n_samples() > 1)
            .then(|| self.squared_mean / ((self.n_samples() - 1) as f64))
    }

    /// Return the standard deviation for inputs to the histogram.
    ///
    /// This is a biased (as a consequence of Jensen’s inequality), estimate of
    /// the population deviation that returns the standard deviation of the
    /// samples seen by the histogram.
    ///
    /// Returns `None` if the variance is `None`, i.e., if there are fewer than
    /// two samples.
    pub fn std_dev(&self) -> Option<f64> {
        match self.variance() {
            Some(variance) => Some(variance.sqrt()),
            None => None,
        }
    }

    /// Return the "corrected" sample standard deviation for inputs to the
    /// histogram.
    ///
    /// This is an unbiased estimate of the population deviation, applying
    /// Bessel's correction, which corrects the bias in the estimation of the
    /// population variance, and some, but not all of the bias in the estimation
    /// of the population standard deviation.
    ///
    /// Returns `None` if the variance is `None`, i.e., if there are fewer than
    /// two samples.
    pub fn sample_std_dev(&self) -> Option<f64> {
        match self.sample_variance() {
            Some(variance) => Some(variance.sqrt()),
            None => None,
        }
    }

    /// Iterate over the bins of the histogram.
    pub fn iter(&self) -> impl Iterator<Item = &Bin<T>> {
        self.bins.iter()
    }

    /// Get the bin at the given index.
    pub fn get(&self, index: usize) -> Option<&Bin<T>> {
        self.bins.get(index)
    }

    /// Return the start time for this histogram.
    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// Set the start time for this histogram.
    pub fn set_start_time(&mut self, start_time: DateTime<Utc>) {
        self.start_time = start_time;
    }

    /// Return the p50 quantile for the histogram.
    pub fn p50q(&self) -> Quantile {
        self.p50
    }

    /// Return the p90 quantile for the histogram.
    pub fn p90q(&self) -> Quantile {
        self.p90
    }

    /// Return the p99 quantile for the histogram.
    pub fn p99q(&self) -> Quantile {
        self.p99
    }

    /// Return the p50 estimate for the histogram.
    pub fn p50(&self) -> Result<f64, QuantileError> {
        self.p50.estimate()
    }

    /// Return the p90 estimate for the histogram.
    pub fn p90(&self) -> Result<f64, QuantileError> {
        self.p90.estimate()
    }

    /// Return the p99 estimate for the histogram.
    pub fn p99(&self) -> Result<f64, QuantileError> {
        self.p99.estimate()
    }
}

impl<T> TryFrom<&[T]> for Bins<T>
where
    T: HistogramSupport,
{
    type Error = HistogramError;

    fn try_from(left_edges: &[T]) -> Result<Self, Self::Error> {
        let mut items = left_edges.iter();
        let mut bins: Vec<Bin<T>> = Vec::with_capacity(left_edges.len() + 1);
        let mut current: T = *items.next().ok_or(HistogramError::EmptyBins)?;
        ensure_finite(current)?;
        let min: T = <T as Bounded>::min_value();
        if current > min {
            // Bin greater than the minimum was specified, insert a new one from `MIN..current`.
            bins.push(Bin { range: BinRange::range(min, current), count: 0 });
        } else if current == min {
            // An edge *at* the minimum was specified. Consume it, and insert a bin from
            // `MIN..next`, if one exists. If one does not, or if this is the last item, the
            // following loop will not be entered.
            let next: T =
                items.next().cloned().unwrap_or_else(<T as Bounded>::max_value);
            bins.push(Bin { range: BinRange::range(min, next), count: 0 });
            current = next;
        }
        for &next in items {
            if current < next {
                ensure_finite(next)?;
                bins.push(Bin {
                    range: BinRange::range(current, next),
                    count: 0,
                });
                current = next;
            } else if current >= next {
                return Err(HistogramError::NonmonotonicBins);
            } else {
                return Err(HistogramError::NonFiniteValue);
            }
        }
        if current < <T as Bounded>::max_value() {
            bins.push(Bin { range: BinRange::from(current), count: 0 });
        }

        Ok(Bins(bins))
    }
}

impl<T> Histogram<T>
where
    T: HistogramSupport,
    u16: LogLinearBins<T, T::Power>,
{
    /// Generate a histogram with 9 linearly-spaced bins, per power of 10.
    ///
    /// This generates a "log-linear" histogram. Within each power of 10, the
    /// bins of the histogram are linearly spaced. Note that additional bins on
    /// the left will be added, as described in [`Histogram::new()`].
    ///
    /// Notes
    /// -----
    ///
    /// Why 9 bins? Most users intuitively want the bins in each power of ten to
    /// have a specific width: the power of 10 itself. For example, consider the
    /// bins between 10 and 100. It is often desirable to have bins placed at 10,
    /// 20, 30, ..., 90, 100. Since 100 itself does _not_ fall within the 1st
    /// decade of 10, i.e., the range `[10, 100)`, this means there are exactly
    /// 9 bins within the range.
    ///
    /// This is much more easily understood compared to the actual edges we get
    /// with 10 bins, which is the sequence `10, 19, 28, ...`. If one wants
    /// exactly the requested number of bins (assuming it's possible), use
    /// [`Histogram::with_log_linear_bins()`].
    ///
    /// Example
    /// -------
    ///
    /// ```rust
    /// # // Rename the types crate so the doctests can refer to the public
    /// # // `oximeter` crate, not the private impl.
    /// # use oximeter_types as oximeter;
    /// use oximeter::histogram::{Histogram, BinRange};
    /// use std::ops::{RangeBounds, Bound};
    ///
    /// let hist: Histogram<f64> = Histogram::span_decades(-1, 1).unwrap();
    /// let bins = hist.iter().collect::<Vec<_>>();
    ///
    /// // There are 9 bins per power of 10, plus 1 additional for everything
    /// // below and above the power of 10.
    /// assert_eq!(bins.len(), 2 * 9 + 2);
    ///
    /// // First bin is from the left support edge to the first bin
    /// assert_eq!(bins[0].range.end_bound(), Bound::Excluded(&0.1));
    ///
    /// // First decade of bins is `[0.1, 0.2, ...)`.
    /// assert_eq!(bins[1].range, BinRange::range(0.1, 0.2));
    ///
    /// // Note that these are floats, which are notoriously difficult to
    /// // compare. The bin edges are not _exact_, but quite close.
    /// let BinRange::Range { start, end } = bins[2].range else { unreachable!() };
    /// let BinRange::Range {
    ///     start: expected_start,
    ///     end: expected_end,
    /// } = BinRange::range(0.2, 0.3) else { unreachable!() };
    /// assert_eq!(start, expected_start);
    /// approx::assert_ulps_eq!(end, expected_end);
    ///
    /// // Second decade is `[1.0, 2.0, 3.0, ...]`
    /// assert_eq!(bins[9].range, BinRange::range(0.9, 1.0));
    /// assert_eq!(bins[10].range, BinRange::range(1.0, 2.0));
    /// assert_eq!(bins[11].range, BinRange::range(2.0, 3.0));
    ///
    /// // Ends at the third decade, so the last bin is the remainder of the support
    /// assert_eq!(bins[19].range, BinRange::from(10.0));
    /// ```
    pub fn span_decades(
        start_decade: T::Power,
        stop_decade: T::Power,
    ) -> Result<Self, HistogramError> {
        Self::with_log_linear_bins(
            10,
            start_decade,
            stop_decade,
            9.try_into().unwrap(),
        )
    }

    /// Generate a histogram with evenly-spaced bin in each power of a base.
    ///
    /// This results in a histogram with `n_bins` bins in each decade over the
    /// range `[base ** start_decade, base ** stop_decade)`. There are two
    /// additional bins, for the values entirely below `base ** start_decade`
    /// and >= `base ** stop_decade`.
    pub fn with_log_linear_bins(
        base: u16,
        start_decade: T::Power,
        stop_decade: T::Power,
        n_bins: NonZeroUsize,
    ) -> Result<Self, HistogramError> {
        let bins = base.bins(start_decade, stop_decade, n_bins)?;
        Histogram::new(&bins)
    }
}

/// A trait for generating linearly-spaced bins over a set of powers.
pub trait LogLinearBins<T: HistogramSupport, Base>:
    ToPrimitive + FromPrimitive + Num
{
    /// Compute the left bin edges for a histogram with `count` bins over each
    /// power of the base.
    fn bins(
        &self,
        lo: Base,
        hi: Base,
        count: NonZeroUsize,
    ) -> Result<Vec<T>, QuantizationError>;
}

impl<T> LogLinearBins<T, u16> for u16
where
    T: HistogramSupport + Integer,
{
    fn bins(
        &self,
        lo: u16,
        hi: u16,
        count: NonZeroUsize,
    ) -> Result<Vec<T>, QuantizationError> {
        // Basic sanity checks
        if *self == 0 || *self > 32 {
            return Err(QuantizationError::InvalidBase);
        }
        if count.get() < 2 {
            return Err(QuantizationError::InvalidSteps);
        }
        if lo >= hi {
            return Err(QuantizationError::PowersOutOfOrder);
        }

        // The base must be <= the number of steps + 1. The one is because we're
        // computing left bin edges.
        if <Self as Into<usize>>::into(*self) > count.get() + 1 {
            return Err(QuantizationError::InvalidSteps);
        }

        // The highest power must be representable in the target type. Note that
        // we have to convert to that target type _before_ doing this check.
        let base = <u64 as From<Self>>::from(*self);
        let Some(highest) = base.checked_pow(hi.into()) else {
            return Err(QuantizationError::Overflow);
        };
        if <T as NumCast>::from(highest).is_none() {
            return Err(QuantizationError::Overflow);
        }

        // Convert everything into wide integers for easy computations that
        // won't overflow during interim processing.
        //
        // Note that we unwrap in a few places below, where we're sure the
        // narrowing conversion cannot fail, such as to a u32.
        let lo = <u64 as From<Self>>::from(lo);
        let hi = <u64 as From<Self>>::from(hi);
        let count = <u64 as NumCast>::from(count.get())
            .ok_or(QuantizationError::Overflow)?;

        fn bin_count_divides_spacing(
            base: u64,
            lo: u64,
            hi: u64,
            count: u64,
        ) -> bool {
            let powers = lo..hi;
            let next_powers = lo + 1..hi + 1;
            powers.zip(next_powers).all(|(lo, hi)| {
                let lo = base.pow(lo as _);
                let hi = base.pow(hi as _);
                let distance = hi - lo;
                Integer::is_multiple_of(&distance, &count)
            })
        }

        if !bin_count_divides_spacing(base, lo, hi, count) {
            return Err(QuantizationError::UnevenStepsForBase);
        }

        // Compute the next step size.
        fn next_step(next: u64, count: u64) -> Result<u64, QuantizationError> {
            if next > count {
                next.checked_div(count).ok_or(QuantizationError::Precision)
            } else {
                Ok(1)
            }
        }

        let mut out = Vec::with_capacity(
            count
                .checked_mul(hi - lo)
                .ok_or(QuantizationError::Overflow)?
                .try_into()
                .unwrap(),
        );
        let powers = lo..hi;
        let mut power = lo;
        let mut value = base
            .checked_pow(lo.try_into().unwrap())
            .ok_or(QuantizationError::Overflow)?;
        let mut next_start = base
            .checked_pow((lo + 1).try_into().unwrap())
            .ok_or(QuantizationError::Overflow)?;
        let mut step = next_step(next_start - value, count)?;
        while powers.contains(&power) {
            out.push(
                <T as NumCast>::from(value)
                    .ok_or(QuantizationError::Overflow)?,
            );
            if value < next_start {
                value = value
                    .checked_add(step)
                    .ok_or(QuantizationError::Overflow)?;
                continue;
            }
            next_start = next_start
                .checked_mul(base)
                .ok_or(QuantizationError::Overflow)?;
            power = power.checked_add(1).ok_or(QuantizationError::Overflow)?;
            step = next_step(next_start - value, count)?;
            value =
                value.checked_add(step).ok_or(QuantizationError::Overflow)?;
        }
        Ok(out)
    }
}

impl<T> LogLinearBins<T, i16> for u16
where
    T: HistogramSupport + Float,
{
    fn bins(
        &self,
        lo: i16,
        hi: i16,
        count: NonZeroUsize,
    ) -> Result<Vec<T>, QuantizationError> {
        // Basic sanity checks.
        //
        // Note that for floating point, we are significantly less constrained
        // in terms of the relationship between the base and the count. For
        // integers, we ensure that they're relatively co-divisible, so that we
        // are not losing precision by computing the steps. Floats are more
        // permissive.
        if *self == 0 || *self > 32 {
            return Err(QuantizationError::InvalidBase);
        }
        if count.get() < 2 {
            return Err(QuantizationError::InvalidSteps);
        }
        if lo >= hi {
            return Err(QuantizationError::PowersOutOfOrder);
        }

        // Compute the next step size.
        fn next_step(next: f64, count: u64) -> Result<f64, QuantizationError> {
            let count_ = <f64 as NumCast>::from(count)
                .ok_or(QuantizationError::Precision)?;
            Ok(next / count_)
        }

        let count = <u64 as NumCast>::from(count.get())
            .ok_or(QuantizationError::Overflow)?;
        let base = <f64 as NumCast>::from(*self).unwrap();
        let n_elems = count
            .checked_mul(
                <u64 as NumCast>::from(hi - lo)
                    .ok_or(QuantizationError::Overflow)?,
            )
            .ok_or(QuantizationError::Overflow)?
            .try_into()
            .unwrap();
        let mut out = Vec::with_capacity(n_elems);
        let powers = lo..hi;

        let mut power = lo;
        let mut start = base.powi(lo.into());
        let mut stop = base.powi((lo + 1).into());
        let mut step = next_step(stop - start, count)?;
        while powers.contains(&power) {
            for i in 0..count {
                let value = start + step * <f64 as NumCast>::from(i).unwrap();
                out.push(
                    <T as NumCast>::from(value)
                        .ok_or(QuantizationError::Precision)?,
                );
            }

            // Move to next power of the base.
            start = stop;
            stop *= base;
            step = next_step(stop - start, count)?;
            power += 1;
        }
        out.push(
            <T as NumCast>::from(start).ok_or(QuantizationError::Overflow)?,
        );
        Ok(out)
    }
}

pub trait Bits: Integer {
    const BITS: u32;
    fn next_power(self) -> Option<Self>;
}

macro_rules! impl_bits {
    ($type_:ty) => {
        impl Bits for $type_ {
            const BITS: u32 = Self::BITS;

            fn next_power(self) -> Option<Self> {
                self.checked_mul(2)
            }
        }
    };
}

impl_bits!(u8);
impl_bits!(u16);
impl_bits!(u32);
impl_bits!(u64);

impl<T> Histogram<T>
where
    T: Bits + HistogramSupport,
{
    /// Create a histogram with logarithmically spaced bins at each power of 2.
    ///
    /// This is only available for unsigned integer support types.
    pub fn power_of_two() -> Self {
        let mut bins = Vec::with_capacity(T::BITS as _);
        let mut x = T::one();
        bins.push(x);
        while let Some(next) = x.next_power() {
            bins.push(next);
            x = next;
        }
        Self::new(&bins).expect("Bits is statically known")
    }
}

// Helper to ensure all values are comparable, i.e., not NaN.
fn ensure_finite<T>(value: T) -> Result<(), HistogramError>
where
    T: HistogramSupport,
{
    if value.is_finite() { Ok(()) } else { Err(HistogramError::NonFiniteValue) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn test_ensure_finite() {
        assert!(ensure_finite(0i64).is_ok());
        assert!(ensure_finite(i64::MIN).is_ok());
        assert!(ensure_finite(i64::MAX).is_ok());

        assert!(ensure_finite(0u64).is_ok());
        assert!(ensure_finite(u64::MIN).is_ok());
        assert!(ensure_finite(u64::MAX).is_ok());

        assert!(ensure_finite(0.0).is_ok());
        assert!(ensure_finite(f32::NEG_INFINITY).is_err());
        assert!(ensure_finite(f32::INFINITY).is_err());
        assert!(ensure_finite(f32::NAN).is_err());

        assert!(ensure_finite(0.0).is_ok());
        assert!(ensure_finite(f64::NEG_INFINITY).is_err());
        assert!(ensure_finite(f64::INFINITY).is_err());
        assert!(ensure_finite(f64::NAN).is_err());
    }

    #[test]
    fn test_bin_range_to() {
        let range = BinRange::to(10_u64);
        assert!(!range.contains(&100));
        assert!(range.contains(&0));
        assert_eq!(range.cmp(&0), Ordering::Equal);
        assert_eq!(range.cmp(&10), Ordering::Greater);
    }

    #[test]
    fn test_bin_range_from() {
        let range = BinRange::from(10_u64);
        assert!(range.contains(&100));
        assert!(!range.contains(&0));
        assert_eq!(range.cmp(&0), Ordering::Less);
        assert_eq!(range.cmp(&10), Ordering::Equal);
    }

    #[test]
    fn test_bin_range() {
        let range = BinRange::range(0_u64, 10);
        assert!(!range.contains(&100));
        assert!(range.contains(&0));
        assert!(!range.contains(&10));
        assert_eq!(range.cmp(&0), Ordering::Equal);
        assert_eq!(range.cmp(&10), Ordering::Greater);
    }

    #[test]
    fn test_histogram() {
        let mut hist = Histogram::new(&[0, 10, 20]).unwrap();
        assert_eq!(
            hist.n_bins(),
            4,
            "Histogram should have 1 more bin than bin edges specified"
        );
        assert_eq!(hist.n_samples(), 0, "Histogram should init with 0 samples");
        let max_sample = 100;
        let min_sample = -10i64;
        let samples = [min_sample, 0, 1, 10, max_sample];
        let expected_counts = [1u64, 2, 1, 1];
        for (i, sample) in samples.iter().enumerate() {
            hist.sample(*sample).unwrap();
            let count = i as u64 + 1;
            let current_sum = samples[..=i].iter().sum::<i64>() as f64;
            let current_mean = current_sum / count as f64;
            let current_std_dev = (samples[..=i]
                .iter()
                .map(|x| (*x as f64 - current_mean).powi(2))
                .sum::<f64>()
                / count as f64)
                .sqrt();
            let current_sample_std_dev = (samples[..=i]
                .iter()
                .map(|x| (*x as f64 - current_mean).powi(2))
                .sum::<f64>()
                / (count - 1) as f64)
                .sqrt();
            assert_eq!(
                hist.n_samples(),
                count,
                "Histogram should have {} sample(s)",
                count
            );

            if count > 0 {
                assert_eq!(
                    hist.mean(),
                    current_mean,
                    "Histogram should have a mean of {}",
                    current_mean
                );
            } else {
                assert!(hist.mean().is_zero());
            }

            if count > 1 {
                assert_eq!(
                    hist.std_dev().unwrap(),
                    current_std_dev,
                    "Histogram should have a sample standard deviation of {}",
                    current_std_dev
                );
                assert_eq!(
                    hist.sample_std_dev().unwrap(),
                    current_sample_std_dev,
                    "Histogram should have a sample standard deviation of {}",
                    current_sample_std_dev
                );
            } else {
                assert!(hist.std_dev().is_none());
                assert!(hist.sample_std_dev().is_none());
            }
        }

        assert_eq!(
            hist.min(),
            min_sample,
            "Histogram should have a minimum value of {}",
            min_sample
        );
        assert_eq!(
            hist.max(),
            max_sample,
            "Histogram should have a maximum value of {}",
            max_sample
        );

        for (bin, &expected_count) in hist.iter().zip(expected_counts.iter()) {
            assert_eq!(
                bin.count, expected_count,
                "Bin {:?} expected to have {} items, but found {}",
                bin.range, expected_count, bin.count
            );
        }

        let p50 = hist.p50().unwrap();
        assert_eq!(p50, 1.0, "P50 should be 1.0, but found {}", p50);

        let p90 = hist.p90().unwrap();
        assert_eq!(p90, 100.0, "P90 should be 100.0, but found {}", p90);

        let p99 = hist.p99().unwrap();
        assert_eq!(p99, 100.0, "P99 should be 100.0, but found {}", p99);
    }

    #[test]
    fn test_histogram_with_bins() {
        let bins = &[(..0).into(), (0..10).into()];
        let hist = Histogram::with_bins(bins).unwrap();
        assert_eq!(hist.n_bins(), 3);
        let data = hist.iter().collect::<Vec<_>>();
        assert_eq!(data[0].range, BinRange::range(i64::MIN, 0));
        assert_eq!(data[1].range, BinRange::range(0, 10));
        assert_eq!(data[2].range, BinRange::from(10));
    }

    #[test]
    fn test_histogram_construct_with() {
        let mut hist = Histogram::new(&[0, 10, 20]).unwrap();
        hist.sample(1).unwrap();
        hist.sample(11).unwrap();

        let (bins, counts) = hist.bins_and_counts();
        assert_eq!(
            bins.len(),
            counts.len(),
            "Bins and counts should have the same size"
        );
        assert_eq!(
            bins.len(),
            hist.n_bins(),
            "Paired-array bins should be of the same length as the histogram"
        );
        assert_eq!(counts, &[0, 1, 1, 0], "Paired-array counts are incorrect");
        assert_eq!(hist.n_samples(), 2);

        let rebuilt = Histogram::from_parts(
            hist.start_time(),
            bins,
            counts,
            hist.min(),
            hist.max(),
            hist.sum_of_samples(),
            hist.squared_mean(),
            hist.p50,
            hist.p90,
            hist.p99,
        )
        .unwrap();
        assert_eq!(
            hist, rebuilt,
            "Histogram reconstructed from paired arrays is not correct"
        );
    }

    #[test]
    fn test_histogram_with_overlapping_bins() {
        let bins = &[(..1_u64).into(), (0..10).into()];
        assert!(Histogram::with_bins(bins).is_err());
    }

    #[test]
    fn test_histogram_with_non_partitioned_bins() {
        let bins = &[(..0).into(), (1..10).into()];
        assert!(
            Histogram::with_bins(bins).is_err(),
            "Bins with gaps should trigger an error"
        );
    }

    #[test]
    fn test_histogram_float_bins() {
        let bins = &[(..0.0).into(), (0.0..10.0).into()];
        assert!(Histogram::with_bins(bins).is_ok());
    }

    #[test]
    fn test_histogram_extreme_samples() {
        let mut hist = Histogram::with_bins(&[(0..1).into()]).unwrap();
        assert!(hist.sample(i64::MIN).is_ok());
        assert!(hist.sample(i64::MAX).is_ok());
        assert_eq!(hist.get(0).unwrap().count, 1);
        assert_eq!(hist.get(1).unwrap().count, 0);
        assert_eq!(hist.get(2).unwrap().count, 1);

        let mut hist = Histogram::with_bins(&[(0.0..1.0).into()]).unwrap();
        assert!(hist.sample(f64::MIN).is_ok());
        assert!(hist.sample(f64::INFINITY).is_err());
        assert!(hist.sample(f64::NAN).is_err());
    }

    #[test]
    fn test_histogram_extreme_bins() {
        let hist = Histogram::with_bins(&[(i64::MIN..).into()]).unwrap();
        assert_eq!(
            hist.n_bins(),
            1,
            "This histogram should have one bin, which covers the whole range"
        );

        assert!(Histogram::with_bins(&[(f64::NEG_INFINITY..).into()]).is_err());
        assert!(Histogram::with_bins(&[(..f64::INFINITY).into()]).is_err());
        let hist = Histogram::with_bins(&[(f64::MIN..).into()]).unwrap();
        assert_eq!(
            hist.n_bins(),
            1,
            "This histogram should have one bin, which covers the whole range"
        );
        let hist = Histogram::with_bins(&[(..f64::MAX).into()]).unwrap();
        assert_eq!(
            hist.n_bins(),
            2,
            "This histogram should have two bins, since `BinRange`s are always exclusive on the right"
        );
        assert!(Histogram::with_bins(&[(f64::NAN..).into()]).is_err());
        assert!(Histogram::with_bins(&[(..f64::NAN).into()]).is_err());
        assert!(
            Histogram::with_bins(&[
                (0.0..f64::NAN).into(),
                (f64::NAN..100.0).into()
            ])
            .is_err()
        );
        assert!(Histogram::new(&[f64::NAN, 0.0]).is_err());

        let hist = Histogram::new(&[i64::MIN]).unwrap();
        assert_eq!(
            hist.bins[0].range,
            BinRange::range(i64::MIN, i64::MAX),
            "A single bin at i64::MIN should be turned into a single bin [i64::MIN, i64::MAX]"
        );
        let hist = Histogram::new(&[i64::MIN, 0]).unwrap();
        assert_eq!(hist.bins[0].range, BinRange::range(i64::MIN, 0));

        let hist = Histogram::new(&[f64::MIN]).unwrap();
        assert_eq!(
            hist.bins[0].range,
            BinRange::range(f64::MIN, f64::MAX),
            "A single bin at f64::MIN should be turned into a single bin [MIN, MAX)"
        );
    }

    #[test]
    fn test_histogram_unsorted_bins() {
        assert!(
            Histogram::new(&[0, -10, 1]).is_err(),
            "Expected an Err when building a histogram with unsorted bins"
        );

        assert!(
            Histogram::with_bins(&[(0..1).into(), (-1..0).into()]).is_err(),
            "Expected an Err when building a histogram with unsorted bins"
        );
    }

    #[test]
    fn test_histogram_unbounded_samples() {
        let mut hist = Histogram::new(&[0.0, 1.0]).unwrap();
        assert!(
            hist.sample(f64::NAN).is_err(),
            "Expected an Err when sampling NaN into a histogram"
        );
        assert!(
            hist.sample(f64::NEG_INFINITY).is_err(),
            "Expected an Err when sampling negative infinity into a histogram"
        );
    }

    #[test]
    fn test_span_decades() {
        let hist = Histogram::<f64>::span_decades(0, 3).unwrap();
        println!("{:#?}", hist.bins);
        // Total number of bins is:
        //
        // 1        -- for bin from (MIN, 1)
        // 9 * 3   -- for each power of 10 in [10 ** 0, 10 ** 3)
        // 1        -- for [10 ** 3, MAX)
        //
        // = 29;
        assert_eq!(hist.n_bins(), 29);
    }

    #[test]
    fn test_span_decades_other_counts_f64() {
        const N_BINS: usize = 20;
        let hist = Histogram::<f64>::with_log_linear_bins(
            10,
            0,
            1,
            N_BINS.try_into().unwrap(),
        )
        .unwrap();
        // Total number of bins is:
        //
        // 1        -- for [MIN, 0)
        // N_BINS   -- for [10 ** 0, 10 ** 1)
        // 1        -- for [10**1, MAX)
        println!("{:#?}", hist.bins);
        assert_eq!(hist.n_bins(), N_BINS + 2);
    }

    #[test]
    fn test_span_decades_other_counts_u64_resolution_too_low() {
        let err = Histogram::<u64>::with_log_linear_bins(
            10,
            0,
            1,
            20.try_into().unwrap(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            HistogramError::Quantization(QuantizationError::UnevenStepsForBase)
        ));
    }

    #[test]
    fn test_span_decades_other_counts_u64_resolution_ok() {
        const N_BINS: usize = 30;
        let hist = Histogram::<u64>::with_log_linear_bins(
            10,
            1,
            2,
            N_BINS.try_into().unwrap(),
        )
        .unwrap();
        // Total number of bins is:
        // 1        -- for [0, 1)
        // N_BINS   -- for each power of ten in [1, 2)
        // 1        -- for the last left edge
        println!("{:#?}", hist.bins);
        assert_eq!(hist.n_bins(), N_BINS + 2);
    }

    // Sanity check that we compute exactly the expected bins for an easy case,
    // where any output type can represent the exact set of bins.
    #[test]
    fn test_log_linear_bins_all_representable() {
        const EXPECTED: &[u8] = &[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
        ];
        let base = 10_u16;
        let lo = 0;
        let hi = 2;
        let n_bins = NonZeroUsize::new(9).unwrap();

        let bins: Vec<u8> = base.bins(lo, hi, n_bins).unwrap();
        assert_eq!(bins, EXPECTED);

        fn cmp<T>(bins: Vec<T>, expected: &[u8])
        where
            T: TryFrom<u8> + std::fmt::Debug + std::cmp::PartialEq,
            <T as TryFrom<u8>>::Error: std::fmt::Debug,
        {
            assert_eq!(
                bins,
                expected
                    .iter()
                    .copied()
                    .map(|x| T::try_from(x).unwrap())
                    .collect::<Vec<_>>()
            );
        }

        let bins: Vec<i8> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);
        let bins: Vec<u16> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);
        let bins: Vec<i16> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);
        let bins: Vec<u32> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);
        let bins: Vec<i32> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);
        let bins: Vec<u64> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);
        let bins: Vec<i64> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);

        let base = 10_u16;
        let lo = 0_i16;
        let hi = 2;
        let n_bins = NonZeroUsize::new(9).unwrap();
        let bins: Vec<f32> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);
        let bins: Vec<f64> = base.bins(lo, hi, n_bins).unwrap();
        cmp(bins, EXPECTED);
    }

    #[test]
    fn test_log_linear_bins_integer_size_checks() {
        // Number of steps must be >= 2.
        let base = 10_u16;
        let res: Result<Vec<u64>, _> =
            base.bins(0_u16, 2, 1.try_into().unwrap());
        assert!(matches!(res.unwrap_err(), QuantizationError::InvalidSteps));

        // 10 ** 100 overflows a u64.
        let res: Result<Vec<u64>, _> =
            base.bins(2_u16, 100, 10.try_into().unwrap());
        assert!(matches!(res.unwrap_err(), QuantizationError::Overflow));

        // 100 bins can't evenly divide the provided base.
        let res: Result<Vec<u64>, _> = base.bins(0, 1, 100.try_into().unwrap());
        assert!(matches!(
            res.unwrap_err(),
            QuantizationError::UnevenStepsForBase
        ));

        // Base is larger than the number of steps
        let res: Result<Vec<u64>, _> = base.bins(0, 1, 5.try_into().unwrap());
        assert!(matches!(res.unwrap_err(), QuantizationError::InvalidSteps));
    }

    #[test]
    fn test_log_linear_bins_small_bin_count() {
        let base = 10_u16;
        let _: Vec<u64> = base
            .bins(3, 4, 20.try_into().unwrap())
            .expect("Should be able to compute widely spaced bins");
    }

    // These are explicit tests against NumPy's linspace implementation, which
    // we're trying to emulate. Specifically, assuming NumPy is installed, the
    // following code will generate these values:
    //
    // ```python
    // def space(base: int, lo: int, hi: int, count: int) -> np.ndarray:
    //      parts = np.concatenate([
    //          np.linspace(base ** b, base ** (b + 1), count, endpoint=False)
    //          for b in range(lo, hi)
    //      ], axis=0)
    //      return np.append(parts, np.atleast_1d(base ** hi))
    // ```
    #[rstest::rstest]
    #[case(
        2,
        -3,
        0,
        7,
        &[
            0.125     , 0.14285714, 0.16071429, 0.17857143, 0.19642857,
            0.21428571, 0.23214286, 0.25      , 0.28571429, 0.32142857,
            0.35714286, 0.39285714, 0.42857143, 0.46428571, 0.5       ,
            0.57142857, 0.64285714, 0.71428571, 0.78571429, 0.85714286,
            0.92857143, 1.
        ]
    )]
    #[case(
        10,
        -1,
        3,
        15,
        &[
            1.0e-01, 1.6e-01, 2.2e-01, 2.8e-01, 3.4e-01, 4.0e-01, 4.6e-01,
            5.2e-01, 5.8e-01, 6.4e-01, 7.0e-01, 7.6e-01, 8.2e-01, 8.8e-01,
            9.4e-01, 1.0e+00, 1.6e+00, 2.2e+00, 2.8e+00, 3.4e+00, 4.0e+00,
            4.6e+00, 5.2e+00, 5.8e+00, 6.4e+00, 7.0e+00, 7.6e+00, 8.2e+00,
            8.8e+00, 9.4e+00, 1.0e+01, 1.6e+01, 2.2e+01, 2.8e+01, 3.4e+01,
            4.0e+01, 4.6e+01, 5.2e+01, 5.8e+01, 6.4e+01, 7.0e+01, 7.6e+01,
            8.2e+01, 8.8e+01, 9.4e+01, 1.0e+02, 1.6e+02, 2.2e+02, 2.8e+02,
            3.4e+02, 4.0e+02, 4.6e+02, 5.2e+02, 5.8e+02, 6.4e+02, 7.0e+02,
            7.6e+02, 8.2e+02, 8.8e+02, 9.4e+02, 1.0e+03
        ]
    )]
    #[case(
        10,
        -12,
        -10,
        10,
        &[
            1.0e-12, 1.9e-12, 2.8e-12, 3.7e-12, 4.6e-12, 5.5e-12, 6.4e-12,
            7.3e-12, 8.2e-12, 9.1e-12, 1.0e-11, 1.9e-11, 2.8e-11, 3.7e-11,
            4.6e-11, 5.5e-11, 6.4e-11, 7.3e-11, 8.2e-11, 9.1e-11, 1.0e-10
        ],
    )]
    #[case(
        10,
        10,
        12,
        10,
        &[
            1.0e+10, 1.9e+10, 2.8e+10, 3.7e+10, 4.6e+10, 5.5e+10, 6.4e+10,
            7.3e+10, 8.2e+10, 9.1e+10, 1.0e+11, 1.9e+11, 2.8e+11, 3.7e+11,
            4.6e+11, 5.5e+11, 6.4e+11, 7.3e+11, 8.2e+11, 9.1e+11, 1.0e+12
        ]
    )]
    fn test_log_linear_bins_f64_matches_reference_implementation(
        #[case] base: u16,
        #[case] lo: i16,
        #[case] hi: i16,
        #[case] count: usize,
        #[case] expected: &[f64],
    ) {
        let bins: Vec<f64> =
            base.bins(lo, hi, count.try_into().unwrap()).unwrap();
        println!("{bins:#?}");
        println!("{expected:#?}");
        assert!(
            all_close(&bins, expected, 1e-8, 1e-5),
            "Linspaced bins don't match reference implementation"
        );
    }

    fn all_close<T>(a: &[T], b: &[T], atol: T, rtol: T) -> bool
    where
        T: Float,
    {
        if a.len() != b.len() {
            return false;
        }
        a.iter()
            .zip(b.iter())
            .all(|(a, b)| (*a - *b).abs() <= (atol + rtol * b.abs()))
    }

    #[test]
    fn test_empty_bins_not_supported() {
        assert!(matches!(
            Histogram::<u64>::new(&[]).unwrap_err(),
            HistogramError::EmptyBins
        ));
    }

    #[test]
    fn test_log_linear_bins_does_not_overflow_wide_bin_type() {
        let start: u16 = 3;
        // 10u16 ** 10u16 overflows, but what we should be computing is 10u64 **
        // 10u16, which would not overflow. We need to compute whether it
        // overflows in the _support_ type.
        let stop = 10;
        Histogram::<u64>::span_decades(start, stop).expect(
            "expected not to overflow, since support type is wide enough",
        );
    }

    #[test]
    fn test_log_linear_bins_does_overflow_narrow_bin_type() {
        // In this case, the start / stop powers _and_ their resulting bins are
        // both representable as u16s and also u64s. But we're generating bins
        // that are u8s, which _the powers do_ overflow.
        let start: u16 = 1;
        let stop: u16 = 4;
        Histogram::<u32>::span_decades(start, stop).expect(
            "expected not to overflow a u32, since support type is wide enough",
        );
        Histogram::<u8>::span_decades(start, stop).expect_err(
            "expected to overflow a u8, since support type is not wide enough",
        );
    }

    #[test]
    fn test_log_bins_u8() {
        let (bins, _) = Histogram::<u8>::power_of_two().bins_and_counts();
        assert_eq!(bins, [0, 1, 2, 4, 8, 16, 32, 64, 128],);
    }

    #[test]
    fn test_log_bins_u64() {
        let (bins, _) = Histogram::<u64>::power_of_two().bins_and_counts();
        assert_eq!(bins[0], 0);
        for (i, bin) in bins.iter().skip(1).enumerate() {
            assert_eq!(*bin, 1u64 << i);
        }
    }
}
