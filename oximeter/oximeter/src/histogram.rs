// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for managing metrics that are histograms.
// Copyright 2021 Oxide Computer Company

use chrono::{DateTime, Utc};
use num_traits::Bounded;
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::Ordering;
use std::ops::{Bound, Range, RangeBounds, RangeFrom, RangeTo};
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
    + DeserializeOwned
    + Clone
    + num_traits::Zero
    + num_traits::One
    + 'static
{
    fn is_finite(&self) -> bool;
}

impl HistogramSupport for i64 {
    fn is_finite(&self) -> bool {
        true
    }
}

impl HistogramSupport for f64 {
    fn is_finite(&self) -> bool {
        f64::is_finite(*self)
    }
}

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
    #[error("Bin edges and samples must be finite values, found: {0:?}")]
    NonFiniteValue(String),

    /// Error returned when two neighboring bins are not adjoining (there's space between them)
    #[error("Neigboring bins {left} and {right} are not adjoining")]
    NonAdjoiningBins { left: String, right: String },

    /// Bin and count arrays are of different sizes.
    #[error("Bin and count arrays must have the same size, found {n_bins} and {n_counts}")]
    ArraySizeMismatch { n_bins: usize, n_counts: usize },
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

/// A simple type for managing a histogram metric.
///
/// A histogram maintains the count of any number of samples, over a set of bins. Bins are
/// specified on construction via their _left_ edges, inclusive. There can't be any "gaps" in the
/// bins, and an additional bin may be added to the left, right, or both so that the bins extend to
/// the entire range of the support.
///
/// Note that any gaps, unsorted bins, or non-finite values will result in an error.
///
/// Example
/// -------
/// ```rust
/// use oximeter::histogram::{BinRange, Histogram};
///
/// let edges = [0i64, 10, 20];
/// let mut hist = Histogram::new(&edges).unwrap();
/// assert_eq!(hist.n_bins(), 4); // One additional bin for the range (20..)
/// assert_eq!(hist.n_samples(), 0);
/// hist.sample(4);
/// hist.sample(100);
/// assert_eq!(hist.n_samples(), 2);
///
/// let data = hist.iter().collect::<Vec<_>>();
/// assert_eq!(data[0].range, BinRange::range(i64::MIN, 0)); // An additional bin for `..0`
/// assert_eq!(data[0].count, 0); // Nothing is in this bin
///
/// assert_eq!(data[1].range, BinRange::range(0, 10)); // The range `0..10`
/// assert_eq!(data[1].count, 1); // 4 is sampled into this bin
/// ```
///
/// Notes
/// -----
///
/// Histograms may be constructed either from their left bin edges, or from a sequence of ranges.
/// In either case, the left-most bin may be converted upon construction. In particular, if the
/// left-most value is not equal to the minimum of the support, a new bin will be added from the
/// minimum to that provided value. If the left-most value _is_ the support's minimum, because the
/// provided bin was unbounded below, such as `(..0)`, then that bin will be converted into one
/// bounded below, `(MIN..0)` in this case.
///
/// The short of this is that, most of the time, it shouldn't matter. If one specifies the extremes
/// of the support as their bins, be aware that the left-most may be converted from a
/// `BinRange::RangeTo` into a `BinRange::Range`. In other words, the first bin of a histogram is
/// _always_ a `Bin::Range` or a `Bin::RangeFrom` after construction. In fact, every bin is one of
/// those variants, the `BinRange::RangeTo` is only provided as a convenience during construction.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(rename = "Histogram{T}")]
pub struct Histogram<T> {
    start_time: DateTime<Utc>,
    bins: Vec<Bin<T>>,
    n_samples: u64,
}

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
                        return Err(
                            HistogramError::NonAdjoiningBins {
                                left: format!("{:?}", first),
                                right: format!("{:?}", second),
                            });
                    }
                }
                _ => unreachable!("Bin ranges should always be excluded above and included below: {:#?}", (first, second))
            }
        }
        if let Bound::Excluded(end) = bins_.last().unwrap().range.end_bound() {
            ensure_finite(*end)?;
        }
        Ok(Self { start_time: Utc::now(), bins: bins_, n_samples: 0 })
    }

    /// Construct a new histogram from left bin edges.
    ///
    /// The left edges of the bins must be specified as a non-empty, monotonically increasing
    /// slice. An `Err` is returned if either constraint is violated.
    pub fn new(left_edges: &[T]) -> Result<Self, HistogramError> {
        let mut items = left_edges.iter();
        let mut bins = Vec::with_capacity(left_edges.len() + 1);
        let mut current = *items.next().ok_or(HistogramError::EmptyBins)?;
        ensure_finite(current)?;
        let min = <T as Bounded>::min_value();
        if current > min {
            // Bin greater than the minimum was specified, insert a new one from `MIN..current`.
            bins.push(Bin { range: BinRange::range(min, current), count: 0 });
        } else if current == min {
            // An edge *at* the minimum was specified. Consume it, and insert a bin from
            // `MIN..next`, if one exists. If one does not, or if this is the last item, the
            // following loop will not be entered.
            let next =
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
                return Err(HistogramError::NonFiniteValue(format!(
                    "{:?}",
                    current
                )));
            }
        }
        if current < <T as Bounded>::max_value() {
            bins.push(Bin { range: BinRange::from(current), count: 0 });
        }
        Ok(Self { start_time: Utc::now(), bins, n_samples: 0 })
    }

    /// Add a new sample into the histogram.
    ///
    /// This bumps the internal counter at the bin containing `value`. An `Err` is returned if the
    /// sample is not within the distribution's support (non-finite).
    pub fn sample(&mut self, value: T) -> Result<(), HistogramError> {
        ensure_finite(value)?;
        let index = self
            .bins
            .binary_search_by(|bin| bin.range.cmp(&value).reverse())
            .unwrap(); // The `ensure_finite` call above catches values that don't end up in a bin
        self.bins[index].count += 1;
        self.n_samples += 1;
        Ok(())
    }

    /// Return the total number of samples contained in the histogram.
    pub fn n_samples(&self) -> u64 {
        self.n_samples
    }

    /// Return the number of bins in the histogram.
    pub fn n_bins(&self) -> usize {
        self.bins.len()
    }

    /// Iterate over the bins of the histogram.
    pub fn iter(&self) -> impl Iterator<Item = &Bin<T>> {
        self.bins.iter()
    }

    /// Generate paired arrays with the left bin edges and the counts, for each bin.
    ///
    /// The returned edges are always left-inclusive, by construction of the histogram.
    pub fn to_arrays(&self) -> (Vec<T>, Vec<u64>) {
        let mut bins = Vec::with_capacity(self.n_bins());
        let mut counts = Vec::with_capacity(self.n_bins());

        // The first bin may either be BinRange::To or BinRange::Range.
        for bin in self.bins.iter() {
            match bin.range {
                BinRange::Range { start, .. } => {
                    bins.push(start);
                },
                BinRange::RangeFrom{start} => {
                    bins.push(start);
                },
                _ => unreachable!("No bins in a constructed histogram should be of type RangeTo"),
            }
            counts.push(bin.count);
        }
        (bins, counts)
    }

    /// Construct a histogram from a start time and paired arrays with the left bin-edge and counts.
    pub fn from_arrays(
        start_time: DateTime<Utc>,
        bins: Vec<T>,
        counts: Vec<u64>,
    ) -> Result<Self, HistogramError> {
        if bins.len() != counts.len() {
            return Err(HistogramError::ArraySizeMismatch {
                n_bins: bins.len(),
                n_counts: counts.len(),
            });
        }
        let mut hist = Self::new(&bins)?;
        hist.start_time = start_time;
        let mut n_samples = 0;
        for (bin, count) in hist.bins.iter_mut().zip(counts.into_iter()) {
            bin.count = count;
            n_samples += count;
        }
        hist.n_samples = n_samples;
        Ok(hist)
    }

    /// Return the start time for this histogram
    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// Generate a histogram with bins linearly spaced within each decade in the range
    /// `[start_decade, stop_decade)`.
    ///
    /// This generates a "log-linear" histogram. Within each power of 10, the bins of the histogram
    /// are linearly spaced. Note that any additional bins on the left will be added, as described
    /// in `[Histogram::new]`. Also, an extra bin will be added from `[0, x)`, where `x == 10 **
    /// start_decade` -- in other words, this will add the first bin from zero to the start of the
    /// decades specified.
    ///
    /// Example
    /// -------
    /// ```rust
    /// use oximeter::histogram::{Histogram, BinRange};
    /// use std::ops::{RangeBounds, Bound};
    ///
    /// let hist = Histogram::span_decades(-1, 1).unwrap();
    /// let bins = hist.iter().collect::<Vec<_>>();
    ///
    /// // First bin is from the left support edge to zero
    /// assert_eq!(bins[0].range.end_bound(), Bound::Excluded(&0.0));
    ///
    /// // First decade of bins is `[0.0, 0.1, 0.2, ...)`.
    /// assert_eq!(bins[1].range, BinRange::range(0.0, 0.1));
    /// assert_eq!(bins[2].range, BinRange::range(0.1, 0.2));
    ///
    /// // Second decade is `[1.0, 2.0, 3.0, ...]`
    /// assert_eq!(bins[10].range, BinRange::range(0.9, 1.0));
    /// assert_eq!(bins[11].range, BinRange::range(1.0, 2.0));
    ///
    /// // Ends at the third decade, so the last bin is the remainder of the support
    /// assert_eq!(bins[19].range, BinRange::from(9.0));
    /// ```
    pub fn span_decades<D>(
        start_decade: D,
        end_decade: D,
    ) -> Result<Self, HistogramError>
    where
        D: SpanDecade<D, T>,
        std::ops::Range<D>: Iterator<Item = D>,
    {
        let edges = [
            vec![<T as num_traits::Zero>::zero()],
            (start_decade..end_decade)
                .into_iter()
                .flat_map(|x| x.span_decade())
                .collect(),
        ]
        .concat();
        Histogram::new(&edges)
    }
}

/// A trait to support generating linearly-spaced bin edges that span the given decade.
///
/// This trait is used to generate what's sometimes called a "log-linear" histogram support. This
/// support has linearly spaced bins over a range, and many ranges, each of which is
/// logarithmically-spaced. Trait accepts a decade, a power of 10, and linearly spaces bins over
/// that decade, from `10 ** decade` to `10 ** (decade + 1)`, not inclusive of the right edges.
/// Note that the left bin is `1.0`, `10.0`, etc, not zero.
///
/// Example
/// -------
/// ```rust
/// use oximeter::histogram::SpanDecade;
/// let x = 0i8.span_decade();
/// assert_eq!(x, &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]);
/// ```
///
/// Note that the `SpanDecade` trait is parametrized by _two_ types, `D` is the input type, and `T`
/// is the output type, the type of elements generated in the return vector. This crate only
/// defines this trait for type pairs `(i8, f64)` and `(u8, i64)`. That is, calling
/// `i8::span_decade()` generates a `Vec<f64>` and `u8::span_decade()` generates a `Vec<i64>`.
pub trait SpanDecade<D, T>
where
    T: HistogramSupport,
{
    /// Return a set of bin edges linearly-spaced across the decade defined by `self`, i.e, `10 **
    /// self`.
    fn span_decade(&self) -> Vec<T>;
}

impl SpanDecade<i8, f64> for i8 {
    fn span_decade(&self) -> Vec<f64> {
        let mul = 10.0f64.powi((*self).into());
        (1..10).map(|x| (x as f64) * mul).collect()
    }
}

impl SpanDecade<u8, i64> for u8 {
    fn span_decade(&self) -> Vec<i64> {
        let mul = 10i64.pow((*self).into());
        (1..10).map(|x| x * mul).collect()
    }
}

// Helper to ensure all values are comparable, i.e., not NaN.
fn ensure_finite<T>(value: T) -> Result<(), HistogramError>
where
    T: HistogramSupport,
{
    if value.is_finite() {
        Ok(())
    } else {
        Err(HistogramError::NonFiniteValue(format!("{:?}", value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_approx_eq(x: f64, y: f64) {
        assert!((x - y).abs() < f64::EPSILON);
    }

    #[test]
    fn test_span_decade_f64() {
        fn run_test(decade: i8) {
            let diff = 10.0f64.powi(decade.into());
            let x = decade.span_decade();
            for (x, y) in x.iter().zip(x.iter().skip(1)) {
                assert_approx_eq(y - x, diff);
            }
        }
        run_test(0);
        run_test(-1);
        run_test(1);
    }

    #[test]
    fn test_span_decade_i64() {
        fn run_test(decade: u8) {
            let diff = 10i64.pow(decade.into());
            let x = decade.span_decade();
            for (x, y) in x.iter().zip(x.iter().skip(1)) {
                assert_eq!(y - x, diff);
            }
        }
        run_test(0);
        run_test(1);
        run_test(2);
    }

    #[test]
    fn test_ensure_finite() {
        assert!(ensure_finite(0i64).is_ok());
        assert!(ensure_finite(i64::MIN).is_ok());
        assert!(ensure_finite(i64::MAX).is_ok());

        assert!(ensure_finite(0.0).is_ok());
        assert!(ensure_finite(f64::NEG_INFINITY).is_err());
        assert!(ensure_finite(f64::INFINITY).is_err());
        assert!(ensure_finite(f64::NAN).is_err());
    }

    #[test]
    fn test_bin_range_to() {
        let range = BinRange::to(10);
        assert!(!range.contains(&100));
        assert!(range.contains(&0));
        assert_eq!(range.cmp(&0), Ordering::Equal);
        assert_eq!(range.cmp(&10), Ordering::Greater);
    }

    #[test]
    fn test_bin_range_from() {
        let range = BinRange::from(10);
        assert!(range.contains(&100));
        assert!(!range.contains(&0));
        assert_eq!(range.cmp(&0), Ordering::Less);
        assert_eq!(range.cmp(&10), Ordering::Equal);
    }

    #[test]
    fn test_bin_range() {
        let range = BinRange::range(0, 10);
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

        let samples = [-10i64, 0, 1, 10, 50];
        let expected_counts = [1u64, 2, 1, 1];
        for (i, sample) in samples.iter().enumerate() {
            hist.sample(*sample).unwrap();
            let count = i as u64 + 1;
            assert_eq!(
                hist.n_samples(),
                count,
                "Histogram should have {} sample(s)",
                count
            );
        }

        for (bin, &expected_count) in hist.iter().zip(expected_counts.iter()) {
            assert_eq!(
                bin.count, expected_count,
                "Bin {:?} expected to have {} items, but found {}",
                bin.range, expected_count, bin.count
            );
        }
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
    fn test_histogram_with_overlapping_bins() {
        let bins = &[(..1).into(), (0..10).into()];
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
        assert_eq!(hist.iter().nth(0).unwrap().count, 1);
        assert_eq!(hist.iter().nth(1).unwrap().count, 0);
        assert_eq!(hist.iter().nth(2).unwrap().count, 1);

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
            hist.n_bins(), 2,
            "This histogram should have two bins, since `BinRange`s are always exclusive on the right"
        );
        assert!(Histogram::with_bins(&[(f64::NAN..).into()]).is_err());
        assert!(Histogram::with_bins(&[(..f64::NAN).into()]).is_err());
        assert!(Histogram::with_bins(&[
            (0.0..f64::NAN).into(),
            (f64::NAN..100.0).into()
        ])
        .is_err());
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
    fn test_histogram_to_arrays() {
        let mut hist = Histogram::new(&[0, 10, 20]).unwrap();
        hist.sample(1).unwrap();
        hist.sample(11).unwrap();

        let (bins, counts) = hist.to_arrays();
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

        let rebuilt =
            Histogram::from_arrays(hist.start_time(), bins, counts).unwrap();
        assert_eq!(
            hist, rebuilt,
            "Histogram reconstructed from paired arrays is not correct"
        );
    }

    #[test]
    fn test_span_decades() {
        let hist = Histogram::span_decades(0i8, 3i8).unwrap();
        println!("{:#?}", hist.bins);
        assert_eq!(hist.n_bins(), 9 * 3 + 2); // 1 for bin from (-infty, 1), 1 for (0, 0.1)
    }
}
