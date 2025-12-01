// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Definition of data points for OxQL.

// Copyright 2024 Oxide Computer Company

use anyhow::Context;
use anyhow::Error;
use chrono::DateTime;
use chrono::Utc;
use num::ToPrimitive;
use oximeter_types::DatumType;
use oximeter_types::Measurement;
use oximeter_types::Quantile;
use oximeter_types::traits::HistogramSupport;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::ops::Sub;

/// The type of each individual data point's value in a timeseries.
#[derive(
    Clone, Copy, Debug, Deserialize, Hash, JsonSchema, PartialEq, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    /// A 64-bit integer.
    Integer,
    /// A 64-bit float.
    Double,
    /// A boolean.
    Boolean,
    /// A string.
    String,
    /// A distribution, a sequence of integer bins and counts.
    IntegerDistribution,
    /// A distribution, a sequence of double bins and integer counts.
    DoubleDistribution,
}

impl DataType {
    /// True if this is a numeric scalar type.
    pub const fn is_numeric(&self) -> bool {
        matches!(self, DataType::Integer | DataType::Double)
    }

    /// Return true if this is a distribution data type.
    pub const fn is_distribution(&self) -> bool {
        matches!(
            self,
            DataType::IntegerDistribution | DataType::DoubleDistribution
        )
    }
}

impl TryFrom<DatumType> for DataType {
    type Error = Error;

    fn try_from(datum_type: DatumType) -> Result<Self, Self::Error> {
        let data_type = match datum_type {
            DatumType::Bool => DataType::Boolean,
            DatumType::I8
            | DatumType::U8
            | DatumType::I16
            | DatumType::U16
            | DatumType::I32
            | DatumType::U32
            | DatumType::I64
            | DatumType::U64
            | DatumType::CumulativeI64
            | DatumType::CumulativeU64 => DataType::Integer,
            DatumType::F32
            | DatumType::F64
            | DatumType::CumulativeF32
            | DatumType::CumulativeF64 => DataType::Double,
            DatumType::String => DataType::String,
            DatumType::HistogramI8
            | DatumType::HistogramU8
            | DatumType::HistogramI16
            | DatumType::HistogramU16
            | DatumType::HistogramI32
            | DatumType::HistogramU32
            | DatumType::HistogramI64
            | DatumType::HistogramU64 => DataType::IntegerDistribution,
            DatumType::HistogramF32 | DatumType::HistogramF64 => {
                DataType::DoubleDistribution
            }
            DatumType::Bytes => {
                anyhow::bail!("Unsupported datum type: {}", datum_type)
            }
        };
        Ok(data_type)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The type of the metric itself, indicating what its values represent.
#[derive(
    Clone, Copy, Debug, Deserialize, Hash, JsonSchema, PartialEq, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    /// The value represents an instantaneous measurement in time.
    Gauge,
    /// The value represents a difference between two points in time.
    Delta,
    /// The value represents an accumulation between two points in time.
    Cumulative,
}
impl MetricType {
    /// Return true if this is cumulative.
    pub const fn is_cumulative(&self) -> bool {
        matches!(self, MetricType::Cumulative)
    }
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// A converted oximeter datum, used internally.
//
// This is used when computing deltas between cumulative measurements, and so
// only represents the possible cumulative types.
#[derive(Clone, Debug, PartialEq)]
enum CumulativeDatum {
    Integer(i64),
    Double(f64),
    IntegerDistribution(Distribution<i64>),
    DoubleDistribution(Distribution<f64>),
}

impl CumulativeDatum {
    // Construct a datum from a cumulative type, failing if the measurement is
    // not cumulative.
    fn from_cumulative(meas: &Measurement) -> Result<Self, Error> {
        let datum = match meas.datum() {
            oximeter_types::Datum::CumulativeI64(val) => {
                CumulativeDatum::Integer(val.value())
            }
            oximeter_types::Datum::CumulativeU64(val) => {
                let int = val
                    .value()
                    .try_into()
                    .context("Overflow converting u64 to i64")?;
                CumulativeDatum::Integer(int)
            }
            oximeter_types::Datum::CumulativeF32(val) => {
                CumulativeDatum::Double(val.value().into())
            }
            oximeter_types::Datum::CumulativeF64(val) => {
                CumulativeDatum::Double(val.value())
            }
            oximeter_types::Datum::HistogramI8(hist) => hist.into(),
            oximeter_types::Datum::HistogramU8(hist) => hist.into(),
            oximeter_types::Datum::HistogramI16(hist) => hist.into(),
            oximeter_types::Datum::HistogramU16(hist) => hist.into(),
            oximeter_types::Datum::HistogramI32(hist) => hist.into(),
            oximeter_types::Datum::HistogramU32(hist) => hist.into(),
            oximeter_types::Datum::HistogramI64(hist) => hist.into(),
            oximeter_types::Datum::HistogramU64(hist) => hist.try_into()?,
            oximeter_types::Datum::HistogramF32(hist) => hist.into(),
            oximeter_types::Datum::HistogramF64(hist) => hist.into(),
            other => anyhow::bail!(
                "Input datum of type {} is not cumulative",
                other.datum_type(),
            ),
        };
        Ok(datum)
    }
}

/// A single list of values, for one dimension of a timeseries.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct Values {
    /// The data values.
    pub values: ValueArray,
    /// The type of this metric.
    pub metric_type: MetricType,
}

impl Values {
    // Construct an empty array of values to hold the provided types.
    fn with_capacity(
        size: usize,
        data_type: DataType,
        metric_type: MetricType,
    ) -> Self {
        Self { values: ValueArray::with_capacity(size, data_type), metric_type }
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

/// Reference type describing a single point in a `Points` array.
///
/// The `Points` type is column-major, in that the timestamps and each data
/// value (one for each dimension) are stored in separate arrays, of the same
/// length. This type holds references to the relevant items in each array that
/// constitutes a single point.
#[derive(Clone, Debug, PartialEq)]
pub struct Point<'a> {
    /// The start time of this point, if any.
    pub start_time: Option<&'a DateTime<Utc>>,
    /// The timestamp for this point.
    pub timestamp: &'a DateTime<Utc>,
    /// One datum and its metric type, for each dimension in the point.
    ///
    /// The datum itself is optional, and will be `None` if the point is missing
    /// a value at the corresponding point and dimension.
    pub values: Vec<(Datum<'a>, MetricType)>,
}

impl fmt::Display for Point<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S.%f";
        match &self.start_time {
            Some(start_time) => write!(
                f,
                "[{}, {}]: ",
                start_time.format(TIMESTAMP_FMT),
                self.timestamp.format(TIMESTAMP_FMT)
            )?,
            None => write!(f, "{}: ", self.timestamp.format(TIMESTAMP_FMT))?,
        }
        let values = self
            .values
            .iter()
            .map(|(datum, _)| datum.to_string())
            .collect::<Vec<_>>()
            .join(",");
        write!(f, "[{}]", values)
    }
}

impl Point<'_> {
    /// Return the dimensionality of this point.
    pub fn dimensionality(&self) -> usize {
        self.values.len()
    }
}

/// A reference to a single datum of a multidimensional value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Datum<'a> {
    Boolean(Option<bool>),
    Integer(Option<&'a i64>),
    Double(Option<&'a f64>),
    String(Option<&'a str>),
    IntegerDistribution(Option<&'a Distribution<i64>>),
    DoubleDistribution(Option<&'a Distribution<f64>>),
}

impl fmt::Display for Datum<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Datum::Boolean(Some(inner)) => write!(f, "{}", inner),
            Datum::Integer(Some(inner)) => write!(f, "{}", inner),
            Datum::Double(Some(inner)) => write!(f, "{}", inner),
            Datum::String(Some(inner)) => write!(f, "{}", inner),
            Datum::IntegerDistribution(Some(inner)) => write!(f, "{}", inner),
            Datum::DoubleDistribution(Some(inner)) => write!(f, "{}", inner),
            Datum::Boolean(None)
            | Datum::Integer(None)
            | Datum::Double(None)
            | Datum::String(None)
            | Datum::IntegerDistribution(None)
            | Datum::DoubleDistribution(None) => {
                write!(f, "-")
            }
        }
    }
}

/// Timepoints and values for one timeseries.
//
// Invariants:
//
// The start_time and timestamp arrays must be the same length, or start_times
// must be None.
//
// The length of timestamps (and possibly start_times) must be the same as the
// length of _each element_ of the `values` array. That is, there are as many
// timestamps as data values.
//
// The length of `values` is the number of dimensions, and is always at least 1.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct Points {
    // The start time points for cumulative or delta metrics.
    pub(crate) start_times: Option<Vec<DateTime<Utc>>>,
    // The timestamp of each value.
    pub(crate) timestamps: Vec<DateTime<Utc>>,
    // The array of data values, one for each dimension.
    pub(crate) values: Vec<Values>,
}

impl Points {
    /// Construct a new `Points` with the provided data.
    pub fn new(
        start_times: Option<Vec<DateTime<Utc>>>,
        timestamps: Vec<DateTime<Utc>>,
        values: Vec<Values>,
    ) -> Self {
        Self { start_times, timestamps, values }
    }

    /// Construct an empty array of points to hold data of the provided type.
    pub fn empty(data_type: DataType, metric_type: MetricType) -> Self {
        Self::with_capacity(
            0,
            std::iter::once(data_type),
            std::iter::once(metric_type),
        )
        .unwrap()
    }

    /// Return the start times of the points, if any.
    pub fn start_times(&self) -> Option<&[DateTime<Utc>]> {
        self.start_times.as_deref()
    }

    /// Clear the start times of the points.
    pub fn clear_start_times(&mut self) {
        self.start_times = None;
    }

    /// Return the timestamps of the points.
    pub fn timestamps(&self) -> &[DateTime<Utc>] {
        &self.timestamps
    }

    pub fn set_timestamps(&mut self, timestamps: Vec<DateTime<Utc>>) {
        self.timestamps = timestamps;
    }

    /// Return a mutable reference to the value array of the specified
    /// dimension, if any.
    pub fn values_mut(&mut self, dim: usize) -> Option<&mut ValueArray> {
        self.values.get_mut(dim).map(|val| &mut val.values)
    }

    /// Return a reference to the value array of the specified dimension, if any.
    pub fn values(&self, dim: usize) -> Option<&ValueArray> {
        self.values.get(dim).map(|val| &val.values)
    }

    /// Return the dimensionality of the data points, i.e., the number of values
    /// at each timestamp.
    pub fn dimensionality(&self) -> usize {
        self.values.len()
    }

    /// Return the number of points in self.
    pub fn len(&self) -> usize {
        self.values[0].len()
    }

    /// Construct an empty array of points to hold size data points of the
    /// provided types.
    ///
    /// The type information may have length > 1 to reserve space for
    /// multi-dimensional values.
    pub fn with_capacity<D, M>(
        size: usize,
        data_types: D,
        metric_types: M,
    ) -> Result<Self, Error>
    where
        D: ExactSizeIterator<Item = DataType>,
        M: ExactSizeIterator<Item = MetricType>,
    {
        anyhow::ensure!(
            data_types.len() == metric_types.len(),
            "Data and metric type iterators must have the same length",
        );
        let timestamps = Vec::with_capacity(size);
        let mut start_times = None;
        let mut values = Vec::with_capacity(data_types.len());
        for (data_type, metric_type) in data_types.zip(metric_types) {
            if matches!(metric_type, MetricType::Delta | MetricType::Cumulative)
                && start_times.is_none()
            {
                start_times.replace(Vec::with_capacity(size));
            }
            values.push(Values::with_capacity(size, data_type, metric_type));
        }
        Ok(Self { start_times, timestamps, values })
    }

    /// Return the data types of self.
    pub fn data_types(&self) -> impl ExactSizeIterator<Item = DataType> + '_ {
        self.values.iter().map(|val| val.values.data_type())
    }

    /// Return the metric types of self.
    pub fn metric_types(
        &self,
    ) -> impl ExactSizeIterator<Item = MetricType> + '_ {
        self.values.iter().map(|val| val.metric_type)
    }

    /// Return the single metric type of all values in self, it they are all the
    /// same.
    pub fn metric_type(&self) -> Option<MetricType> {
        let mut types = self.metric_types();
        let Some(first_type) = types.next() else {
            unreachable!();
        };
        if types.all(|ty| ty == first_type) { Some(first_type) } else { None }
    }

    /// Construct a list of gauge points from a list of gauge measurements.
    ///
    /// An error is returned if the provided input measurements are not gauges,
    /// or do not all have the same datum type.
    pub fn gauge_from_gauge(
        measurements: &[Measurement],
    ) -> Result<Self, Error> {
        let Some(first) = measurements.first() else {
            anyhow::bail!(
                "Cannot construct points from empty measurements array"
            );
        };
        let datum_type = first.datum_type();
        anyhow::ensure!(
            !datum_type.is_cumulative(),
            "Measurements are not gauges"
        );
        let data_type = DataType::try_from(datum_type)?;
        let mut self_ = Self::with_capacity(
            measurements.len(),
            std::iter::once(data_type),
            std::iter::once(MetricType::Gauge),
        )?;

        // Since we're directly pushing gauges, each measurement is independent
        // of the others. Simply translate types and push the data.
        for measurement in measurements.iter() {
            anyhow::ensure!(
                measurement.datum_type() == datum_type,
                "Measurements must all have the same datum type",
            );
            self_
                .values_mut(0)
                .unwrap()
                .push_value_from_datum(measurement.datum())?;
            self_.timestamps.push(measurement.timestamp());
        }
        Ok(self_)
    }

    /// Construct a list of delta points from a list of cumulative measurements.
    ///
    /// An error is returned if the provided measurements are not of the same
    /// type or not cumulative.
    pub fn delta_from_cumulative(
        measurements: &[Measurement],
    ) -> Result<Self, Error> {
        let mut iter = measurements.iter();
        let Some(first) = iter.next() else {
            anyhow::bail!(
                "Cannot construct points from empty measurements array"
            );
        };
        let datum_type = first.datum_type();
        anyhow::ensure!(
            datum_type.is_cumulative(),
            "Measurements are not cumulative",
        );
        let data_type = DataType::try_from(datum_type)?;
        let mut self_ = Self::with_capacity(
            measurements.len(),
            std::iter::once(data_type),
            std::iter::once(MetricType::Delta),
        )?;

        // Construct the first point, which directly uses the start / end time
        // of the first measurement itself.
        self_.values_mut(0).unwrap().push_value_from_datum(first.datum())?;
        self_.start_times.as_mut().unwrap().push(first.start_time().unwrap());
        self_.timestamps.push(first.timestamp());

        // We need to keep track of the last cumulative measurement that's not
        // _missing_, to compute successive differences between neighboring
        // points. Note that we only need the datum from the measurement,
        // because even missing samples have valid timestamp information. So we
        // can always generate the timestamp for each delta, even if the datum
        // is missing.
        let mut last_datum = if first.is_missing() {
            None
        } else {
            // Safety: We're confirming above the measurement is cumulative, and
            // in this block if the datum is missing. So we know this conversion
            // should succeed.
            Some(CumulativeDatum::from_cumulative(first).unwrap())
        };

        // We also need to keep track of the start time of this "epoch", periods
        // where the cumulative data has the same start time. If there are jumps
        // forward in this, and thus gaps in the records, we need to update the
        // start_time of the epoch and also the last datum.
        let mut epoch_start_time = first.start_time().unwrap();

        // Push the remaining values.
        for measurement in iter {
            anyhow::ensure!(
                measurement.datum_type() == datum_type,
                "Measurements must all have the same datum type"
            );

            // For the time ranges we must have either:
            //
            // 1. Either the start time of the _first_ and new points must be
            //    equal, with the timestamp of the new strictly later than the
            //    timestamp of the last, OR
            // 2. Both the start time and timestamp of the new point must be
            //    strictly later than the timestamp (and thus start time) of the
            //    last point. In this case, we effectively have a _gap_ in the
            //    timeseries, and so we need to update `first_start_time` to
            //    reflect this new epoch.
            let last_start_time =
                *self_.start_times.as_ref().unwrap().last().unwrap();
            let last_timestamp = *self_.timestamps.last().unwrap();
            let new_start_time = measurement.start_time().unwrap();
            let new_timestamp = measurement.timestamp();

            if epoch_start_time == new_start_time
                && last_timestamp < new_timestamp
            {
                // Push the timestamps to reflect this interval, from the end of
                // the last sample to the end of this one.
                self_.start_times.as_mut().unwrap().push(last_timestamp);
                self_.timestamps.push(new_timestamp);

                // The data value is the difference between the last non-missing
                // datum and the new datum.
                self_.values_mut(0).unwrap().push_diff_from_last_to_datum(
                    &last_datum,
                    measurement.datum(),
                    data_type,
                )?;
            } else if new_start_time > last_timestamp
                && new_timestamp > last_timestamp
            {
                // Push the new start time directly, since it begins a new
                // epoch.
                self_.start_times.as_mut().unwrap().push(new_start_time);
                self_.timestamps.push(new_timestamp);

                // Update the epoch start time, and also simply push the datum
                // directly. The difference with the previous is not meaningful,
                // since we've begun a new epoch.
                epoch_start_time = new_start_time;
                self_
                    .values_mut(0)
                    .unwrap()
                    .push_value_from_datum(measurement.datum())?;
            } else {
                // Print as useful a message as we can here.
                anyhow::bail!(
                    "Cannot compute a delta, the timestamp of the next \
                    sample has a new start time, or overlaps with the \
                    last processed sample. \n \
                    epoch start time = {epoch_start_time}\n \
                    last timestamp = [{last_start_time}, {last_timestamp}]\n \
                    new timestamp = [{new_start_time}, {new_timestamp}]"
                );
            }

            // If the new datum is _not_ missing, we'll update the last one.
            if !measurement.is_missing() {
                last_datum.replace(
                    CumulativeDatum::from_cumulative(measurement).unwrap(),
                );
            }
        }
        Ok(self_)
    }

    /// Iterate over each point in self.
    pub fn iter_points(&self) -> impl Iterator<Item = Point<'_>> + '_ {
        (0..self.len()).map(|i| Point {
            start_time: self.start_times.as_ref().map(|s| &s[i]),
            timestamp: &self.timestamps[i],
            values: self
                .values
                .iter()
                .map(|val| (val.values.get(i), val.metric_type))
                .collect(),
        })
    }

    /// Filter points in self to those where `to_keep` is true.
    pub fn filter(&self, to_keep: Vec<bool>) -> Result<Points, Error> {
        anyhow::ensure!(
            to_keep.len() == self.len(),
            "Filter array must be the same length as self",
        );

        // Compute the indices of values we're keeping.
        let indices: Vec<_> = to_keep
            .iter()
            .enumerate()
            .filter(|(_ix, to_keep)| **to_keep)
            .map(|(ix, _)| ix)
            .collect();
        let n_true = indices.len();
        let mut out = Self::with_capacity(
            n_true,
            self.data_types(),
            self.metric_types(),
        )?;

        // Push the compressed start times, if any.
        if let Some(start_times) = self.start_times.as_ref() {
            let Some(new_start_times) = out.start_times.as_mut() else {
                unreachable!();
            };
            for ix in indices.iter().copied() {
                new_start_times.push(start_times[ix]);
            }
        }

        // Push the compressed timestamps.
        for ix in indices.iter().copied() {
            out.timestamps.push(self.timestamps[ix]);
        }

        // Push each dimension of the data values themselves.
        for (new_values, existing_values) in
            out.values.iter_mut().zip(self.values.iter())
        {
            match (&mut new_values.values, &existing_values.values) {
                (ValueArray::Integer(new), ValueArray::Integer(existing)) => {
                    for ix in indices.iter().copied() {
                        new.push(existing[ix]);
                    }
                }
                (ValueArray::Double(new), ValueArray::Double(existing)) => {
                    for ix in indices.iter().copied() {
                        new.push(existing[ix]);
                    }
                }
                (ValueArray::Boolean(new), ValueArray::Boolean(existing)) => {
                    for ix in indices.iter().copied() {
                        new.push(existing[ix]);
                    }
                }
                (ValueArray::String(new), ValueArray::String(existing)) => {
                    for ix in indices.iter().copied() {
                        new.push(existing[ix].clone());
                    }
                }
                (
                    ValueArray::IntegerDistribution(new),
                    ValueArray::IntegerDistribution(existing),
                ) => {
                    for ix in indices.iter().copied() {
                        new.push(existing[ix].clone());
                    }
                }
                (
                    ValueArray::DoubleDistribution(new),
                    ValueArray::DoubleDistribution(existing),
                ) => {
                    for ix in indices.iter().copied() {
                        new.push(existing[ix].clone());
                    }
                }
                (_, _) => unreachable!(),
            }
        }
        Ok(out)
    }

    /// Return a new set of points, with the values casted to the provided types.
    pub fn cast(&self, types: &[DataType]) -> Result<Self, Error> {
        anyhow::ensure!(
            types.len() == self.dimensionality(),
            "Cannot cast to {} types, the data has dimensionality {}",
            types.len(),
            self.dimensionality(),
        );
        let start_times = self.start_times.clone();
        let timestamps = self.timestamps.clone();
        let mut new_values = Vec::with_capacity(self.dimensionality());
        for (new_type, existing_values) in types.iter().zip(self.values.iter())
        {
            let values = match (new_type, &existing_values.values) {
                // "Cast" from i64 -> i64
                (DataType::Integer, ValueArray::Integer(vals)) => {
                    ValueArray::Integer(vals.clone())
                }

                // Cast f64 -> i64
                (DataType::Integer, ValueArray::Double(doubles)) => {
                    let mut new = Vec::with_capacity(doubles.len());
                    for maybe_double in doubles.iter().copied() {
                        if let Some(d) = maybe_double {
                            let as_int = d
                                .to_i64()
                                .context("Cannot cast double {d} to i64")?;
                            new.push(Some(as_int));
                        } else {
                            new.push(None);
                        }
                    }
                    ValueArray::Integer(new)
                }

                // Cast bool -> i64
                (DataType::Integer, ValueArray::Boolean(bools)) => {
                    ValueArray::Integer(
                        bools
                            .iter()
                            .copied()
                            .map(|b| b.map(i64::from))
                            .collect(),
                    )
                }

                // Cast string -> i64, by parsing.
                (DataType::Integer, ValueArray::String(strings)) => {
                    let mut new = Vec::with_capacity(strings.len());
                    for maybe_str in strings.iter() {
                        if let Some(s) = maybe_str {
                            let as_int = s
                                .parse()
                                .context("Cannot cast string '{s}' to i64")?;
                            new.push(Some(as_int));
                        } else {
                            new.push(None);
                        }
                    }
                    ValueArray::Integer(new)
                }

                // Cast i64 -> f64
                (DataType::Double, ValueArray::Integer(ints)) => {
                    let mut new = Vec::with_capacity(ints.len());
                    for maybe_int in ints.iter().copied() {
                        if let Some(int) = maybe_int {
                            let as_double = int.to_f64().context(
                                "Cannot cast integer {int} as double",
                            )?;
                            new.push(Some(as_double));
                        } else {
                            new.push(None);
                        }
                    }
                    ValueArray::Double(new)
                }

                // "Cast" f64 -> f64
                (DataType::Double, ValueArray::Double(vals)) => {
                    ValueArray::Double(vals.clone())
                }

                // Cast bool -> f64
                (DataType::Double, ValueArray::Boolean(bools)) => {
                    ValueArray::Double(
                        bools
                            .iter()
                            .copied()
                            .map(|b| b.map(f64::from))
                            .collect(),
                    )
                }

                // Cast string -> f64, by parsing.
                (DataType::Double, ValueArray::String(strings)) => {
                    let mut new = Vec::with_capacity(strings.len());
                    for maybe_str in strings.iter() {
                        if let Some(s) = maybe_str {
                            let as_double = s
                                .parse()
                                .context("Cannot cast string '{s}' to f64")?;
                            new.push(Some(as_double));
                        } else {
                            new.push(None);
                        }
                    }
                    ValueArray::Double(new)
                }

                // Cast i64 -> bool
                //
                // Any non-zero value is considered truthy.
                (DataType::Boolean, ValueArray::Integer(ints)) => {
                    let mut new = Vec::with_capacity(ints.len());
                    for maybe_int in ints.iter().copied() {
                        match maybe_int {
                            Some(0) => new.push(Some(false)),
                            Some(_) => new.push(Some(true)),
                            None => new.push(None),
                        }
                    }
                    ValueArray::Boolean(new)
                }

                // Cast f64 -> bool
                //
                // Any non-zero value is considered truthy.
                (DataType::Boolean, ValueArray::Double(doubles)) => {
                    let mut new = Vec::with_capacity(doubles.len());
                    for maybe_double in doubles.iter().copied() {
                        match maybe_double {
                            Some(0.0) => new.push(Some(false)),
                            Some(_) => new.push(Some(true)),
                            None => new.push(None),
                        }
                    }
                    ValueArray::Boolean(new)
                }

                // "Cast" bool -> bool
                (DataType::Boolean, ValueArray::Boolean(vals)) => {
                    ValueArray::Boolean(vals.clone())
                }

                // Cast string -> bool.
                //
                // Any non-empty string is considered truthy
                (DataType::Boolean, ValueArray::String(strings)) => {
                    let mut new = Vec::with_capacity(strings.len());
                    for maybe_str in strings.iter() {
                        match maybe_str {
                            Some(s) if s.is_empty() => new.push(Some(false)),
                            Some(_) => new.push(Some(true)),
                            None => new.push(None),
                        }
                    }
                    ValueArray::Boolean(new)
                }

                // Cast i64 -> string
                (DataType::String, ValueArray::Integer(ints)) => {
                    ValueArray::String(
                        ints.iter().map(|x| x.map(|x| x.to_string())).collect(),
                    )
                }

                // Cast f64 -> string
                (DataType::String, ValueArray::Double(doubles)) => {
                    ValueArray::String(
                        doubles
                            .iter()
                            .map(|x| x.map(|x| x.to_string()))
                            .collect(),
                    )
                }

                // Cast bool -> string
                (DataType::String, ValueArray::Boolean(bools)) => {
                    ValueArray::String(
                        bools
                            .iter()
                            .map(|x| x.map(|x| x.to_string()))
                            .collect(),
                    )
                }

                // "Cast" string -> string
                (DataType::String, ValueArray::String(vals)) => {
                    ValueArray::String(vals.clone())
                }

                // "Cast" distributions to the same type of distribution
                (
                    DataType::IntegerDistribution,
                    ValueArray::IntegerDistribution(vals),
                ) => ValueArray::IntegerDistribution(vals.clone()),
                (
                    DataType::DoubleDistribution,
                    ValueArray::DoubleDistribution(vals),
                ) => ValueArray::DoubleDistribution(vals.clone()),

                // All other casts are invalid
                (_, vals) => anyhow::bail!(
                    "Cannot cast {} -> {}",
                    new_type,
                    vals.data_type(),
                ),
            };
            new_values.push(Values {
                values,
                metric_type: existing_values.metric_type,
            });
        }
        Ok(Self { start_times, timestamps, values: new_values })
    }

    /// Given two arrays of points, stack them together at matching timepoints.
    ///
    /// For time points in either which do not have a corresponding point in
    /// the other, the entire time point is elided.
    pub fn inner_join(&self, right: &Points) -> Result<Points, Error> {
        // Create an output array with roughly the right capacity, and double the
        // number of dimensions. We're trying to stack output value arrays together
        // along the dimension axis.
        let data_types =
            self.data_types().chain(right.data_types()).collect::<Vec<_>>();
        let metric_types =
            self.metric_types().chain(right.metric_types()).collect::<Vec<_>>();
        let mut out = Points::with_capacity(
            self.len().max(right.len()),
            data_types.iter().copied(),
            metric_types.iter().copied(),
        )?;

        // Iterate through each array until one is exhausted. We're only inserting
        // values from both arrays where the timestamps actually match, since this
        // is an inner join. We may want to insert missing values where timestamps
        // do not match on either side, when we support an outer join of some kind.
        let n_left_dim = self.dimensionality();
        let mut left_ix = 0;
        let mut right_ix = 0;
        while left_ix < self.len() && right_ix < right.len() {
            let left_timestamp = self.timestamps()[left_ix];
            let right_timestamp = right.timestamps()[right_ix];
            if left_timestamp == right_timestamp {
                out.timestamps.push(left_timestamp);
                push_concrete_values(
                    &mut out.values[..n_left_dim],
                    &self.values,
                    left_ix,
                );
                push_concrete_values(
                    &mut out.values[n_left_dim..],
                    &right.values,
                    right_ix,
                );
                left_ix += 1;
                right_ix += 1;
            } else if left_timestamp < right_timestamp {
                left_ix += 1;
            } else {
                right_ix += 1;
            }
        }
        Ok(out)
    }

    /// Return true if self contains no data points.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// Push the `i`th value from each dimension of `from` onto `to`.
fn push_concrete_values(to: &mut [Values], from: &[Values], i: usize) {
    assert_eq!(to.len(), from.len());
    for (output, input) in to.iter_mut().zip(from.iter()) {
        let input_array = &input.values;
        let output_array = &mut output.values;
        assert_eq!(input_array.data_type(), output_array.data_type());
        if let Ok(ints) = input_array.as_integer() {
            output_array.as_integer_mut().unwrap().push(ints[i]);
            continue;
        }
        if let Ok(doubles) = input_array.as_double() {
            output_array.as_double_mut().unwrap().push(doubles[i]);
            continue;
        }
        if let Ok(bools) = input_array.as_boolean() {
            output_array.as_boolean_mut().unwrap().push(bools[i]);
            continue;
        }
        if let Ok(strings) = input_array.as_string() {
            output_array.as_string_mut().unwrap().push(strings[i].clone());
            continue;
        }
        if let Ok(dists) = input_array.as_integer_distribution() {
            output_array
                .as_integer_distribution_mut()
                .unwrap()
                .push(dists[i].clone());
            continue;
        }
        if let Ok(dists) = input_array.as_double_distribution() {
            output_array
                .as_double_distribution_mut()
                .unwrap()
                .push(dists[i].clone());
            continue;
        }
        unreachable!();
    }
}

/// List of data values for one timeseries.
///
/// Each element is an option, where `None` represents a missing sample.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "values")]
pub enum ValueArray {
    Integer(Vec<Option<i64>>),
    Double(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    IntegerDistribution(Vec<Option<Distribution<i64>>>),
    DoubleDistribution(Vec<Option<Distribution<f64>>>),
}

impl ValueArray {
    // Create an empty array with capacity `size` of the provided data type.
    fn with_capacity(size: usize, data_type: DataType) -> Self {
        match data_type {
            DataType::Integer => Self::Integer(Vec::with_capacity(size)),
            DataType::Double => Self::Double(Vec::with_capacity(size)),
            DataType::Boolean => Self::Boolean(Vec::with_capacity(size)),
            DataType::String => Self::String(Vec::with_capacity(size)),
            DataType::IntegerDistribution => {
                Self::IntegerDistribution(Vec::with_capacity(size))
            }
            DataType::DoubleDistribution => {
                Self::DoubleDistribution(Vec::with_capacity(size))
            }
        }
    }

    /// Return the data type in self.
    pub fn data_type(&self) -> DataType {
        match self {
            ValueArray::Integer(_) => DataType::Integer,
            ValueArray::Double(_) => DataType::Double,
            ValueArray::Boolean(_) => DataType::Boolean,
            ValueArray::String(_) => DataType::String,
            ValueArray::IntegerDistribution(_) => DataType::IntegerDistribution,
            ValueArray::DoubleDistribution(_) => DataType::DoubleDistribution,
        }
    }

    // Access the inner array of booleans, if possible.
    pub(super) fn as_boolean_mut(
        &mut self,
    ) -> Result<&mut Vec<Option<bool>>, Error> {
        let ValueArray::Boolean(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as boolean type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    /// Access the values as an array of bools, if they have that type.
    pub fn as_boolean(&self) -> Result<&Vec<Option<bool>>, Error> {
        let ValueArray::Boolean(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as boolean type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    /// Access the values as an array of integers, if they have that type.
    pub fn as_integer(&self) -> Result<&Vec<Option<i64>>, Error> {
        let ValueArray::Integer(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as integer type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    /// Access the inner array of integers, if possible.
    pub fn as_integer_mut(&mut self) -> Result<&mut Vec<Option<i64>>, Error> {
        let ValueArray::Integer(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as integer type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    /// Access the values as an array of doubles, if they have that type.
    pub fn as_double(&self) -> Result<&Vec<Option<f64>>, Error> {
        let ValueArray::Double(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as double type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    // Access the inner array of doubles, if possible.
    pub(super) fn as_double_mut(
        &mut self,
    ) -> Result<&mut Vec<Option<f64>>, Error> {
        let ValueArray::Double(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as double type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    /// Access the values as an array of strings, if they have that type.
    pub fn as_string(&self) -> Result<&Vec<Option<String>>, Error> {
        let ValueArray::String(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as string type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    // Access the inner array of strings, if possible.
    pub(super) fn as_string_mut(
        &mut self,
    ) -> Result<&mut Vec<Option<String>>, Error> {
        let ValueArray::String(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as string type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    /// Access the values as an array of integer distribution, if they have that
    /// type.
    pub fn as_integer_distribution(
        &self,
    ) -> Result<&Vec<Option<Distribution<i64>>>, Error> {
        let ValueArray::IntegerDistribution(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as integer \
                distribution type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    // Access the inner array of integer distribution, if possible.
    pub(super) fn as_integer_distribution_mut(
        &mut self,
    ) -> Result<&mut Vec<Option<Distribution<i64>>>, Error> {
        let ValueArray::IntegerDistribution(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as integer \
                distribution type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    /// Access the values as an array of double distribution, if they have that
    /// type.
    pub fn as_double_distribution(
        &self,
    ) -> Result<&Vec<Option<Distribution<f64>>>, Error> {
        let ValueArray::DoubleDistribution(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as double \
                distribution type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    // Access the inner array of double distributions, if possible.
    pub(super) fn as_double_distribution_mut(
        &mut self,
    ) -> Result<&mut Vec<Option<Distribution<f64>>>, Error> {
        let ValueArray::DoubleDistribution(inner) = self else {
            anyhow::bail!(
                "Cannot access value array as double \
                distribution type, it has type {}",
                self.data_type(),
            );
        };
        Ok(inner)
    }

    fn push_missing(&mut self, datum_type: DatumType) -> Result<(), Error> {
        match datum_type {
            DatumType::Bool => self.as_boolean_mut()?.push(None),
            DatumType::I8
            | DatumType::U8
            | DatumType::I16
            | DatumType::U16
            | DatumType::I32
            | DatumType::U32
            | DatumType::I64
            | DatumType::U64
            | DatumType::CumulativeI64
            | DatumType::CumulativeU64 => self.as_integer_mut()?.push(None),
            DatumType::F32
            | DatumType::F64
            | DatumType::CumulativeF32
            | DatumType::CumulativeF64 => self.as_double_mut()?.push(None),
            DatumType::String => self.as_string_mut()?.push(None),
            DatumType::Bytes => {
                anyhow::bail!("Bytes data types are not yet supported")
            }
            DatumType::HistogramI8
            | DatumType::HistogramU8
            | DatumType::HistogramI16
            | DatumType::HistogramU16
            | DatumType::HistogramI32
            | DatumType::HistogramU32
            | DatumType::HistogramI64
            | DatumType::HistogramU64 => {
                self.as_integer_distribution_mut()?.push(None)
            }
            DatumType::HistogramF32 | DatumType::HistogramF64 => {
                self.as_double_distribution_mut()?.push(None)
            }
        }
        Ok(())
    }

    // Push a value directly from a datum, without modification.
    fn push_value_from_datum(
        &mut self,
        datum: &oximeter_types::Datum,
    ) -> Result<(), Error> {
        match datum {
            oximeter_types::Datum::Bool(b) => {
                self.as_boolean_mut()?.push(Some(*b))
            }
            oximeter_types::Datum::I8(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter_types::Datum::U8(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter_types::Datum::I16(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter_types::Datum::U16(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter_types::Datum::I32(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter_types::Datum::U32(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter_types::Datum::I64(i) => {
                self.as_integer_mut()?.push(Some(*i))
            }
            oximeter_types::Datum::U64(i) => {
                let i =
                    i.to_i64().context("Failed to convert u64 datum to i64")?;
                self.as_integer_mut()?.push(Some(i));
            }
            oximeter_types::Datum::F32(f) => {
                self.as_double_mut()?.push(Some(f64::from(*f)))
            }
            oximeter_types::Datum::F64(f) => {
                self.as_double_mut()?.push(Some(*f))
            }
            oximeter_types::Datum::String(s) => {
                self.as_string_mut()?.push(Some(s.clone()))
            }
            oximeter_types::Datum::Bytes(_) => {
                anyhow::bail!("Bytes data types are not yet supported")
            }
            oximeter_types::Datum::CumulativeI64(c) => {
                self.as_integer_mut()?.push(Some(c.value()))
            }
            oximeter_types::Datum::CumulativeU64(c) => {
                let c = c
                    .value()
                    .to_i64()
                    .context("Failed to convert u64 datum to i64")?;
                self.as_integer_mut()?.push(Some(c));
            }
            oximeter_types::Datum::CumulativeF32(c) => {
                self.as_double_mut()?.push(Some(f64::from(c.value())))
            }
            oximeter_types::Datum::CumulativeF64(c) => {
                self.as_double_mut()?.push(Some(c.value()))
            }
            oximeter_types::Datum::HistogramI8(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::HistogramU8(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::HistogramI16(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::HistogramU16(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::HistogramI32(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::HistogramU32(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::HistogramI64(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::HistogramU64(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::try_from(h)?)),
            oximeter_types::Datum::HistogramF32(h) => self
                .as_double_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::HistogramF64(h) => self
                .as_double_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter_types::Datum::Missing(missing) => {
                self.push_missing(missing.datum_type())?
            }
        }
        Ok(())
    }

    // Push a delta from the last valid datum and a new one.
    //
    // This takes the last valid datum, if any, and a new one. It computes the
    // delta between the the values of the datum, if possible, and pushes it
    // onto the correct value array inside `self`.
    //
    // If both the last datum and new one exist (are not missing), the normal
    // diff is pushed. If the last datum is missing, but the new one exists,
    // then the new value is pushed directly. If the last datum exists but the
    // new one does not, then a missing datum is pushed. If both are missing,
    // then a missing one is pushed as well.
    //
    // In other words, the diff is always between the new datum and the last
    // non-None value. If such a last value does not exist, the datum is
    // inserted directly.
    fn push_diff_from_last_to_datum(
        &mut self,
        last_datum: &Option<CumulativeDatum>,
        new_datum: &oximeter_types::Datum,
        data_type: DataType,
    ) -> Result<(), Error> {
        match (last_datum.as_ref(), new_datum.is_missing()) {
            (None, true) | (Some(_), true) => {
                // In this case, either both values are missing, or just the new
                // one is. In either case, we cannot compute a new value, and
                // need to insert None to represent the new missing datum.
                match data_type {
                    DataType::Integer => self.as_integer_mut()?.push(None),
                    DataType::Double => self.as_double_mut()?.push(None),
                    DataType::Boolean => self.as_boolean_mut()?.push(None),
                    DataType::String => self.as_string_mut()?.push(None),
                    DataType::IntegerDistribution => {
                        self.as_integer_distribution_mut()?.push(None)
                    }
                    DataType::DoubleDistribution => {
                        self.as_double_distribution_mut()?.push(None)
                    }
                }
            }
            (None, false) => {
                // The last datum was missing, but the new one is not. We cannot
                // compute the difference, since we have no previous point.
                // However, we can still push some value by inserting the datum
                // directly.
                self.push_value_from_datum(new_datum)?;
            }
            (Some(last_datum), false) => {
                // Both values exist, so we can compute the difference between
                // them and insert that.
                //
                // Note that we're asserting both are the same _datum_ type,
                // which is guaranteed by a check in the caller.
                match (last_datum, new_datum) {
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::I8(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::U8(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::I16(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::U16(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::I32(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::U32(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::I64(new),
                    ) => {
                        let diff = new
                            .checked_sub(*last)
                            .context("Overflow computing deltas")?;
                        self.as_integer_mut()?.push(Some(diff));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::U64(new),
                    ) => {
                        let new = new
                            .to_i64()
                            .context("Failed to convert u64 datum to i64")?;
                        let diff = new
                            .checked_sub(*last)
                            .context("Overflow computing deltas")?;
                        self.as_integer_mut()?.push(Some(diff));
                    }
                    (
                        CumulativeDatum::Double(last),
                        oximeter_types::Datum::F32(new),
                    ) => {
                        self.as_double_mut()?
                            .push(Some(f64::from(*new) - last));
                    }
                    (
                        CumulativeDatum::Double(last),
                        oximeter_types::Datum::F64(new),
                    ) => {
                        self.as_double_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::CumulativeI64(new),
                    ) => {
                        let new = new.value();
                        let diff = new
                            .checked_sub(*last)
                            .context("Overflow computing deltas")?;
                        self.as_integer_mut()?.push(Some(diff));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter_types::Datum::CumulativeU64(new),
                    ) => {
                        let new = new
                            .value()
                            .to_i64()
                            .context("Failed to convert u64 datum to i64")?;
                        let diff = new
                            .checked_sub(*last)
                            .context("Overflow computing deltas")?;
                        self.as_integer_mut()?.push(Some(diff));
                    }
                    (
                        CumulativeDatum::Double(last),
                        oximeter_types::Datum::CumulativeF32(new),
                    ) => {
                        self.as_double_mut()?
                            .push(Some(f64::from(new.value()) - last));
                    }
                    (
                        CumulativeDatum::Double(last),
                        oximeter_types::Datum::CumulativeF64(new),
                    ) => {
                        self.as_double_mut()?.push(Some(new.value() - last));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter_types::Datum::HistogramI8(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter_types::Datum::HistogramU8(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter_types::Datum::HistogramI16(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter_types::Datum::HistogramU16(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter_types::Datum::HistogramI32(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter_types::Datum::HistogramU32(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter_types::Datum::HistogramI64(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter_types::Datum::HistogramU64(new),
                    ) => {
                        let new = Distribution::try_from(new)?;
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::DoubleDistribution(last),
                        oximeter_types::Datum::HistogramF32(new),
                    ) => {
                        let new = Distribution::<f64>::from(new);
                        self.as_double_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::DoubleDistribution(last),
                        oximeter_types::Datum::HistogramF64(new),
                    ) => {
                        let new = Distribution::<f64>::from(new);
                        self.as_double_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (_, _) => unreachable!(),
                }
            }
        }
        Ok(())
    }

    // Return the number of samples in self.
    fn len(&self) -> usize {
        match self {
            ValueArray::Boolean(inner) => inner.len(),
            ValueArray::Integer(inner) => inner.len(),
            ValueArray::Double(inner) => inner.len(),
            ValueArray::String(inner) => inner.len(),
            ValueArray::IntegerDistribution(inner) => inner.len(),
            ValueArray::DoubleDistribution(inner) => inner.len(),
        }
    }

    // Return a reference to the i-th value in the array.
    //
    // This panics if `i >= self.len()`.
    fn get(&self, i: usize) -> Datum<'_> {
        match self {
            ValueArray::Boolean(inner) => Datum::Boolean(inner[i]),
            ValueArray::Integer(inner) => {
                Datum::Integer(inner.get(i).unwrap().as_ref())
            }
            ValueArray::Double(inner) => {
                Datum::Double(inner.get(i).unwrap().as_ref())
            }
            ValueArray::String(inner) => {
                Datum::String(inner.get(i).unwrap().as_deref())
            }
            ValueArray::IntegerDistribution(inner) => {
                Datum::IntegerDistribution(inner.get(i).unwrap().as_ref())
            }
            ValueArray::DoubleDistribution(inner) => {
                Datum::DoubleDistribution(inner.get(i).unwrap().as_ref())
            }
        }
    }

    /// Swap the value in self with other, asserting they're the same type.
    pub fn swap(&mut self, mut values: ValueArray) {
        use std::mem::swap;
        match (self, &mut values) {
            (ValueArray::Integer(x), ValueArray::Integer(y)) => swap(x, y),
            (ValueArray::Double(x), ValueArray::Double(y)) => swap(x, y),
            (ValueArray::Boolean(x), ValueArray::Boolean(y)) => swap(x, y),
            (ValueArray::String(x), ValueArray::String(y)) => swap(x, y),
            (
                ValueArray::IntegerDistribution(x),
                ValueArray::IntegerDistribution(y),
            ) => swap(x, y),
            (
                ValueArray::DoubleDistribution(x),
                ValueArray::DoubleDistribution(y),
            ) => swap(x, y),
            (_, _) => panic!("Cannot swap values of different types"),
        }
    }
}

mod private {
    pub trait Sealed {}
    impl Sealed for i64 {}
    impl Sealed for f64 {}
}

pub trait DistributionSupport:
    fmt::Display + Clone + Copy + fmt::Debug + PartialEq + private::Sealed
{
}
impl DistributionSupport for i64 {}
impl DistributionSupport for f64 {}

/// A distribution is a sequence of bins and counts in those bins, and some
/// statistical information tracked to compute the mean, standard deviation, and
/// quantile estimates.
///
/// Min, max, and the p-* quantiles are treated as optional due to the
/// possibility of distribution operations, like subtraction.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[schemars(rename = "Distribution{T}")]
pub struct Distribution<T: DistributionSupport> {
    bins: Vec<T>,
    counts: Vec<u64>,
    min: Option<T>,
    max: Option<T>,
    sum_of_samples: T,
    squared_mean: f64,
    #[serde(serialize_with = "serialize_quantile")]
    #[schemars(with = "Option<f64>")]
    p50: Option<Quantile>,
    #[serde(serialize_with = "serialize_quantile")]
    #[schemars(with = "Option<f64>")]
    p90: Option<Quantile>,
    #[serde(serialize_with = "serialize_quantile")]
    #[schemars(with = "Option<f64>")]
    p99: Option<Quantile>,
}

/// Simplify quantiles to an estimate to abstract the details of the algorithm from the user.
fn serialize_quantile<S>(
    q: &Option<Quantile>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    q.and_then(|quantile| quantile.estimate().ok()).serialize(serializer)
}

impl<T> fmt::Display for Distribution<T>
where
    T: DistributionSupport + HistogramSupport + Sub<Output = T>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let elems = self
            .bins
            .iter()
            .zip(self.counts.iter())
            .map(|(bin, count)| format!("{bin}: {count}"))
            .collect::<Vec<_>>()
            .join(", ");

        let unwrap_estimate = |opt: Option<Quantile>| {
            opt.map_or("None".to_string(), |v| match v.estimate() {
                Ok(v) => v.to_string(),
                Err(err) => err.to_string(),
            })
        };

        let p50_estimate = unwrap_estimate(self.p50);
        let p90_estimate = unwrap_estimate(self.p90);
        let p99_estimate = unwrap_estimate(self.p99);

        write!(
            f,
            "{}, min: {}, max: {}, mean: {}, std_dev: {}, p50: {}, p90: {}, p99: {}",
            elems,
            self.min.map_or("none".to_string(), |m| m.to_string()),
            self.max.unwrap_or_default(),
            self.mean(),
            self.std_dev().unwrap_or_default(),
            p50_estimate,
            p90_estimate,
            p99_estimate
        )
    }
}

impl<T> Distribution<T>
where
    T: DistributionSupport + HistogramSupport + Sub<Output = T>,
{
    /// Subtract two distributions, checking that they have the same bins.
    ///
    /// Min and max values are returned as None, as they lose meaning
    /// when subtracting distributions. The same is true for p50, p90, and p99
    /// quantiles.
    ///
    /// TODO: It's not really clear how to compute the "difference" of two
    /// histograms for items like min, max, p*'s. It's certainly not linear, and
    /// although we might be able to make some estimates in the case of min and
    /// max, we'll defer it for now. Instead, we'll store None for all these
    /// values when computing the diff. They will be very useful later, when we
    /// start generating distributions in OxQL itself, from a sequence of
    /// scalars (similar to a DTrace aggregation). We'll wait to put that in
    /// place until we have more data that we want to start aggregating that
    /// way.
    fn checked_sub(
        &self,
        rhs: &Distribution<T>,
    ) -> Result<Distribution<T>, Error> {
        anyhow::ensure!(
            self.bins == rhs.bins,
            "Cannot subtract distributions with different bins",
        );
        let counts: Vec<_> = self
            .counts
            .iter()
            .zip(rhs.counts.iter())
            .map(|(x, y)| x.checked_sub(*y))
            .collect::<Option<_>>()
            .context("Underflow subtracting distributions values")?;

        // Subtract sum_of_samples.
        // This can be negative as T is either i64 or f64.
        let sum_of_samples = self.sum_of_samples - rhs.sum_of_samples;

        // Squared means are not linear, so we subtract the means and then
        // square that number.
        let sub_means = self.mean() - rhs.mean();
        let squared_mean = sub_means.powi(2);

        Ok(Self {
            bins: self.bins.clone(),
            counts,
            min: None,
            max: None,
            sum_of_samples,
            squared_mean,
            p50: None,
            p90: None,
            p99: None,
        })
    }

    /// Return the slice of bins.
    pub fn bins(&self) -> &[T] {
        &self.bins
    }

    /// Return the slice of counts.
    pub fn counts(&self) -> &[u64] {
        &self.counts
    }

    /// Return the number of samples in the distribution.
    pub fn n_samples(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Return the minimum value in the distribution.
    pub fn min(&self) -> Option<T> {
        self.min
    }

    /// Return the maximum value in the distribution.
    pub fn max(&self) -> Option<T> {
        self.max
    }

    /// Return the mean of the distribution.
    pub fn mean(&self) -> f64 {
        if self.n_samples() > 0 {
            // We can unwrap here because we know n_samples() > 0,
            // so the sum_of_samples should convert to f64 without issue.
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

    /// Return an iterator over each bin and count.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&T, &u64)> + '_ {
        self.bins.iter().zip(self.counts.iter())
    }
}

macro_rules! i64_dist_from {
    ($t:ty) => {
        impl From<&oximeter_types::histogram::Histogram<$t>>
            for Distribution<i64>
        {
            fn from(hist: &oximeter_types::histogram::Histogram<$t>) -> Self {
                let (bins, counts) = hist.bins_and_counts();
                Self {
                    bins: bins.into_iter().map(i64::from).collect(),
                    counts,
                    min: Some(i64::from(hist.min())),
                    max: Some(i64::from(hist.max())),
                    sum_of_samples: hist.sum_of_samples(),
                    squared_mean: hist.squared_mean(),
                    p50: Some(hist.p50q()),
                    p90: Some(hist.p90q()),
                    p99: Some(hist.p99q()),
                }
            }
        }

        impl From<&oximeter_types::histogram::Histogram<$t>>
            for CumulativeDatum
        {
            fn from(hist: &oximeter_types::histogram::Histogram<$t>) -> Self {
                CumulativeDatum::IntegerDistribution(hist.into())
            }
        }
    };
}

i64_dist_from!(i8);
i64_dist_from!(u8);
i64_dist_from!(i16);
i64_dist_from!(u16);
i64_dist_from!(i32);
i64_dist_from!(u32);
i64_dist_from!(i64);

impl TryFrom<&oximeter_types::histogram::Histogram<u64>> for Distribution<i64> {
    type Error = Error;
    fn try_from(
        hist: &oximeter_types::histogram::Histogram<u64>,
    ) -> Result<Self, Self::Error> {
        let (bins, counts) = hist.bins_and_counts();
        let bins = bins
            .into_iter()
            .map(i64::try_from)
            .collect::<Result<_, _>>()
            .context("Overflow converting u64 to i64")?;
        Ok(Self {
            bins,
            counts,
            min: Some(hist.min() as i64),
            max: Some(hist.max() as i64),
            sum_of_samples: hist.sum_of_samples(),
            squared_mean: hist.squared_mean(),
            p50: Some(hist.p50q()),
            p90: Some(hist.p90q()),
            p99: Some(hist.p99q()),
        })
    }
}

impl TryFrom<&oximeter_types::histogram::Histogram<u64>> for CumulativeDatum {
    type Error = Error;
    fn try_from(
        hist: &oximeter_types::histogram::Histogram<u64>,
    ) -> Result<Self, Self::Error> {
        hist.try_into().map(CumulativeDatum::IntegerDistribution)
    }
}

macro_rules! f64_dist_from {
    ($t:ty) => {
        impl From<&oximeter_types::histogram::Histogram<$t>>
            for Distribution<f64>
        {
            fn from(hist: &oximeter_types::histogram::Histogram<$t>) -> Self {
                let (bins, counts) = hist.bins_and_counts();
                Self {
                    bins: bins.into_iter().map(f64::from).collect(),
                    counts,
                    min: Some(f64::from(hist.min())),
                    max: Some(f64::from(hist.max())),
                    sum_of_samples: hist.sum_of_samples() as f64,
                    squared_mean: hist.squared_mean(),
                    p50: Some(hist.p50q()),
                    p90: Some(hist.p90q()),
                    p99: Some(hist.p99q()),
                }
            }
        }

        impl From<&oximeter_types::histogram::Histogram<$t>>
            for CumulativeDatum
        {
            fn from(hist: &oximeter_types::histogram::Histogram<$t>) -> Self {
                CumulativeDatum::DoubleDistribution(hist.into())
            }
        }
    };
}

f64_dist_from!(f32);
f64_dist_from!(f64);

#[cfg(test)]
mod tests {
    use super::{Distribution, MetricType, Points, Values};
    use crate::point::{DataType, Datum, ValueArray, push_concrete_values};
    use chrono::{DateTime, Utc};
    use oximeter_types::{
        Measurement, Quantile, histogram::Record, types::Cumulative,
    };
    use std::time::Duration;

    #[test]
    fn test_point_delta_between() {
        let mut datum = Cumulative::new(2i64);
        let now = Utc::now();
        let meas0 = Measurement::new(now + Duration::from_secs(1), datum);
        datum.set(10i64);
        let meas1 = Measurement::new(now + Duration::from_secs(2), datum);
        let measurements = vec![meas0.clone(), meas1.clone()];
        let points = Points::delta_from_cumulative(&measurements).unwrap();

        assert_eq!(points.len(), 2);
        assert_eq!(
            points.values(0).unwrap().as_integer().unwrap(),
            &[Some(2i64), Some(8)],
        );
        assert_eq!(
            Duration::from_secs(1),
            (points.timestamps[1] - points.timestamps[0]).to_std().unwrap(),
        );
        let expected = vec![now, meas0.timestamp()];
        let actual = points.start_times.as_ref().unwrap();
        assert_eq!(expected.len(), actual.len());
        for (x, y) in expected.into_iter().zip(actual.into_iter()) {
            assert!((*y - x).num_nanoseconds().unwrap() <= 1);
        }
    }

    #[test]
    fn test_point_delta_between_with_new_epoch() {
        let datum = Cumulative::new(2i64);
        let now = Utc::now();
        let meas0 = Measurement::new(now + Duration::from_secs(1), datum);

        // Create a new datum, with a completely new start time, representing a
        // new epoch.
        let now = Utc::now() + Duration::from_secs(10);
        let datum = Cumulative::with_start_time(now, 10i64);
        let meas1 = Measurement::new(now + Duration::from_secs(2), datum);
        let measurements = vec![meas0.clone(), meas1.clone()];
        let points = Points::delta_from_cumulative(&measurements).unwrap();

        // The second point should not be referenced to the first, because
        // they're in different epochs.
        assert_eq!(points.len(), 2);
        assert_eq!(
            points.values(0).unwrap().as_integer().unwrap(),
            &[Some(2i64), Some(10)],
        );

        // The start times should be the start times of the measurements
        // themselves as well. Same for timestamps.
        assert_eq!(
            points.timestamps,
            vec![meas0.timestamp(), meas1.timestamp()],
        );
        assert_eq!(
            points.start_times.as_ref().unwrap(),
            &[meas0.start_time().unwrap(), meas1.start_time().unwrap()],
        );
    }

    #[test]
    fn test_point_delta_between_overlapping_time_ranges() {
        // These data points start at `T` and `T + 100ms` respectively, and end
        // at those times + 1s. That means their time ranges overlap, and so we
        // can't compute a delta from them.
        let start_time = Utc::now() - Duration::from_secs(1);
        let datum1 = Cumulative::with_start_time(start_time, 1i64);
        let datum2 = Cumulative::with_start_time(
            start_time + Duration::from_millis(100),
            10i64,
        );
        let meas1 = Measurement::new(
            datum1.start_time() + Duration::from_secs(1),
            datum1,
        );
        let meas2 = Measurement::new(
            datum2.start_time() + Duration::from_secs(1),
            datum2,
        );

        assert!(
            Points::delta_from_cumulative(&[meas1.clone(), meas2.clone()])
                .is_err(),
            "Should not be able to compute a delta point \
            between two measuremenst with overlapping start \
            times: [{}, {}] and [{}, {}]",
            meas1.start_time().unwrap(),
            meas1.timestamp(),
            meas2.start_time().unwrap(),
            meas2.timestamp(),
        );
    }

    #[test]
    fn test_sub_between_histogram_distributions() {
        let now = Utc::now();
        let current1 = now + Duration::from_secs(1);
        let mut hist1 =
            oximeter_types::histogram::Histogram::new(&[0i64, 10, 20]).unwrap();
        hist1.sample(1).unwrap();
        hist1.set_start_time(current1);
        let current2 = now + Duration::from_secs(2);
        let mut hist2 =
            oximeter_types::histogram::Histogram::new(&[0i64, 10, 20]).unwrap();
        hist2.sample(5).unwrap();
        hist2.sample(10).unwrap();
        hist2.sample(15).unwrap();
        hist2.set_start_time(current2);
        let dist1 = Distribution::from(&hist1);
        let dist2 = Distribution::from(&hist2);

        let diff = dist2.checked_sub(&dist1).unwrap();
        assert_eq!(diff.bins(), &[i64::MIN, 0, 10, 20]);
        assert_eq!(diff.counts(), &[0, 0, 2, 0]);
        assert_eq!(diff.n_samples(), 2);
        assert!(diff.min().is_none());
        assert!(diff.max().is_none());
        assert_eq!(diff.mean(), 14.5);
        assert_eq!(diff.std_dev(), Some(6.363961030678928));
        assert_eq!(diff.sample_std_dev(), Some(9.0));
        assert!(diff.p50.is_none());
        assert!(diff.p90.is_none());
        assert!(diff.p99.is_none());
    }

    fn timestamps(n: usize) -> Vec<DateTime<Utc>> {
        let now = Utc::now();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(now - Duration::from_secs(i as _));
        }
        out.into_iter().rev().collect()
    }

    #[test]
    fn test_cast_points_from_bool() {
        let points = Points {
            start_times: None,
            timestamps: timestamps(2),
            values: vec![Values {
                values: ValueArray::Boolean(vec![Some(false), Some(true)]),
                metric_type: MetricType::Gauge,
            }],
        };

        let as_same = points.cast(&[DataType::Boolean]).unwrap();
        let vals = as_same.values[0].values.as_boolean().unwrap();
        assert_eq!(vals, points.values[0].values.as_boolean().unwrap());

        let as_int = points.cast(&[DataType::Integer]).unwrap();
        let vals = as_int.values[0].values.as_integer().unwrap();
        assert_eq!(vals, &vec![Some(0), Some(1)]);

        let as_double = points.cast(&[DataType::Double]).unwrap();
        let vals = as_double.values[0].values.as_double().unwrap();
        assert_eq!(vals, &vec![Some(0.0), Some(1.0)]);

        let as_string = points.cast(&[DataType::String]).unwrap();
        let vals = as_string.values[0].values.as_string().unwrap();
        assert_eq!(
            vals,
            &vec![Some("false".to_string()), Some("true".to_string())]
        );

        for ty in [DataType::IntegerDistribution, DataType::DoubleDistribution]
        {
            assert!(
                points.cast(&[ty]).is_err(),
                "Should not be able to cast bool array to distributions"
            );
        }
        assert!(points.cast(&[]).is_err(), "Should fail to cast with no types");
        assert!(
            points.cast(&[DataType::Boolean, DataType::Boolean]).is_err(),
            "Should fail to cast to the wrong number of types"
        );
    }

    #[test]
    fn test_cast_points_from_integer() {
        let points = Points {
            start_times: None,
            timestamps: timestamps(2),
            values: vec![Values {
                values: ValueArray::Integer(vec![Some(0), Some(10)]),
                metric_type: MetricType::Gauge,
            }],
        };

        let as_same = points.cast(&[DataType::Integer]).unwrap();
        let vals = as_same.values[0].values.as_integer().unwrap();
        assert_eq!(vals, points.values[0].values.as_integer().unwrap());

        let as_bools = points.cast(&[DataType::Boolean]).unwrap();
        let vals = as_bools.values[0].values.as_boolean().unwrap();
        assert_eq!(vals, &vec![Some(false), Some(true)]);

        let as_double = points.cast(&[DataType::Double]).unwrap();
        let vals = as_double.values[0].values.as_double().unwrap();
        assert_eq!(vals, &vec![Some(0.0), Some(10.0)]);

        let as_string = points.cast(&[DataType::String]).unwrap();
        let vals = as_string.values[0].values.as_string().unwrap();
        assert_eq!(vals, &vec![Some("0".to_string()), Some("10".to_string())]);

        for ty in [DataType::IntegerDistribution, DataType::DoubleDistribution]
        {
            assert!(
                points.cast(&[ty]).is_err(),
                "Should not be able to cast int array to distributions"
            );
        }
        assert!(points.cast(&[]).is_err(), "Should fail to cast with no types");
        assert!(
            points.cast(&[DataType::Boolean, DataType::Boolean]).is_err(),
            "Should fail to cast to the wrong number of types"
        );
    }

    #[test]
    fn test_cast_points_from_double() {
        let points = Points {
            start_times: None,
            timestamps: timestamps(2),
            values: vec![Values {
                values: ValueArray::Double(vec![Some(0.0), Some(10.5)]),
                metric_type: MetricType::Gauge,
            }],
        };

        let as_same = points.cast(&[DataType::Double]).unwrap();
        let vals = as_same.values[0].values.as_double().unwrap();
        assert_eq!(vals, points.values[0].values.as_double().unwrap());

        let as_bools = points.cast(&[DataType::Boolean]).unwrap();
        let vals = as_bools.values[0].values.as_boolean().unwrap();
        assert_eq!(vals, &vec![Some(false), Some(true)]);

        let as_ints = points.cast(&[DataType::Integer]).unwrap();
        let vals = as_ints.values[0].values.as_integer().unwrap();
        assert_eq!(vals, &vec![Some(0), Some(10)]);

        let as_string = points.cast(&[DataType::String]).unwrap();
        let vals = as_string.values[0].values.as_string().unwrap();
        assert_eq!(
            vals,
            &vec![Some("0".to_string()), Some("10.5".to_string())]
        );

        let points = Points {
            start_times: None,
            timestamps: timestamps(2),
            values: vec![Values {
                values: ValueArray::Double(vec![Some(0.0), Some(f64::MAX)]),
                metric_type: MetricType::Gauge,
            }],
        };
        assert!(
            points.cast(&[DataType::Integer]).is_err(),
            "Should fail to cast out-of-range doubles to integer"
        );

        for ty in [DataType::IntegerDistribution, DataType::DoubleDistribution]
        {
            assert!(
                points.cast(&[ty]).is_err(),
                "Should not be able to cast double array to distributions"
            );
        }
        assert!(points.cast(&[]).is_err(), "Should fail to cast with no types");
        assert!(
            points.cast(&[DataType::Boolean, DataType::Boolean]).is_err(),
            "Should fail to cast to the wrong number of types"
        );
    }

    #[test]
    fn test_cast_points_from_string() {
        fn make_points(strings: &[&str]) -> Points {
            Points {
                start_times: None,
                timestamps: timestamps(strings.len()),
                values: vec![Values {
                    values: ValueArray::String(
                        strings.iter().map(|&s| Some(s.into())).collect(),
                    ),
                    metric_type: MetricType::Gauge,
                }],
            }
        }

        let points = make_points(&["some", "strings"]);
        let as_same = points.cast(&[DataType::String]).unwrap();
        assert_eq!(as_same, points);

        // Any non-empty string is truthy, even "false".
        let points = make_points(&["", "false", "true"]);
        let as_bools = points.cast(&[DataType::Boolean]).unwrap();
        let vals = as_bools.values[0].values.as_boolean().unwrap();
        assert_eq!(vals, &vec![Some(false), Some(true), Some(true)]);

        // Conversion to integers happens by parsing.
        let points = make_points(&["0", "1"]);
        let as_ints = points.cast(&[DataType::Integer]).unwrap();
        let vals = as_ints.values[0].values.as_integer().unwrap();
        assert_eq!(vals, &vec![Some(0), Some(1)]);
        for bad in ["1.0", "", "foo", "[]"] {
            assert!(
                make_points(&[bad]).cast(&[DataType::Integer]).is_err(),
                "Should fail to cast non-int string '{}' to integers",
                bad,
            );
        }

        // Conversion to doubles happens by parsing.
        let points = make_points(&["0", "1.1"]);
        let as_doubles = points.cast(&[DataType::Double]).unwrap();
        let vals = as_doubles.values[0].values.as_double().unwrap();
        assert_eq!(vals, &vec![Some(0.0), Some(1.1)]);
        for bad in ["", "foo", "[]"] {
            assert!(
                make_points(&[bad]).cast(&[DataType::Double]).is_err(),
                "Should fail to cast non-double string '{}' to double",
                bad,
            );
        }

        // Checks for invalid casts
        for ty in [DataType::IntegerDistribution, DataType::DoubleDistribution]
        {
            assert!(
                points.cast(&[ty]).is_err(),
                "Should not be able to cast double array to distributions"
            );
        }
        assert!(points.cast(&[]).is_err(), "Should fail to cast with no types");
        assert!(
            points.cast(&[DataType::Boolean, DataType::Boolean]).is_err(),
            "Should fail to cast to the wrong number of types"
        );
    }

    #[test]
    fn test_cast_points_from_int_distribution() {
        // We can only "cast" to the same type here.
        let points = Points {
            start_times: None,
            timestamps: timestamps(1),
            values: vec![Values {
                values: ValueArray::IntegerDistribution(vec![Some(
                    Distribution {
                        bins: vec![0, 1, 2],
                        counts: vec![0; 3],
                        min: Some(0),
                        max: Some(2),
                        sum_of_samples: 0,
                        squared_mean: 0.0,
                        p50: Some(Quantile::p50()),
                        p90: Some(Quantile::p90()),
                        p99: Some(Quantile::p99()),
                    },
                )]),
                metric_type: MetricType::Gauge,
            }],
        };
        let as_same = points.cast(&[DataType::IntegerDistribution]).unwrap();
        assert_eq!(points, as_same);

        for ty in [
            DataType::Boolean,
            DataType::String,
            DataType::Integer,
            DataType::Double,
            DataType::DoubleDistribution,
        ] {
            assert!(
                points.cast(&[ty]).is_err(),
                "Should not be able to cast distributions to anything other than itself"
            );
        }
        assert!(points.cast(&[]).is_err());
        assert!(
            points
                .cast(&[
                    DataType::IntegerDistribution,
                    DataType::IntegerDistribution
                ])
                .is_err()
        );
    }

    #[test]
    fn test_cast_points_from_double_distribution() {
        // We can only "cast" to the same type here.
        let points = Points {
            start_times: None,
            timestamps: timestamps(1),
            values: vec![Values {
                values: ValueArray::DoubleDistribution(vec![Some(
                    Distribution {
                        bins: vec![0.0, 1.0, 2.0],
                        counts: vec![0; 3],
                        min: Some(0.0),
                        max: Some(2.0),
                        sum_of_samples: 0.0,
                        squared_mean: 0.0,
                        p50: Some(Quantile::p50()),
                        p90: Some(Quantile::p90()),
                        p99: Some(Quantile::p99()),
                    },
                )]),
                metric_type: MetricType::Gauge,
            }],
        };
        let as_same = points.cast(&[DataType::DoubleDistribution]).unwrap();
        assert_eq!(points, as_same);

        for ty in [
            DataType::Boolean,
            DataType::String,
            DataType::Integer,
            DataType::Double,
            DataType::IntegerDistribution,
        ] {
            assert!(
                points.cast(&[ty]).is_err(),
                "Should not be able to cast distributions to anything other than itself"
            );
        }
        assert!(points.cast(&[]).is_err());
        assert!(
            points
                .cast(&[
                    DataType::DoubleDistribution,
                    DataType::DoubleDistribution
                ])
                .is_err()
        );
    }

    #[test]
    fn test_push_concrete_values() {
        let mut points = Points::with_capacity(
            2,
            [DataType::Integer, DataType::Double].into_iter(),
            [MetricType::Gauge, MetricType::Gauge].into_iter(),
        )
        .unwrap();

        // Push a concrete value for the integer dimension
        let from_ints = vec![Values {
            values: ValueArray::Integer(vec![Some(1)]),
            metric_type: MetricType::Gauge,
        }];
        push_concrete_values(&mut points.values[..1], &from_ints, 0);

        // And another for the double dimension.
        let from_doubles = vec![Values {
            values: ValueArray::Double(vec![Some(2.0)]),
            metric_type: MetricType::Gauge,
        }];
        push_concrete_values(&mut points.values[1..], &from_doubles, 0);

        assert_eq!(
            points.dimensionality(),
            2,
            "Points should have 2 dimensions",
        );
        let ints = points.values[0].values.as_integer().unwrap();
        assert_eq!(
            ints.len(),
            1,
            "Should have pushed one point in the first dimension"
        );
        assert_eq!(
            ints[0],
            Some(1),
            "Should have pushed 1 onto the first dimension"
        );
        let doubles = points.values[1].values.as_double().unwrap();
        assert_eq!(
            doubles.len(),
            1,
            "Should have pushed one point in the second dimension"
        );
        assert_eq!(
            doubles[0],
            Some(2.0),
            "Should have pushed 2.0 onto the second dimension"
        );
    }

    #[test]
    fn test_join_point_arrays() {
        let now = Utc::now();

        // Create a set of integer points to join with.
        //
        // This will have two timestamps, one of which will match the points
        // below that are merged in.
        let int_points = Points {
            start_times: None,
            timestamps: vec![
                now - Duration::from_secs(3),
                now - Duration::from_secs(2),
                now,
            ],
            values: vec![Values {
                values: ValueArray::Integer(vec![Some(1), Some(2), Some(3)]),
                metric_type: MetricType::Gauge,
            }],
        };

        // Create an additional set of double points.
        //
        // This also has two timepoints, one of which matches with the above,
        // and one of which does not.
        let double_points = Points {
            start_times: None,
            timestamps: vec![
                now - Duration::from_secs(3),
                now - Duration::from_secs(1),
                now,
            ],
            values: vec![Values {
                values: ValueArray::Double(vec![
                    Some(4.0),
                    Some(5.0),
                    Some(6.0),
                ]),
                metric_type: MetricType::Gauge,
            }],
        };

        // Merge the arrays.
        let merged = int_points.inner_join(&double_points).unwrap();

        // Basic checks that we merged in the right values and have the right
        // types and dimensions.
        assert_eq!(
            merged.dimensionality(),
            2,
            "Should have appended the dimensions from each input array"
        );
        assert_eq!(merged.len(), 2, "Should have merged two common points",);
        assert_eq!(
            merged.data_types().collect::<Vec<_>>(),
            &[DataType::Integer, DataType::Double],
            "Should have combined the data types of the input arrays"
        );
        assert_eq!(
            merged.metric_types().collect::<Vec<_>>(),
            &[MetricType::Gauge, MetricType::Gauge],
            "Should have combined the metric types of the input arrays"
        );

        // Check the actual values of the array.
        let mut points = merged.iter_points();

        // The first and last timepoint overlapped between the two arrays, so we
        // should have both of them as concrete samples.
        let pt = points.next().unwrap();
        assert_eq!(pt.start_time, None, "Gauges don't have a start time");
        assert_eq!(
            *pt.timestamp, int_points.timestamps[0],
            "Should have taken the first input timestamp from both arrays",
        );
        assert_eq!(
            *pt.timestamp, double_points.timestamps[0],
            "Should have taken the first input timestamp from both arrays",
        );
        let values = pt.values;
        assert_eq!(values.len(), 2, "Should have 2 dimensions");
        assert_eq!(
            &values[0],
            &(Datum::Integer(Some(&1)), MetricType::Gauge),
            "Should have pulled value from first integer array."
        );
        assert_eq!(
            &values[1],
            &(Datum::Double(Some(&4.0)), MetricType::Gauge),
            "Should have pulled value from second double array."
        );

        // And the next point
        let pt = points.next().unwrap();
        assert_eq!(pt.start_time, None, "Gauges don't have a start time");
        assert_eq!(
            *pt.timestamp, int_points.timestamps[2],
            "Should have taken the input timestamp from both arrays",
        );
        assert_eq!(
            *pt.timestamp, double_points.timestamps[2],
            "Should have taken the input timestamp from both arrays",
        );
        let values = pt.values;
        assert_eq!(values.len(), 2, "Should have 2 dimensions");
        assert_eq!(
            &values[0],
            &(Datum::Integer(Some(&3)), MetricType::Gauge),
            "Should have pulled value from first integer array."
        );
        assert_eq!(
            &values[1],
            &(Datum::Double(Some(&6.0)), MetricType::Gauge),
            "Should have pulled value from second double array."
        );

        // And there should be no other values.
        assert!(points.next().is_none(), "There should be no more points");
    }
}
