// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Definition of data points for OxQL.

// Copyright 2024 Oxide Computer Company

use super::Error;
use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use num::ToPrimitive;
use oximeter::DatumType;
use oximeter::Measurement;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

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
    pub fn is_numeric(&self) -> bool {
        matches!(self, DataType::Integer | DataType::Double)
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
            oximeter::Datum::CumulativeI64(val) => {
                CumulativeDatum::Integer(val.value())
            }
            oximeter::Datum::CumulativeU64(val) => {
                let int = val
                    .value()
                    .try_into()
                    .context("Overflow converting u64 to i64")?;
                CumulativeDatum::Integer(int)
            }
            oximeter::Datum::CumulativeF32(val) => {
                CumulativeDatum::Double(val.value().into())
            }
            oximeter::Datum::CumulativeF64(val) => {
                CumulativeDatum::Double(val.value())
            }
            oximeter::Datum::HistogramI8(hist) => hist.into(),
            oximeter::Datum::HistogramU8(hist) => hist.into(),
            oximeter::Datum::HistogramI16(hist) => hist.into(),
            oximeter::Datum::HistogramU16(hist) => hist.into(),
            oximeter::Datum::HistogramI32(hist) => hist.into(),
            oximeter::Datum::HistogramU32(hist) => hist.into(),
            oximeter::Datum::HistogramI64(hist) => hist.into(),
            oximeter::Datum::HistogramU64(hist) => hist.try_into()?,
            oximeter::Datum::HistogramF32(hist) => hist.into(),
            oximeter::Datum::HistogramF64(hist) => hist.into(),
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
    // The data values.
    pub(super) values: ValueArray,
    // The type of this metric.
    pub(super) metric_type: MetricType,
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

impl<'a> fmt::Display for Point<'a> {
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

impl<'a> Point<'a> {
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

impl<'a> fmt::Display for Datum<'a> {
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
    pub(super) start_times: Option<Vec<DateTime<Utc>>>,
    // The timestamp of each value.
    pub(super) timestamps: Vec<DateTime<Utc>>,
    // The array of data values, one for each dimension.
    pub(super) values: Vec<Values>,
}

impl Points {
    /// Construct an empty array of points to hold data of the provided type.
    pub fn empty(data_type: DataType, metric_type: MetricType) -> Self {
        Self::with_capacity(
            0,
            std::iter::once(data_type),
            std::iter::once(metric_type),
        )
        .unwrap()
    }

    // Return a mutable reference to the value array of the specified dimension, if any.
    pub(super) fn values_mut(&mut self, dim: usize) -> Option<&mut ValueArray> {
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
        if types.all(|ty| ty == first_type) {
            Some(first_type)
        } else {
            None
        }
    }

    /// Construct a list of gauge points from a list of gauge measurements.
    ///
    /// An error is returned if the provided
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

    // Filter points in self to those where `to_keep` is true.
    pub(crate) fn filter(&self, to_keep: Vec<bool>) -> Result<Points, Error> {
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

    // Return a new set of points, with the values casted to the provided types.
    pub(crate) fn cast(&self, types: &[DataType]) -> Result<Self, Error> {
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
                    ValueArray::Integer(new)
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
                            Some(d) if d == 0.0 => new.push(Some(false)),
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

    /// Return true if self contains no data points.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    // Return the data type in self.
    pub(super) fn data_type(&self) -> DataType {
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

    // Access the inner array of integers, if possible.
    pub(super) fn as_integer_mut(
        &mut self,
    ) -> Result<&mut Vec<Option<i64>>, Error> {
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
            | DatumType::HistogramU64
            | DatumType::HistogramF32
            | DatumType::HistogramF64 => {
                self.as_integer_distribution_mut()?.push(None)
            }
        }
        Ok(())
    }

    // Push a value directly from a datum, without modification.
    fn push_value_from_datum(
        &mut self,
        datum: &oximeter::Datum,
    ) -> Result<(), Error> {
        match datum {
            oximeter::Datum::Bool(b) => self.as_boolean_mut()?.push(Some(*b)),
            oximeter::Datum::I8(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter::Datum::U8(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter::Datum::I16(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter::Datum::U16(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter::Datum::I32(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter::Datum::U32(i) => {
                self.as_integer_mut()?.push(Some(i64::from(*i)))
            }
            oximeter::Datum::I64(i) => self.as_integer_mut()?.push(Some(*i)),
            oximeter::Datum::U64(i) => {
                let i =
                    i.to_i64().context("Failed to convert u64 datum to i64")?;
                self.as_integer_mut()?.push(Some(i));
            }
            oximeter::Datum::F32(f) => {
                self.as_double_mut()?.push(Some(f64::from(*f)))
            }
            oximeter::Datum::F64(f) => self.as_double_mut()?.push(Some(*f)),
            oximeter::Datum::String(s) => {
                self.as_string_mut()?.push(Some(s.clone()))
            }
            oximeter::Datum::Bytes(_) => {
                anyhow::bail!("Bytes data types are not yet supported")
            }
            oximeter::Datum::CumulativeI64(c) => {
                self.as_integer_mut()?.push(Some(c.value()))
            }
            oximeter::Datum::CumulativeU64(c) => {
                let c = c
                    .value()
                    .to_i64()
                    .context("Failed to convert u64 datum to i64")?;
                self.as_integer_mut()?.push(Some(c));
            }
            oximeter::Datum::CumulativeF32(c) => {
                self.as_double_mut()?.push(Some(f64::from(c.value())))
            }
            oximeter::Datum::CumulativeF64(c) => {
                self.as_double_mut()?.push(Some(c.value()))
            }
            oximeter::Datum::HistogramI8(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::HistogramU8(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::HistogramI16(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::HistogramU16(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::HistogramI32(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::HistogramU32(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::HistogramI64(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::HistogramU64(h) => self
                .as_integer_distribution_mut()?
                .push(Some(Distribution::try_from(h)?)),
            oximeter::Datum::HistogramF32(h) => self
                .as_double_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::HistogramF64(h) => self
                .as_double_distribution_mut()?
                .push(Some(Distribution::from(h))),
            oximeter::Datum::Missing(missing) => {
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
        new_datum: &oximeter::Datum,
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
                        oximeter::Datum::I8(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::U8(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::I16(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::U16(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::I32(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::U32(new),
                    ) => {
                        let new = i64::from(*new);
                        self.as_integer_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::I64(new),
                    ) => {
                        let diff = new
                            .checked_sub(*last)
                            .context("Overflow computing deltas")?;
                        self.as_integer_mut()?.push(Some(diff));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::U64(new),
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
                        oximeter::Datum::F32(new),
                    ) => {
                        self.as_double_mut()?
                            .push(Some(f64::from(*new) - last));
                    }
                    (
                        CumulativeDatum::Double(last),
                        oximeter::Datum::F64(new),
                    ) => {
                        self.as_double_mut()?.push(Some(new - last));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::CumulativeI64(new),
                    ) => {
                        let new = new.value();
                        let diff = new
                            .checked_sub(*last)
                            .context("Overflow computing deltas")?;
                        self.as_integer_mut()?.push(Some(diff));
                    }
                    (
                        CumulativeDatum::Integer(last),
                        oximeter::Datum::CumulativeU64(new),
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
                        oximeter::Datum::CumulativeF32(new),
                    ) => {
                        self.as_double_mut()?
                            .push(Some(f64::from(new.value()) - last));
                    }
                    (
                        CumulativeDatum::Double(last),
                        oximeter::Datum::CumulativeF64(new),
                    ) => {
                        self.as_double_mut()?.push(Some(new.value() - last));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter::Datum::HistogramI8(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter::Datum::HistogramU8(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter::Datum::HistogramI16(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter::Datum::HistogramU16(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter::Datum::HistogramI32(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter::Datum::HistogramU32(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter::Datum::HistogramI64(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::IntegerDistribution(last),
                        oximeter::Datum::HistogramU64(new),
                    ) => {
                        let new = Distribution::try_from(new)?;
                        self.as_integer_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::DoubleDistribution(last),
                        oximeter::Datum::HistogramF32(new),
                    ) => {
                        let new = Distribution::from(new);
                        self.as_double_distribution_mut()?
                            .push(Some(new.checked_sub(&last)?));
                    }
                    (
                        CumulativeDatum::DoubleDistribution(last),
                        oximeter::Datum::HistogramF64(new),
                    ) => {
                        let new = Distribution::from(new);
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

/// A distribution is a sequence of bins and counts in those bins.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[schemars(rename = "Distribution{T}")]
pub struct Distribution<T: DistributionSupport> {
    bins: Vec<T>,
    counts: Vec<u64>,
}

impl<T: DistributionSupport> fmt::Display for Distribution<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let elems = self
            .bins
            .iter()
            .zip(self.counts.iter())
            .map(|(bin, count)| format!("{bin}: {count}"))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}", elems)
    }
}

impl<T: DistributionSupport> Distribution<T> {
    // Subtract two distributions, checking that they have the same bins.
    fn checked_sub(
        &self,
        rhs: &Distribution<T>,
    ) -> Result<Distribution<T>, Error> {
        anyhow::ensure!(
            self.bins == rhs.bins,
            "Cannot subtract distributions with different bins",
        );
        let counts = self
            .counts
            .iter()
            .zip(rhs.counts.iter().copied())
            .map(|(x, y)| x.checked_sub(y))
            .collect::<Option<_>>()
            .context("Underflow subtracting distributions values")?;
        Ok(Self { bins: self.bins.clone(), counts })
    }

    /// Return the slice of bins.
    pub fn bins(&self) -> &[T] {
        &self.bins
    }

    /// Return the slice of counts.
    pub fn counts(&self) -> &[u64] {
        &self.counts
    }

    /// Return an iterator over each bin and count.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&T, &u64)> + '_ {
        self.bins.iter().zip(self.counts.iter())
    }
}

macro_rules! i64_dist_from {
    ($t:ty) => {
        impl From<&oximeter::histogram::Histogram<$t>> for Distribution<i64> {
            fn from(hist: &oximeter::histogram::Histogram<$t>) -> Self {
                let (bins, counts) = hist.to_arrays();
                Self { bins: bins.into_iter().map(i64::from).collect(), counts }
            }
        }

        impl From<&oximeter::histogram::Histogram<$t>> for CumulativeDatum {
            fn from(hist: &oximeter::histogram::Histogram<$t>) -> Self {
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

impl TryFrom<&oximeter::histogram::Histogram<u64>> for Distribution<i64> {
    type Error = Error;
    fn try_from(
        hist: &oximeter::histogram::Histogram<u64>,
    ) -> Result<Self, Self::Error> {
        let (bins, counts) = hist.to_arrays();
        let bins = bins
            .into_iter()
            .map(i64::try_from)
            .collect::<Result<_, _>>()
            .context("Overflow converting u64 to i64")?;
        Ok(Self { bins, counts })
    }
}

impl TryFrom<&oximeter::histogram::Histogram<u64>> for CumulativeDatum {
    type Error = Error;
    fn try_from(
        hist: &oximeter::histogram::Histogram<u64>,
    ) -> Result<Self, Self::Error> {
        hist.try_into().map(CumulativeDatum::IntegerDistribution)
    }
}

macro_rules! f64_dist_from {
    ($t:ty) => {
        impl From<&oximeter::histogram::Histogram<$t>> for Distribution<f64> {
            fn from(hist: &oximeter::histogram::Histogram<$t>) -> Self {
                let (bins, counts) = hist.to_arrays();
                Self { bins: bins.into_iter().map(f64::from).collect(), counts }
            }
        }

        impl From<&oximeter::histogram::Histogram<$t>> for CumulativeDatum {
            fn from(hist: &oximeter::histogram::Histogram<$t>) -> Self {
                CumulativeDatum::DoubleDistribution(hist.into())
            }
        }
    };
}

f64_dist_from!(f32);
f64_dist_from!(f64);

#[cfg(test)]
mod tests {
    use super::Points;
    use chrono::Utc;
    use oximeter::types::Cumulative;
    use oximeter::Measurement;
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
}
