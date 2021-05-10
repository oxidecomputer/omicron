//! Types used to describe targets, metrics, and measurements.
// Copyright 2021 Oxide Computer Company

use std::boxed::Box;
use std::net::IpAddr;
use std::ops::{Add, AddAssign};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use num_traits::identities::{One, Zero};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::histogram;
use crate::traits;
use crate::MeasurementType;

/// The `FieldType` identifies the type of a target or metric field.
#[derive(Clone, Copy, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum FieldType {
    String,
    I64,
    IpAddr,
    Uuid,
    Bool,
}

/// The `FieldValue` contains the value of a target or metric field.
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum FieldValue {
    String(String),
    I64(i64),
    IpAddr(IpAddr),
    Uuid(Uuid),
    Bool(bool),
}

impl From<i64> for FieldValue {
    fn from(value: i64) -> Self {
        FieldValue::I64(value)
    }
}

impl From<String> for FieldValue {
    fn from(value: String) -> Self {
        FieldValue::String(value)
    }
}

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        FieldValue::String(String::from(value))
    }
}

impl From<IpAddr> for FieldValue {
    fn from(value: IpAddr) -> Self {
        FieldValue::IpAddr(value)
    }
}

impl From<Uuid> for FieldValue {
    fn from(value: Uuid) -> Self {
        FieldValue::Uuid(value)
    }
}

impl From<bool> for FieldValue {
    fn from(value: bool) -> Self {
        FieldValue::Bool(value)
    }
}

/// A measurement is a single sampled data point from a metric.
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum Measurement {
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Bytes),
    CumulativeI64(Cumulative<i64>),
    CumulativeF64(Cumulative<f64>),
    HistogramI64(histogram::Histogram<i64>),
    HistogramF64(histogram::Histogram<f64>),
}

impl Measurement {
    /// Return the [`MeasurementType`] for this measurement.
    pub fn measurement_type(&self) -> MeasurementType {
        match self {
            Measurement::Bool(_) => MeasurementType::Bool,
            Measurement::I64(_) => MeasurementType::I64,
            Measurement::F64(_) => MeasurementType::F64,
            Measurement::String(_) => MeasurementType::String,
            Measurement::Bytes(_) => MeasurementType::Bytes,
            Measurement::CumulativeI64(_) => MeasurementType::CumulativeI64,
            Measurement::CumulativeF64(_) => MeasurementType::CumulativeF64,
            Measurement::HistogramI64(_) => MeasurementType::HistogramI64,
            Measurement::HistogramF64(_) => MeasurementType::HistogramF64,
        }
    }
}

impl From<bool> for Measurement {
    fn from(value: bool) -> Self {
        Measurement::Bool(value)
    }
}

impl From<i64> for Measurement {
    fn from(value: i64) -> Self {
        Measurement::I64(value)
    }
}

impl From<f64> for Measurement {
    fn from(value: f64) -> Self {
        Measurement::F64(value)
    }
}

impl From<String> for Measurement {
    fn from(value: String) -> Self {
        Measurement::String(value)
    }
}

impl From<&str> for Measurement {
    fn from(value: &str) -> Self {
        Measurement::String(value.to_string())
    }
}

impl From<&Bytes> for Measurement {
    fn from(value: &Bytes) -> Self {
        Measurement::Bytes(value.clone())
    }
}

impl From<Cumulative<i64>> for Measurement {
    fn from(value: Cumulative<i64>) -> Self {
        Measurement::CumulativeI64(value)
    }
}

impl From<Cumulative<f64>> for Measurement {
    fn from(value: Cumulative<f64>) -> Self {
        Measurement::CumulativeF64(value)
    }
}

impl From<histogram::Histogram<i64>> for Measurement {
    fn from(value: histogram::Histogram<i64>) -> Measurement {
        Measurement::HistogramI64(value)
    }
}

impl From<histogram::Histogram<f64>> for Measurement {
    fn from(value: histogram::Histogram<f64>) -> Measurement {
        Measurement::HistogramF64(value)
    }
}

/// Errors related to the generation or collection of metrics.
#[derive(Debug, Clone, Error)]
pub enum Error {
    /// An error related to creating or sampling a [`histogram::Histogram`] metric.
    #[error("{0}")]
    HistogramError(#[from] histogram::HistogramError),
}

/// A cumulative or counter data type.
#[derive(Debug, Clone, Copy, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct Cumulative<T>(T);

impl<T> Cumulative<T>
where
    T: traits::DataPoint + Add + AddAssign + Copy + One + Zero,
{
    /// Construct a new counter with the given initial value.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// Add 1 to the internal counter.
    pub fn increment(&mut self) {
        self.0 += One::one();
    }

    /// Return the current value of the counter.
    pub fn value(&self) -> T {
        self.0
    }
}

impl<T> Add<T> for Cumulative<T>
where
    T: traits::DataPoint + Add + AddAssign + Copy + One + Zero,
{
    type Output = Self;

    fn add(self, other: T) -> Self {
        Self(self.0 + other)
    }
}

impl<T> AddAssign<T> for Cumulative<T>
where
    T: traits::DataPoint + Add + AddAssign + Copy + One + Zero,
{
    fn add_assign(&mut self, other: T) {
        self.0 += other;
    }
}

impl<T> Default for Cumulative<T>
where
    T: traits::DataPoint + Add + AddAssign + Copy + One + Zero,
{
    fn default() -> Self {
        Self(Zero::zero())
    }
}

impl<T> Add for Cumulative<T>
where
    T: traits::DataPoint + Add<Output = T> + AddAssign + Copy,
{
    type Output = Self;
    fn add(self, other: Cumulative<T>) -> Self {
        Self(self.0 + other.0)
    }
}

/// A concrete type carrying information about a target.
///
/// This type is used to "materialize" the information from the [`Target`](crate::traits::Target)
/// interface. It can only be constructed from a type that implements that trait, generally when a
/// [`Producer`](traits::Producer) generates [`Sample`]s for its monitored resources.
///
/// See the [`Target`](crate::traits::Target) trait for more details on each field.
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
pub struct Target {
    /// The name of target.
    pub name: String,

    /// The key for this target, which its name and the value of each field, concatenated with a
    /// `':'` character.
    pub key: String,

    /// The names of this target's fields.
    pub field_names: Vec<String>,

    /// The types of this target's fields.
    pub field_types: Vec<FieldType>,

    /// The values of this target's fields.
    pub field_values: Vec<FieldValue>,
}

impl<T> From<&T> for Target
where
    T: traits::Target,
{
    fn from(target: &T) -> Self {
        Self {
            name: target.name().to_string(),
            key: target.key(),
            field_names: target.field_names().iter().map(|x| x.to_string()).collect(),
            field_types: target.field_types().to_vec(),
            field_values: target.field_values(),
        }
    }
}

/// A concrete type carrying information about a metric.
///
/// This type is used to "materialize" the information from the [`Metric`](crate::traits::Metric)
/// interface. It can only be constructed from a type that implements that trait, generally when a
/// [`Producer`](traits::Producer) generates [`Sample`]s for its monitored resources.
///
/// See the [`Metric`](crate::traits::Metric) trait for more details on each field.
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
pub struct Metric {
    /// The name of target.
    pub name: String,

    /// The key for this metric, which its value of each field and its name, concatenated with a
    /// `':'` character.
    pub key: String,

    /// The names of this metric's fields.
    pub field_names: Vec<String>,

    /// The types of this metric's fields.
    pub field_types: Vec<FieldType>,

    /// The values of this metric's fields.
    pub field_values: Vec<FieldValue>,

    /// The data type of a measurement from this metric.
    pub measurement_type: MeasurementType,
}

impl<M> From<&M> for Metric
where
    M: traits::Metric,
{
    fn from(metric: &M) -> Self {
        Self {
            name: metric.name().to_string(),
            key: metric.key().clone(),
            field_names: metric.field_names().iter().map(|x| x.to_string()).collect(),
            field_types: metric.field_types().to_vec(),
            field_values: metric.field_values(),
            measurement_type: metric.measurement_type(),
        }
    }
}

/// A concrete type representing a single, timestamped measurement from a timeseries.
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
pub struct Sample {
    /// The timestamp for this sample
    pub timestamp: DateTime<Utc>,

    /// The `Target` this sample is derived from.
    pub target: Target,

    /// The `Metric` this sample is derived from.
    pub metric: Metric,

    /// The actual measured data point for this sample.
    pub measurement: Measurement,
}

impl Sample {
    /// Construct a new sample.
    ///
    /// This materializes the data from the target and metric, and stores that information along
    /// with the measurement data itself. Users may optionally specify a timestamp, which defaults
    /// to the current time if `None` is passed.
    pub fn new<T, M, Meas>(
        target: &T,
        metric: &M,
        measurement: Meas,
        timestamp: Option<DateTime<Utc>>,
    ) -> Self
    where
        T: traits::Target,
        M: traits::Metric<Measurement = Meas>,
        Meas: traits::DataPoint + Into<Measurement>,
    {
        Self {
            timestamp: timestamp.unwrap_or_else(Utc::now),
            target: target.into(),
            metric: metric.into(),
            measurement: measurement.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use chrono::Utc;

    use super::histogram::Histogram;
    use super::{Cumulative, Measurement};
    use crate::types;
    use crate::{Metric, Target};

    #[derive(Clone, Target)]
    struct Targ {
        pub good: bool,
        pub id: i64,
    }

    #[crate::metric(i64)]
    #[derive(Clone)]
    struct Met {
        pub good: bool,
        pub id: i64,
    }

    #[test]
    fn test_cumulative_i64() {
        let mut x = Cumulative::<i64>::default();
        assert_eq!(x.value(), 0);
        x.increment();
        assert_eq!(x.value(), 1);
        x += 10;
        assert_eq!(x.value(), 11);
        x = x + 4;
        assert_eq!(x.value(), 15);
    }

    #[test]
    fn test_cumulative_f64() {
        let mut x = Cumulative::<f64>::new(0.0);
        assert_eq!(x.value(), 0.0);
        x.increment();
        assert_eq!(x.value(), 1.0);
        x += 1.0;
        assert_eq!(x.value(), 2.0);
        x = x + 0.5;
        assert_eq!(x.value(), 2.5);
    }

    #[test]
    fn test_measurement() {
        assert!(matches!(Measurement::from(false), Measurement::Bool(_)));
        assert!(matches!(Measurement::from(0i64), Measurement::I64(_)));
        assert!(matches!(Measurement::from(0f64), Measurement::F64(_)));
        assert!(matches!(Measurement::from("foo"), Measurement::String(_)));
        assert!(matches!(
            Measurement::from(&Bytes::new()),
            Measurement::Bytes(_)
        ));
        assert!(matches!(
            Measurement::from(Cumulative::new(0i64)),
            Measurement::CumulativeI64(_)
        ));
        assert!(matches!(
            Measurement::from(Cumulative::new(0f64)),
            Measurement::CumulativeF64(_)
        ));
        assert!(matches!(
            Measurement::from(Histogram::new(&[0i64, 10]).unwrap()),
            Measurement::HistogramI64(_)
        ));
        assert!(matches!(
            Measurement::from(Histogram::new(&[0f64, 10.0]).unwrap()),
            Measurement::HistogramF64(_)
        ));
    }

    #[test]
    fn test_target_struct() {
        let t = Targ { good: false, id: 2 };
        let t2 = types::Target::from(&t);
        assert_eq!(t.name(), t2.name);
        assert_eq!(t.key(), t2.key);
        assert_eq!(t.field_names(), t2.field_names);
        assert_eq!(t.field_types(), t2.field_types);
        assert_eq!(t.field_values(), t2.field_values);
    }

    #[test]
    fn test_metric_struct() {
        let m = Met { good: false, id: 2 };
        let m2 = types::Metric::from(&m);
        assert_eq!(m.name(), m2.name);
        assert_eq!(m.key(), m2.key);
        assert_eq!(m.field_names(), m2.field_names);
        assert_eq!(m.field_types(), m2.field_types);
        assert_eq!(m.field_values(), m2.field_values);
        assert_eq!(m.measurement_type(), m2.measurement_type);
    }

    #[test]
    fn test_sample_struct() {
        let t = Targ { good: false, id: 2 };
        let m = Met { good: false, id: 2 };
        let measurement: i64 = 1;
        let timestamp = Utc::now();
        let sample = types::Sample::new(&t, &m, measurement, Some(timestamp));
        assert_eq!(sample.target.key, t.key());
        assert_eq!(sample.metric.key, m.key());
        assert_eq!(sample.timestamp, timestamp);
        assert_eq!(sample.measurement, Measurement::I64(measurement));
    }
}
