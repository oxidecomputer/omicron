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

use crate::distribution;
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

/// A measurement is a single sample of a metric.
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum Measurement {
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Bytes),
    CumulativeI64(Cumulative<i64>),
    CumulativeF64(Cumulative<f64>),
    DistributionI64(distribution::Distribution<i64>),
    DistributionF64(distribution::Distribution<f64>),
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
            Measurement::DistributionI64(_) => MeasurementType::DistributionI64,
            Measurement::DistributionF64(_) => MeasurementType::DistributionF64,
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

impl From<distribution::Distribution<i64>> for Measurement {
    fn from(value: distribution::Distribution<i64>) -> Measurement {
        Measurement::DistributionI64(value)
    }
}

impl From<distribution::Distribution<f64>> for Measurement {
    fn from(value: distribution::Distribution<f64>) -> Measurement {
        Measurement::DistributionF64(value)
    }
}

/// Errors related to the generation or collection of metrics.
#[derive(Debug, Clone, Error)]
pub enum Error {
    /// An error occurred registering or building a collection of metrics.
    #[error("Error building metric collection: {0}")]
    InvalidCollection(String),
    /// A collection of metrics is already registered.
    #[error("The metric collection is already registered")]
    CollectionAlreadyRegistered,
    /// A collection of metrics is not registered.
    #[error("The collection is not registered")]
    CollectionNotRegistered,
    /// An error occurred calling the registered
    /// [`Producer::setup_collection`](crate::producer::Producer::setup_collection) method.
    #[error("Failed to set up collection of metric: {0}")]
    CollectionSetupFailed(String),
    /// An error occurred calling the registered
    /// [`Producer::collect`](crate::producer::Producer::collect) method.
    #[error("Error collecting measurement: {0}")]
    MeasurementError(String),
    /// The [`Producer::collect`](crate::producer::Producer::collect) method return an unexpected
    /// measurement type for a metric.
    #[error("The producer function returned an unexpected type, expected {0:?}, found {1:?})")]
    ProducerTypeMismatch(MeasurementType, MeasurementType),
    /// An error related to creating or sampling a [`distribution::Distribution`] metric.
    #[error("{0}")]
    DistributionError(#[from] distribution::DistributionError),
}

/// A cumulative or counter data type.
#[derive(Debug, Clone, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct Cumulative<T>(T);

impl<T> Cumulative<T>
where
    T: traits::DataPoint + Add + AddAssign + Copy + One + Zero,
{
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn increment(&mut self) {
        self.0 += One::one();
    }

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
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
pub struct Target {
    pub name: String,
    pub key: String,
    pub field_names: Vec<String>,
    pub field_types: Vec<FieldType>,
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
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
pub struct Metric {
    pub name: String,
    pub key: String,
    pub field_names: Vec<String>,
    pub field_types: Vec<FieldType>,
    pub field_values: Vec<FieldValue>,
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
    pub timestamp: DateTime<Utc>,
    pub target: Target,
    pub metric: Metric,
    pub measurement: Measurement,
}

impl Sample {
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

    use super::distribution::Distribution;
    use super::{Cumulative, Measurement};
    use crate::{Target, Metric};
    use crate::types;

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
            Measurement::from(Distribution::new(&[0i64, 10]).unwrap()),
            Measurement::DistributionI64(_)
        ));
        assert!(matches!(
            Measurement::from(Distribution::new(&[0f64, 10.0]).unwrap()),
            Measurement::DistributionF64(_)
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
