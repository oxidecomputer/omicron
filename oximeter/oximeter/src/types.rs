//! Types used to describe targets, metrics, and measurements.
// Copyright 2021 Oxide Computer Company

use std::boxed::Box;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
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

/// The `FieldType` identifies the type of a target or metric field.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize,
)]
pub enum FieldType {
    String,
    I64,
    IpAddr,
    Uuid,
    Bool,
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The `FieldValue` contains the value of a target or metric field.
#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub enum FieldValue {
    String(String),
    I64(i64),
    IpAddr(IpAddr),
    Uuid(Uuid),
    Bool(bool),
}

impl FieldValue {
    /// Return the type associated with this field
    pub fn field_type(&self) -> FieldType {
        match self {
            FieldValue::String(_) => FieldType::String,
            FieldValue::I64(_) => FieldType::I64,
            FieldValue::IpAddr(_) => FieldType::IpAddr,
            FieldValue::Uuid(_) => FieldType::Uuid,
            FieldValue::Bool(_) => FieldType::Bool,
        }
    }

    /// Parse a field from a string, assuming it is of a certain type. An Err is returned if the
    /// value cannot be parsed as that type.
    pub fn parse_as_type(
        s: &str,
        field_type: FieldType,
    ) -> Result<Self, Error> {
        let make_err =
            || Error::ParseError(s.to_string(), field_type.to_string());
        match field_type {
            FieldType::String => Ok(FieldValue::String(s.to_string())),
            FieldType::I64 => {
                Ok(FieldValue::I64(s.parse().map_err(|_| make_err())?))
            }
            FieldType::IpAddr => {
                Ok(FieldValue::IpAddr(s.parse().map_err(|_| make_err())?))
            }
            FieldType::Uuid => {
                Ok(FieldValue::Uuid(s.parse().map_err(|_| make_err())?))
            }
            FieldType::Bool => {
                Ok(FieldValue::Bool(s.parse().map_err(|_| make_err())?))
            }
        }
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FieldValue::String(ref inner) => write!(f, "{}", inner),
            FieldValue::I64(ref inner) => write!(f, "{}", inner),
            FieldValue::IpAddr(ref inner) => write!(f, "{}", inner),
            FieldValue::Uuid(ref inner) => write!(f, "{}", inner),
            FieldValue::Bool(ref inner) => write!(f, "{}", inner),
        }
    }
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

impl From<Ipv4Addr> for FieldValue {
    fn from(value: Ipv4Addr) -> Self {
        FieldValue::IpAddr(IpAddr::V4(value))
    }
}

impl From<Ipv6Addr> for FieldValue {
    fn from(value: Ipv6Addr) -> Self {
        FieldValue::IpAddr(IpAddr::V6(value))
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

impl<T> From<&T> for FieldValue
where
    T: Clone + Into<FieldValue>,
{
    fn from(value: &T) -> Self {
        value.clone().into()
    }
}

/// A `Field` is a named aspect of a target or metric.
#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub value: FieldValue,
}

impl Field {
    /// Construct a field from its name and value.
    pub fn new<S, T>(name: S, value: T) -> Self
    where
        S: AsRef<str>,
        T: Into<FieldValue>,
    {
        Field { name: name.as_ref().to_string(), value: value.into() }
    }
}

/// The data type of an individual measurement of a metric.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    PartialOrd,
    Ord,
    Eq,
    Hash,
    JsonSchema,
    Serialize,
    Deserialize,
)]
pub enum MeasurementType {
    Bool,
    I64,
    F64,
    String,
    Bytes,
    CumulativeI64,
    CumulativeF64,
    HistogramI64,
    HistogramF64,
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

// Helper macro to generate `From<T>` and `From<&T>` for the measurement types.
macro_rules! impl_from {
    {$type_:ty, $variant:ident} => {
        impl From<$type_> for Measurement {
            fn from(value: $type_) -> Self {
                Measurement::$variant(value)
            }
        }

        impl From<&$type_> for Measurement where $type_: Clone {
            fn from(value: &$type_) -> Self {
                Measurement::$variant(value.clone())
            }
        }
    }
}

impl_from! { bool, Bool }
impl_from! { i64, I64 }
impl_from! { f64, F64 }
impl_from! { String, String }
impl_from! { Bytes, Bytes }
impl_from! { Cumulative<i64>, CumulativeI64 }
impl_from! { Cumulative<f64>, CumulativeF64 }
impl_from! { histogram::Histogram<i64>, HistogramI64 }
impl_from! { histogram::Histogram<f64>, HistogramF64 }

impl From<&str> for Measurement {
    fn from(value: &str) -> Self {
        Measurement::String(value.to_string())
    }
}

/// Errors related to the generation or collection of metrics.
#[derive(Debug, Clone, Error, JsonSchema, Serialize, Deserialize)]
pub enum Error {
    /// An error occurred during the production of metric samples.
    #[error("Error during sample production: {0}")]
    ProductionError(String),

    /// An error occured running a `ProducerServer`
    #[error("Error running metric server: {0}")]
    ProducerServer(String),

    /// An error running an `Oximeter` server
    #[error("Error running oximeter: {0}")]
    OximeterServer(String),

    /// An error interacting with the timeseries database
    #[error("Error interacting with timeseries database: {0}")]
    Database(String),

    /// A schema provided when collecting samples did not match the expected schema
    #[error("Schema mismatch for timeseries '{name}', expected fields {expected:?} found fields {actual:?}")]
    SchemaMismatch {
        name: String,
        expected: BTreeMap<String, FieldType>,
        actual: BTreeMap<String, FieldType>,
    },

    /// An error related to creating or sampling a [`histogram::Histogram`] metric.
    #[error("{0}")]
    HistogramError(#[from] histogram::HistogramError),

    /// An error querying or filtering data
    #[error("Invalid query or data filter: {0}")]
    QueryError(String),

    /// An error parsing a field or measurement from a string.
    #[error("String '{0}' could not be parsed as type '{1}'")]
    ParseError(String, String),
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

// A helper type for representing the name and fields derived from targets and metrics
#[derive(Clone, Debug, PartialEq, JsonSchema, Deserialize, Serialize)]
pub(crate) struct FieldSet {
    pub name: String,
    pub fields: Vec<Field>,
}

impl FieldSet {
    pub fn key(&self) -> String {
        self.fields
            .iter()
            .map(|field| field.value.to_string())
            .collect::<Vec<_>>()
            .join(":")
    }

    fn from_target(target: &impl traits::Target) -> Self {
        Self { name: target.name().to_string(), fields: target.fields() }
    }

    fn from_metric(metric: &impl traits::Metric) -> Self {
        Self { name: metric.name().to_string(), fields: metric.fields() }
    }
}

/// A concrete type carrying information about a target.
///
/// This type is used to "materialize" the information from the [`Target`](crate::traits::Target)
/// interface. It can only be constructed from a type that implements that trait, generally when a
/// [`Producer`](traits::Producer) generates [`Sample`]s for its monitored resources.
///
/// See the [`Target`](crate::traits::Target) trait for more details on each field.
#[derive(Debug, Clone, PartialEq, Eq, JsonSchema, Deserialize, Serialize)]
pub struct Target {
    /// The name of target.
    pub name: String,

    /// The fields for this target
    pub fields: Vec<Field>,
}

impl<T> From<&T> for Target
where
    T: traits::Target,
{
    fn from(target: &T) -> Self {
        Self { name: target.name().to_string(), fields: target.fields() }
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
    /// The name of metric.
    pub name: String,

    /// The fields for this metric
    pub fields: Vec<Field>,

    /// The data type of a measurement from this metric.
    pub measurement_type: MeasurementType,

    /// The measured value of this metric
    pub measurement: Measurement,
}

impl PartialEq for Metric {
    fn eq(&self, other: &Metric) -> bool {
        self.fields == other.fields
            && self.measurement_type == other.measurement_type
    }
}

impl Eq for Metric {}

impl<M> From<&M> for Metric
where
    M: traits::Metric,
{
    fn from(metric: &M) -> Self {
        Self {
            name: metric.name().to_string(),
            fields: metric.fields(),
            measurement_type: metric.measurement_type(),
            measurement: metric.measure(),
        }
    }
}

/// A concrete type representing a single, timestamped measurement from a timeseries.
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
pub struct Sample {
    /// The timestamp for this sample
    pub timestamp: DateTime<Utc>,

    /// The measured value of the metric at this sample
    pub measurement: Measurement,

    // Target name and fields
    target: FieldSet,

    // Metric name and fields
    metric: FieldSet,
}

impl PartialEq for Sample {
    /// Compare two Samples for equality.
    ///
    /// Two samples are considered equal if they have equal targets and metrics, and occur at the
    /// same time. Importantly, the _data_ is not used during comparison.
    fn eq(&self, other: &Sample) -> bool {
        self.target.eq(&other.target)
            && self.metric.eq(&other.metric)
            && self.timestamp.eq(&other.timestamp)
    }
}

impl Eq for Sample {}

impl Ord for Sample {
    /// Order two Samples.
    ///
    /// Samples are ordered by their target and metric keys, which include the field values of
    /// those, and then by timestamps. Importantly, the _data_ is not used for ordering.
    fn cmp(&self, other: &Sample) -> Ordering {
        self.timeseries_key()
            .cmp(&other.timeseries_key())
            .then(self.timestamp.cmp(&other.timestamp))
    }
}

impl PartialOrd for Sample {
    fn partial_cmp(&self, other: &Sample) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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
        timestamp: Option<DateTime<Utc>>,
    ) -> Self
    where
        T: traits::Target,
        M: traits::Metric<Measurement = Meas>,
        Meas: traits::DataPoint + Into<Measurement>,
    {
        Self {
            timestamp: timestamp.unwrap_or_else(Utc::now),
            target: FieldSet::from_target(target),
            metric: FieldSet::from_metric(metric),
            measurement: metric.measure(),
        }
    }

    /// Return the timeseries key for this sample, the concatenation of the target/metric field
    /// values.
    pub fn timeseries_key(&self) -> String {
        format!("{}:{}", self.target.key(), self.metric.key())
    }

    /// Return the name of the timeseries for this sample, the concatenation of the target/metric
    /// names.
    pub fn timeseries_name(&self) -> String {
        format!("{}:{}", self.target.name, self.metric.name)
    }

    /// Return the fields for this sample.
    ///
    /// This returns the target fields and metric fields, chained, although there is no distinction
    /// between them in this method.
    pub fn fields(&self) -> Vec<Field> {
        [self.target.fields.clone(), self.metric.fields.clone()].concat()
    }

    /// Return the name of this sample's target.
    pub fn target_name(&self) -> &str {
        &self.target.name
    }

    /// Return the fields of this sample's target.
    pub fn target_fields(&self) -> &Vec<Field> {
        &self.target.fields
    }

    /// Return the name of this sample's metric.
    pub fn metric_name(&self) -> &str {
        &self.metric.name
    }

    /// Return the fields of this sample's metric.
    pub fn metric_fields(&self) -> &Vec<Field> {
        &self.metric.fields
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    use super::histogram::Histogram;
    use super::{Cumulative, FieldType, FieldValue, Measurement};
    use crate::types;
    use crate::{Metric, Target};

    #[derive(Clone, Target)]
    struct Targ {
        pub good: bool,
        pub id: i64,
    }

    #[derive(Clone, Metric)]
    struct Met {
        pub good: bool,
        pub id: i64,
        pub value: i64,
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
            Measurement::from(Bytes::new()),
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
        assert_eq!(t.fields(), t2.fields);
    }

    #[test]
    fn test_metric_struct() {
        let m = Met { good: false, id: 2, value: 0 };
        let m2 = types::Metric::from(&m);
        assert_eq!(m.name(), m2.name);
        assert_eq!(m.fields(), m2.fields);
        assert_eq!(m.measurement_type(), m2.measurement_type);
    }

    #[test]
    fn test_sample_struct() {
        let t = Targ { good: false, id: 2 };
        let m = Met { good: false, id: 2, value: 1 };
        let timestamp = Utc::now();
        let sample = types::Sample::new(&t, &m, Some(timestamp));
        assert_eq!(
            sample.timeseries_name(),
            format!("{}:{}", t.name(), m.name())
        );
        assert_eq!(sample.timeseries_key(), format!("{}:{}", t.key(), m.key()));
        assert_eq!(sample.timestamp, timestamp);
        assert_eq!(sample.measurement, Measurement::I64(m.value));
    }

    #[test]
    fn test_field_value_parse_as_type() {
        let as_string = "some string";
        let as_i64 = "2";
        let as_ipaddr = "::1";
        let as_uuid = "3c937cd9-348f-42c2-bd44-d0a4dfffabd9";
        let as_bool = "false";

        assert_eq!(
            FieldValue::parse_as_type(&as_string, FieldType::String).unwrap(),
            FieldValue::from(&as_string),
        );
        assert_eq!(
            FieldValue::parse_as_type(&as_i64, FieldType::I64).unwrap(),
            FieldValue::from(2_i64),
        );
        assert_eq!(
            FieldValue::parse_as_type(&as_ipaddr, FieldType::IpAddr).unwrap(),
            FieldValue::from(as_ipaddr.parse::<IpAddr>().unwrap()),
        );
        assert_eq!(
            FieldValue::parse_as_type(&as_uuid, FieldType::Uuid).unwrap(),
            FieldValue::from(as_uuid.parse::<Uuid>().unwrap()),
        );
        assert_eq!(
            FieldValue::parse_as_type(&as_bool, FieldType::Bool).unwrap(),
            FieldValue::from(false),
        );

        assert!(FieldValue::parse_as_type(&as_string, FieldType::Uuid).is_err());
    }
}
