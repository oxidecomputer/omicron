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

/// The `FieldType` identifies the data type of a target or metric field.
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

    // Format the value for use in a query to the database, e.g., `... WHERE (field_value = {})`.
    pub(crate) fn as_db_str(&self) -> String {
        match self {
            FieldValue::Bool(ref inner) => {
                format!("{}", if *inner { 1 } else { 0 })
            }
            FieldValue::I64(ref inner) => format!("{}", inner),
            FieldValue::IpAddr(ref inner) => {
                let addr = match inner {
                    IpAddr::V4(ref v4) => v4.to_ipv6_mapped(),
                    IpAddr::V6(ref v6) => *v6,
                };
                format!("'{}'", addr)
            }
            FieldValue::String(ref inner) => format!("'{}'", inner),
            FieldValue::Uuid(ref inner) => format!("'{}'", inner),
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

/// The type of an individual datum of a metric.
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
pub enum DatumType {
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

impl DatumType {
    // Return the name of the type as it's referred to in the timeseries database. This is used
    // internally to build the table containing samples of the corresponding datum type.
    pub(crate) fn db_type_name(&self) -> &str {
        match self {
            DatumType::Bool => "bool",
            DatumType::I64 => "i64",
            DatumType::F64 => "f64",
            DatumType::String => "string",
            DatumType::Bytes => "bytes",
            DatumType::CumulativeI64 => "cumulativei64",
            DatumType::CumulativeF64 => "cumulativef64",
            DatumType::HistogramI64 => "histogrami64",
            DatumType::HistogramF64 => "histogramf64",
        }
    }

    /// Return `true` if this datum type is cumulative, and `false` otherwise.
    pub fn is_cumulative(&self) -> bool {
        matches!(
            self,
            DatumType::CumulativeI64
                | DatumType::CumulativeF64
                | DatumType::HistogramI64
                | DatumType::HistogramF64
        )
    }
}

/// The `SampleTime` represents the instant or range of time over which a metric is sampled.
///
/// Gauge metrics are sampled at instantaneous points in time. Cumulative metrics are by definition
/// over an interval of time. The `SampleTime` provides a safer interface to both these concepts.
///
/// The type may be constructed via the `SampleTime::instant` or `SampleTime::interval` associated
/// functions. The latter is fallible, and enforces that the start time is strictly before the end
/// time.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    Deserialize,
    Serialize,
)]
pub struct SampleTime {
    start_time: Option<DateTime<Utc>>,
    timestamp: DateTime<Utc>,
}

impl SampleTime {
    /// Create a new instantaneous sample time
    pub fn instant(timestamp: DateTime<Utc>) -> Self {
        SampleTime { start_time: None, timestamp }
    }

    /// Create a new interval sample time
    pub fn interval(
        start_time: DateTime<Utc>,
        timestamp: DateTime<Utc>,
    ) -> Result<Self, Error> {
        if start_time < timestamp {
            Ok(Self { start_time: Some(start_time), timestamp })
        } else {
            Err(Error::DatumError(
                "Start time of an interval must be strictly before end time"
                    .to_string(),
            ))
        }
    }

    /// Return the current timestamp for a `SampleTime`.
    ///
    /// Note that this is defined as the end time for an `Interval`.
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Return the start time for an interval, or `None` if this is an instant.
    pub fn start_time(&self) -> Option<DateTime<Utc>> {
        self.start_time
    }

    /// Return true if this is an instant, else false.
    pub fn is_instant(&self) -> bool {
        self.start_time.is_none()
    }
}

/// A `Datum` is a single sampled data point from a metric.
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub enum Datum {
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

impl Datum {
    /// Return the [`DatumType`] for this measurement.
    pub fn datum_type(&self) -> DatumType {
        match self {
            Datum::Bool(_) => DatumType::Bool,
            Datum::I64(_) => DatumType::I64,
            Datum::F64(_) => DatumType::F64,
            Datum::String(_) => DatumType::String,
            Datum::Bytes(_) => DatumType::Bytes,
            Datum::CumulativeI64(_) => DatumType::CumulativeI64,
            Datum::CumulativeF64(_) => DatumType::CumulativeF64,
            Datum::HistogramI64(_) => DatumType::HistogramI64,
            Datum::HistogramF64(_) => DatumType::HistogramF64,
        }
    }

    /// Return `true` if this `Datum` is cumulative.
    pub fn is_cumulative(&self) -> bool {
        self.datum_type().is_cumulative()
    }

    /// Return the start time of the underlying data, if this is cumulative, or `None`
    pub fn start_time(&self) -> Option<DateTime<Utc>> {
        match self {
            Datum::Bool(_) => None,
            Datum::I64(_) => None,
            Datum::F64(_) => None,
            Datum::String(_) => None,
            Datum::Bytes(_) => None,
            Datum::CumulativeI64(ref inner) => Some(inner.start_time()),
            Datum::CumulativeF64(ref inner) => Some(inner.start_time()),
            Datum::HistogramI64(ref inner) => Some(inner.start_time()),
            Datum::HistogramF64(ref inner) => Some(inner.start_time()),
        }
    }
}

// Helper macro to generate `From<T>` and `From<&T>` for the datum types.
macro_rules! impl_from {
    {$type_:ty, $variant:ident} => {
        impl From<$type_> for Datum {
            fn from(value: $type_) -> Self {
                Datum::$variant(value)
            }
        }

        impl From<&$type_> for Datum where $type_: Clone {
            fn from(value: &$type_) -> Self {
                Datum::$variant(value.clone())
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

impl From<&str> for Datum {
    fn from(value: &str) -> Self {
        Datum::String(value.to_string())
    }
}

/// A `Measurement` is a timestamped datum from a single metric
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub struct Measurement {
    // The time point or interval over which the datum is collected
    sample_time: SampleTime,
    // The underlying data point for the metric
    datum: Datum,
}

impl Measurement {
    // Internal constructor. `sample_time` is assumed valid for `datum`.
    pub(crate) fn with_sample_time(
        sample_time: SampleTime,
        datum: Datum,
    ) -> Self {
        Self { sample_time, datum }
    }

    /// Generate a new measurement from a `Datum`
    pub fn new<D: Into<Datum>>(datum: D) -> Measurement {
        let datum = datum.into();
        let sample_time = SampleTime {
            start_time: datum.start_time(),
            timestamp: Utc::now(),
        };
        Measurement { sample_time, datum }
    }

    /// Return the datum for this measurement
    pub fn datum(&self) -> &Datum {
        &self.datum
    }

    /// Return the sample time for this measurement
    pub fn sample_time(&self) -> &SampleTime {
        &self.sample_time
    }

    /// Return the type of the underlying datum of a measurement
    pub fn datum_type(&self) -> DatumType {
        self.datum.datum_type()
    }
}

/// Errors related to the generation or collection of metrics.
#[derive(Debug, Clone, Error, JsonSchema, Serialize, Deserialize)]
pub enum Error {
    /// An error related to generating metric data points
    #[error("Metric data error: {0}")]
    DatumError(String),

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
pub struct Cumulative<T> {
    start_time: DateTime<Utc>,
    value: T,
}

pub trait CumulativeType:
    traits::DataPoint + Add + AddAssign + Copy + One + Zero
{
}
impl CumulativeType for i64 {}
impl CumulativeType for f64 {}

impl<T> Cumulative<T>
where
    T: CumulativeType,
{
    // Internal constructor
    pub(crate) fn with_start_time(start_time: DateTime<Utc>, value: T) -> Self {
        Self { start_time, value }
    }

    /// Construct a new counter with the given initial value.
    pub fn new(value: T) -> Self {
        Self { start_time: Utc::now(), value }
    }

    /// Add 1 to the internal counter.
    pub fn increment(&mut self) {
        self.value += One::one();
    }

    /// Return the current value of the counter.
    pub fn value(&self) -> T {
        self.value
    }

    /// Return the start time of this cumulative counter.
    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }
}

impl<T> Add<T> for Cumulative<T>
where
    T: CumulativeType,
{
    type Output = Self;

    fn add(self, other: T) -> Self {
        Self::new(self.value + other)
    }
}

impl<T> AddAssign<T> for Cumulative<T>
where
    T: CumulativeType,
{
    fn add_assign(&mut self, other: T) {
        self.value += other;
    }
}

impl<T> Default for Cumulative<T>
where
    T: CumulativeType,
{
    fn default() -> Self {
        Self { start_time: Utc::now(), value: Zero::zero() }
    }
}

impl<T> From<T> for Cumulative<T>
where
    T: CumulativeType,
{
    fn from(value: T) -> Cumulative<T> {
        Cumulative::new(value)
    }
}

// A helper type for representing the name and fields derived from targets and metrics
#[derive(Clone, Debug, PartialEq, JsonSchema, Deserialize, Serialize)]
pub(crate) struct FieldSet {
    pub name: String,
    pub fields: Vec<Field>,
}

impl FieldSet {
    fn from_target(target: &impl traits::Target) -> Self {
        Self { name: target.name().to_string(), fields: target.fields() }
    }

    fn from_metric(metric: &impl traits::Metric) -> Self {
        Self { name: metric.name().to_string(), fields: metric.fields() }
    }
}

/// A concrete type representing a single, timestamped measurement from a timeseries.
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
pub struct Sample {
    /// The measured value of the metric at this sample
    pub measurement: Measurement,

    /// The name of the timeseries this sample belongs to
    pub timeseries_name: String,

    /// The key of the timeseries this sample belongs to
    pub timeseries_key: String,

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
            && self
                .measurement
                .sample_time()
                .eq(&other.measurement.sample_time())
    }
}

impl Eq for Sample {}

impl Ord for Sample {
    /// Order two Samples.
    ///
    /// Samples are ordered by their target and metric keys, which include the field values of
    /// those, and then by timestamps. Importantly, the _data_ is not used for ordering.
    fn cmp(&self, other: &Sample) -> Ordering {
        self.timeseries_key.cmp(&other.timeseries_key).then(
            self.measurement
                .sample_time()
                .cmp(&other.measurement.sample_time()),
        )
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
    /// with the measurement data itself.
    pub fn new<T, M, D>(target: &T, metric: &M) -> Self
    where
        T: traits::Target,
        M: traits::Metric<Datum = D>,
        D: traits::DataPoint + Into<Datum>,
    {
        Self {
            timeseries_name: format!("{}:{}", target.name(), metric.name()),
            timeseries_key: format!("{}:{}", target.key(), metric.key()),
            target: FieldSet::from_target(target),
            metric: FieldSet::from_metric(metric),
            measurement: metric.measure(),
        }
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
    use bytes::Bytes;
    use chrono::Utc;
    use std::net::IpAddr;
    use uuid::Uuid;

    use super::histogram::Histogram;
    use super::{
        Cumulative, Datum, DatumType, FieldType, FieldValue, Measurement,
        SampleTime,
    };
    use crate::test_util;
    use crate::types;
    use crate::{Metric, Target};

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
    fn test_sample_time() {
        let now = Utc::now();
        let then = now + chrono::Duration::seconds(1);
        let t = SampleTime::instant(now);
        assert_eq!(t.timestamp(), now);
        assert!(t.start_time().is_none());

        let t = SampleTime::interval(now, then).unwrap();
        assert_eq!(t.start_time().unwrap(), now);
        assert_eq!(t.timestamp(), then);

        let t = SampleTime::interval(then, now);
        assert!(t.is_err());
    }

    #[test]
    fn test_datum() {
        assert!(matches!(Datum::from(false), Datum::Bool(_)));
        assert!(matches!(Datum::from(0i64), Datum::I64(_)));
        assert!(matches!(Datum::from(0f64), Datum::F64(_)));
        assert!(matches!(Datum::from("foo"), Datum::String(_)));
        assert!(matches!(Datum::from(Bytes::new()), Datum::Bytes(_)));
        assert!(matches!(
            Datum::from(Cumulative::new(0i64)),
            Datum::CumulativeI64(_)
        ));
        assert!(matches!(
            Datum::from(Cumulative::new(0f64)),
            Datum::CumulativeF64(_)
        ));
        assert!(matches!(
            Datum::from(Histogram::new(&[0i64, 10]).unwrap()),
            Datum::HistogramI64(_)
        ));
        assert!(matches!(
            Datum::from(Histogram::new(&[0f64, 10.0]).unwrap()),
            Datum::HistogramF64(_)
        ));
    }

    #[test]
    fn test_measurement() {
        let measurement = Measurement::new(0i64);
        assert_eq!(measurement.datum_type(), DatumType::I64);
        assert_eq!(measurement.sample_time().start_time(), None);

        let datum = Cumulative::new(0i64);
        let measurement = Measurement::new(datum.clone());
        assert_eq!(measurement.datum(), &Datum::from(datum));
        let sample_time = measurement.sample_time();
        assert!(sample_time.start_time().is_some());
        assert!(sample_time.timestamp() >= sample_time.start_time().unwrap());
    }

    #[test]
    fn test_sample_struct() {
        let t = test_util::TestTarget::default();
        let m = test_util::TestMetric {
            id: Uuid::new_v4(),
            good: true,
            datum: 1i64,
        };
        let sample = types::Sample::new(&t, &m);
        assert_eq!(
            sample.timeseries_name,
            format!("{}:{}", t.name(), m.name())
        );
        assert_eq!(sample.timeseries_key, format!("{}:{}", t.key(), m.key()));
        assert!(sample.measurement.sample_time().start_time().is_none());
        assert_eq!(sample.measurement.datum(), &Datum::from(1i64));

        let m = test_util::TestCumulativeMetric {
            id: Uuid::new_v4(),
            good: true,
            datum: 1i64.into(),
        };
        let sample = types::Sample::new(&t, &m);
        assert!(sample.measurement.sample_time().start_time().is_some());
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
