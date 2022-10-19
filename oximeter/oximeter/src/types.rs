// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used to describe targets, metrics, and measurements.
// Copyright 2021 Oxide Computer Company

use crate::histogram;
use crate::traits;
use crate::Producer;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use num_traits::{One, Zero};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::boxed::Box;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::ops::{Add, AddAssign};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use uuid::Uuid;

/// The `FieldType` identifies the data type of a target or metric field.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    JsonSchema,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
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

macro_rules! impl_field_type_from {
    ($ty:ty, $variant:path) => {
        impl From<&$ty> for FieldType {
            fn from(_: &$ty) -> FieldType {
                $variant
            }
        }
    };
}

impl_field_type_from! { String, FieldType::String }
impl_field_type_from! { i64, FieldType::I64 }
impl_field_type_from! { IpAddr, FieldType::IpAddr }
impl_field_type_from! { Uuid, FieldType::Uuid }
impl_field_type_from! { bool, FieldType::Bool }

/// The `FieldValue` contains the value of a target or metric field.
#[derive(
    Clone, Debug, Hash, PartialEq, Eq, JsonSchema, Serialize, Deserialize,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
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
    ) -> Result<Self, MetricsError> {
        let make_err = || MetricsError::ParseError {
            src: s.to_string(),
            typ: field_type.to_string(),
        };
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
#[derive(
    Clone, Debug, Hash, PartialEq, Eq, JsonSchema, Serialize, Deserialize,
)]
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
#[serde(rename_all = "snake_case")]
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

impl std::fmt::Display for DatumType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// A `Datum` is a single sampled data point from a metric.
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
#[serde(tag = "type", content = "datum", rename_all = "snake_case")]
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
    // The timestamp at which the measurement of the metric was taken.
    timestamp: DateTime<Utc>,
    // The underlying data point for the metric
    datum: Datum,
}

impl PartialEq<&Measurement> for Measurement {
    fn eq(&self, other: &&Measurement) -> bool {
        self.timestamp.eq(&other.timestamp) && self.datum.eq(&other.datum)
    }
}

impl Measurement {
    /// Construct a `Measurement` with the given timestamp.
    pub fn with_timestamp(timestamp: DateTime<Utc>, datum: Datum) -> Self {
        Self { timestamp, datum }
    }

    /// Generate a new measurement from a `Datum`, using the current time as the timestamp
    pub fn new<D: Into<Datum>>(datum: D) -> Measurement {
        Measurement { timestamp: Utc::now(), datum: datum.into() }
    }

    /// Return the datum for this measurement
    pub fn datum(&self) -> &Datum {
        &self.datum
    }

    /// Return the timestamp for this measurement
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Return the start time for this measurement, if it is from a cumulative metric, or `None` if
    /// a gauge.
    pub fn start_time(&self) -> Option<DateTime<Utc>> {
        self.datum.start_time()
    }

    /// Return the type of the underlying datum of a measurement
    pub fn datum_type(&self) -> DatumType {
        self.datum.datum_type()
    }
}

/// Errors related to the generation or collection of metrics.
#[derive(Debug, Clone, Error, JsonSchema, Serialize, Deserialize)]
#[serde(tag = "type", content = "content", rename_all = "snake_case")]
pub enum MetricsError {
    /// An error related to generating metric data points
    #[error("Metric data error: {0}")]
    DatumError(String),

    /// An error running an `Oximeter` server
    #[error("Error running oximeter: {0}")]
    OximeterServer(String),

    /// An error related to creating or sampling a [`histogram::Histogram`] metric.
    #[error("{0}")]
    HistogramError(#[from] histogram::HistogramError),

    /// An error parsing a field or measurement from a string.
    #[error("String '{src}' could not be parsed as type '{typ}'")]
    ParseError { src: String, typ: String },
}

/// A cumulative or counter data type.
#[derive(Debug, Clone, Copy, PartialEq, JsonSchema, Deserialize, Serialize)]
#[schemars(rename = "Cumulative{T}")]
pub struct Cumulative<T> {
    start_time: DateTime<Utc>,
    value: T,
}

impl<T> Cumulative<T>
where
    T: traits::Cumulative,
{
    /// Construct a new counter with the given start time.
    pub fn with_start_time(start_time: DateTime<Utc>, value: T) -> Self {
        Self { start_time, value }
    }

    /// Construct a new counter with the given initial value, using the current time as the start
    /// time.
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
    T: traits::Cumulative,
{
    type Output = Self;

    fn add(self, other: T) -> Self {
        Self::new(self.value + other)
    }
}

impl<T> AddAssign<T> for Cumulative<T>
where
    T: traits::Cumulative,
{
    fn add_assign(&mut self, other: T) {
        self.value += other;
    }
}

impl<T> Default for Cumulative<T>
where
    T: traits::Cumulative,
{
    fn default() -> Self {
        Self { start_time: Utc::now(), value: Zero::zero() }
    }
}

impl<T> From<T> for Cumulative<T>
where
    T: traits::Cumulative,
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
            && self.measurement.start_time().eq(&other.measurement.start_time())
            && self.measurement.timestamp().eq(&other.measurement.timestamp())
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
    {
        Self {
            timeseries_name: format!("{}:{}", target.name(), metric.name()),
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

type ProducerList = Vec<Box<dyn Producer>>;
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
#[serde(tag = "status", content = "info", rename_all = "snake_case")]
pub enum ProducerResultsItem {
    Ok(Vec<Sample>),
    Err(MetricsError),
}
pub type ProducerResults = Vec<ProducerResultsItem>;

/// The `ProducerRegistry` is a centralized collection point for metrics in consumer code.
#[derive(Debug, Clone)]
pub struct ProducerRegistry {
    producers: Arc<Mutex<ProducerList>>,
    producer_id: Uuid,
}

impl Default for ProducerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProducerRegistry {
    /// Construct a new `ProducerRegistry`.
    pub fn new() -> Self {
        Self::with_id(Uuid::new_v4())
    }

    /// Construct a new `ProducerRegistry` with the given producer ID.
    pub fn with_id(producer_id: Uuid) -> Self {
        Self { producers: Arc::new(Mutex::new(vec![])), producer_id }
    }

    /// Add a new [`Producer`] object to the registry.
    pub fn register_producer<P>(&self, producer: P) -> Result<(), MetricsError>
    where
        P: Producer,
    {
        self.producers.lock().unwrap().push(Box::new(producer));
        Ok(())
    }

    /// Collect available samples from all registered producers.
    ///
    /// This method returns a vector of results, one from each producer. If the producer generates
    /// an error, that's propagated here. Successfully produced samples are returned in a vector,
    /// ordered in the way they're produced internally.
    pub fn collect(&self) -> ProducerResults {
        let mut producers = self.producers.lock().unwrap();
        let mut results = Vec::with_capacity(producers.len());
        for producer in producers.iter_mut() {
            results.push(
                producer.produce().map(Iterator::collect).map_or_else(
                    ProducerResultsItem::Err,
                    ProducerResultsItem::Ok,
                ),
            );
        }
        results
    }

    /// Return the producer ID associated with this registry.
    pub fn producer_id(&self) -> Uuid {
        self.producer_id
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::net::IpAddr;
    use uuid::Uuid;

    use super::histogram::Histogram;
    use super::{
        Cumulative, Datum, DatumType, FieldType, FieldValue, Measurement,
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
        assert_eq!(measurement.start_time(), None);

        let datum = Cumulative::new(0i64);
        let measurement = Measurement::new(datum);
        assert_eq!(measurement.datum(), &Datum::from(datum));
        assert!(measurement.start_time().is_some());
        assert!(measurement.timestamp() >= measurement.start_time().unwrap());
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
        assert!(sample.measurement.start_time().is_none());
        assert_eq!(sample.measurement.datum(), &Datum::from(1i64));

        let m = test_util::TestCumulativeMetric {
            id: Uuid::new_v4(),
            good: true,
            datum: 1i64.into(),
        };
        let sample = types::Sample::new(&t, &m);
        assert!(sample.measurement.start_time().is_some());
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
