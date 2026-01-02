// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used to describe targets, metrics, and measurements.

// Copyright 2024 Oxide Computer Company

use crate::Producer;
use crate::TimeseriesName;
use crate::histogram;
use crate::traits;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use num::traits::One;
use num::traits::Zero;
use parse_display::Display;
use parse_display::FromStr;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Cow;
use std::boxed::Box;
use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::num::NonZeroU8;
use std::ops::Add;
use std::ops::AddAssign;
use std::sync::Arc;
use std::sync::Mutex;
use thiserror::Error;
use uuid::Uuid;

/// The `FieldType` identifies the data type of a target or metric field.
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
    Serialize,
    Deserialize,
    strum::EnumIter,
    FromStr,
    Display,
)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String,
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    IpAddr,
    Uuid,
    Bool,
}

impl FieldType {
    /// Return `true` if a field of this type is copyable.
    ///
    /// NOTE: This doesn't mean `self` is copyable, instead refering to a field
    /// with this type.
    pub const fn is_copyable(&self) -> bool {
        match self {
            FieldType::String => false,
            FieldType::I8
            | FieldType::U8
            | FieldType::I16
            | FieldType::U16
            | FieldType::I32
            | FieldType::U32
            | FieldType::I64
            | FieldType::U64
            | FieldType::IpAddr
            | FieldType::Uuid
            | FieldType::Bool => true,
        }
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
impl_field_type_from! { &'static str, FieldType::String }
impl_field_type_from! { Cow<'static, str>, FieldType::String }
impl_field_type_from! { i8, FieldType::I8 }
impl_field_type_from! { u8, FieldType::U8 }
impl_field_type_from! { i16, FieldType::I16 }
impl_field_type_from! { u16, FieldType::U16 }
impl_field_type_from! { i32, FieldType::I32 }
impl_field_type_from! { u32, FieldType::U32 }
impl_field_type_from! { i64, FieldType::I64 }
impl_field_type_from! { u64, FieldType::U64 }
impl_field_type_from! { IpAddr, FieldType::IpAddr }
impl_field_type_from! { Uuid, FieldType::Uuid }
impl_field_type_from! { bool, FieldType::Bool }

/// The `FieldValue` contains the value of a target or metric field.
#[derive(
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    JsonSchema,
    Serialize,
    Deserialize,
    strum::EnumCount,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum FieldValue {
    String(Cow<'static, str>),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    IpAddr(IpAddr),
    Uuid(Uuid),
    Bool(bool),
}

impl FieldValue {
    /// Return the type associated with this field
    pub fn field_type(&self) -> FieldType {
        match self {
            FieldValue::String(_) => FieldType::String,
            FieldValue::I8(_) => FieldType::I8,
            FieldValue::U8(_) => FieldType::U8,
            FieldValue::I16(_) => FieldType::I16,
            FieldValue::U16(_) => FieldType::U16,
            FieldValue::I32(_) => FieldType::I32,
            FieldValue::U32(_) => FieldType::U32,
            FieldValue::I64(_) => FieldType::I64,
            FieldValue::U64(_) => FieldType::U64,
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
            FieldType::String => {
                Ok(FieldValue::String(Cow::Owned(s.to_string())))
            }
            FieldType::I8 => {
                Ok(FieldValue::I8(s.parse().map_err(|_| make_err())?))
            }
            FieldType::U8 => {
                Ok(FieldValue::U8(s.parse().map_err(|_| make_err())?))
            }
            FieldType::I16 => {
                Ok(FieldValue::I16(s.parse().map_err(|_| make_err())?))
            }
            FieldType::U16 => {
                Ok(FieldValue::U16(s.parse().map_err(|_| make_err())?))
            }
            FieldType::I32 => {
                Ok(FieldValue::I32(s.parse().map_err(|_| make_err())?))
            }
            FieldType::U32 => {
                Ok(FieldValue::U32(s.parse().map_err(|_| make_err())?))
            }
            FieldType::I64 => {
                Ok(FieldValue::I64(s.parse().map_err(|_| make_err())?))
            }
            FieldType::U64 => {
                Ok(FieldValue::U64(s.parse().map_err(|_| make_err())?))
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
            FieldValue::String(inner) => write!(f, "{}", inner),
            FieldValue::I8(inner) => write!(f, "{}", inner),
            FieldValue::U8(inner) => write!(f, "{}", inner),
            FieldValue::I16(inner) => write!(f, "{}", inner),
            FieldValue::U16(inner) => write!(f, "{}", inner),
            FieldValue::I32(inner) => write!(f, "{}", inner),
            FieldValue::U32(inner) => write!(f, "{}", inner),
            FieldValue::I64(inner) => write!(f, "{}", inner),
            FieldValue::U64(inner) => write!(f, "{}", inner),
            FieldValue::IpAddr(inner) => write!(f, "{}", inner),
            FieldValue::Uuid(inner) => write!(f, "{}", inner),
            FieldValue::Bool(inner) => write!(f, "{}", inner),
        }
    }
}

macro_rules! impl_field_value_from {
    ($int:ty, $variant:path) => {
        impl From<$int> for FieldValue {
            fn from(value: $int) -> Self {
                $variant(value)
            }
        }
    };
}

impl_field_value_from! { i8, FieldValue::I8 }
impl_field_value_from! { u8, FieldValue::U8 }
impl_field_value_from! { i16, FieldValue::I16 }
impl_field_value_from! { u16, FieldValue::U16 }
impl_field_value_from! { i32, FieldValue::I32 }
impl_field_value_from! { u32, FieldValue::U32 }
impl_field_value_from! { i64, FieldValue::I64 }
impl_field_value_from! { u64, FieldValue::U64 }
impl_field_value_from! { Cow<'static, str>, FieldValue::String }
impl_field_value_from! { IpAddr, FieldValue::IpAddr }
impl_field_value_from! { Uuid, FieldValue::Uuid }
impl_field_value_from! { bool, FieldValue::Bool }

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        FieldValue::String(Cow::Owned(String::from(value)))
    }
}

impl From<String> for FieldValue {
    fn from(value: String) -> Self {
        FieldValue::String(Cow::Owned(value))
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
    strum::EnumIter,
    FromStr,
    Display,
)]
#[serde(rename_all = "snake_case")]
pub enum DatumType {
    Bool,
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    String,
    Bytes,
    CumulativeI64,
    CumulativeU64,
    CumulativeF32,
    CumulativeF64,
    HistogramI8,
    HistogramU8,
    HistogramI16,
    HistogramU16,
    HistogramI32,
    HistogramU32,
    HistogramI64,
    HistogramU64,
    HistogramF32,
    HistogramF64,
}

impl DatumType {
    /// Return `true` if this datum type is cumulative, and `false` otherwise.
    pub const fn is_cumulative(&self) -> bool {
        matches!(
            self,
            DatumType::CumulativeI64
                | DatumType::CumulativeU64
                | DatumType::CumulativeF32
                | DatumType::CumulativeF64
                | DatumType::HistogramI8
                | DatumType::HistogramU8
                | DatumType::HistogramI16
                | DatumType::HistogramU16
                | DatumType::HistogramI32
                | DatumType::HistogramU32
                | DatumType::HistogramI64
                | DatumType::HistogramU64
                | DatumType::HistogramF32
                | DatumType::HistogramF64
        )
    }

    /// Return `true` if this datum type is a scalar, and `false` otherwise.
    pub const fn is_scalar(&self) -> bool {
        !self.is_histogram()
    }

    /// Return `true` if this datum type is a histogram, and `false` otherwise.
    pub const fn is_histogram(&self) -> bool {
        matches!(
            self,
            DatumType::HistogramI8
                | DatumType::HistogramU8
                | DatumType::HistogramI16
                | DatumType::HistogramU16
                | DatumType::HistogramI32
                | DatumType::HistogramU32
                | DatumType::HistogramI64
                | DatumType::HistogramU64
                | DatumType::HistogramF32
                | DatumType::HistogramF64
        )
    }
}

/// A `Datum` is a single sampled data point from a metric.
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
#[serde(tag = "type", content = "datum", rename_all = "snake_case")]
pub enum Datum {
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Bytes),
    CumulativeI64(Cumulative<i64>),
    CumulativeU64(Cumulative<u64>),
    CumulativeF32(Cumulative<f32>),
    CumulativeF64(Cumulative<f64>),
    HistogramI8(histogram::Histogram<i8>),
    HistogramU8(histogram::Histogram<u8>),
    HistogramI16(histogram::Histogram<i16>),
    HistogramU16(histogram::Histogram<u16>),
    HistogramI32(histogram::Histogram<i32>),
    HistogramU32(histogram::Histogram<u32>),
    HistogramI64(histogram::Histogram<i64>),
    HistogramU64(histogram::Histogram<u64>),
    HistogramF32(histogram::Histogram<f32>),
    HistogramF64(histogram::Histogram<f64>),
    Missing(MissingDatum),
}

impl Datum {
    /// Return the [`DatumType`] for this measurement.
    pub fn datum_type(&self) -> DatumType {
        match self {
            Datum::Bool(_) => DatumType::Bool,
            Datum::I8(_) => DatumType::I8,
            Datum::U8(_) => DatumType::U8,
            Datum::I16(_) => DatumType::I16,
            Datum::U16(_) => DatumType::U16,
            Datum::I32(_) => DatumType::I32,
            Datum::U32(_) => DatumType::U32,
            Datum::I64(_) => DatumType::I64,
            Datum::U64(_) => DatumType::U64,
            Datum::F32(_) => DatumType::F32,
            Datum::F64(_) => DatumType::F64,
            Datum::String(_) => DatumType::String,
            Datum::Bytes(_) => DatumType::Bytes,
            Datum::CumulativeI64(_) => DatumType::CumulativeI64,
            Datum::CumulativeU64(_) => DatumType::CumulativeU64,
            Datum::CumulativeF32(_) => DatumType::CumulativeF32,
            Datum::CumulativeF64(_) => DatumType::CumulativeF64,
            Datum::HistogramI8(_) => DatumType::HistogramI8,
            Datum::HistogramU8(_) => DatumType::HistogramU8,
            Datum::HistogramI16(_) => DatumType::HistogramI16,
            Datum::HistogramU16(_) => DatumType::HistogramU16,
            Datum::HistogramI32(_) => DatumType::HistogramI32,
            Datum::HistogramU32(_) => DatumType::HistogramU32,
            Datum::HistogramI64(_) => DatumType::HistogramI64,
            Datum::HistogramU64(_) => DatumType::HistogramU64,
            Datum::HistogramF32(_) => DatumType::HistogramF32,
            Datum::HistogramF64(_) => DatumType::HistogramF64,
            Datum::Missing(inner) => inner.datum_type(),
        }
    }

    /// Return `true` if this `Datum` is cumulative.
    pub fn is_cumulative(&self) -> bool {
        self.datum_type().is_cumulative()
    }

    /// Return the start time of the underlying data, if this is cumulative, or `None`
    pub fn start_time(&self) -> Option<DateTime<Utc>> {
        match self {
            Datum::Bool(_)
            | Datum::I8(_)
            | Datum::U8(_)
            | Datum::I16(_)
            | Datum::U16(_)
            | Datum::I32(_)
            | Datum::U32(_)
            | Datum::I64(_)
            | Datum::U64(_)
            | Datum::F32(_)
            | Datum::F64(_)
            | Datum::String(_)
            | Datum::Bytes(_) => None,
            Datum::CumulativeI64(inner) => Some(inner.start_time()),
            Datum::CumulativeU64(inner) => Some(inner.start_time()),
            Datum::CumulativeF32(inner) => Some(inner.start_time()),
            Datum::CumulativeF64(inner) => Some(inner.start_time()),
            Datum::HistogramI8(inner) => Some(inner.start_time()),
            Datum::HistogramU8(inner) => Some(inner.start_time()),
            Datum::HistogramI16(inner) => Some(inner.start_time()),
            Datum::HistogramU16(inner) => Some(inner.start_time()),
            Datum::HistogramI32(inner) => Some(inner.start_time()),
            Datum::HistogramU32(inner) => Some(inner.start_time()),
            Datum::HistogramI64(inner) => Some(inner.start_time()),
            Datum::HistogramU64(inner) => Some(inner.start_time()),
            Datum::HistogramF32(inner) => Some(inner.start_time()),
            Datum::HistogramF64(inner) => Some(inner.start_time()),
            Datum::Missing(inner) => inner.start_time(),
        }
    }

    /// Return true if this datum is missing.
    pub fn is_missing(&self) -> bool {
        matches!(self, Datum::Missing(_))
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
impl_from! { i8, I8 }
impl_from! { u8, U8 }
impl_from! { i16, I16 }
impl_from! { u16, U16 }
impl_from! { i32, I32 }
impl_from! { u32, U32 }
impl_from! { i64, I64 }
impl_from! { u64, U64 }
impl_from! { f32, F32 }
impl_from! { f64, F64 }
impl_from! { String, String }
impl_from! { Bytes, Bytes }
impl_from! { Cumulative<i64>, CumulativeI64 }
impl_from! { Cumulative<u64>, CumulativeU64 }
impl_from! { Cumulative<f32>, CumulativeF32 }
impl_from! { Cumulative<f64>, CumulativeF64 }
impl_from! { histogram::Histogram<i8>, HistogramI8 }
impl_from! { histogram::Histogram<u8>, HistogramU8 }
impl_from! { histogram::Histogram<i16>, HistogramI16 }
impl_from! { histogram::Histogram<u16>, HistogramU16 }
impl_from! { histogram::Histogram<i32>, HistogramI32 }
impl_from! { histogram::Histogram<u32>, HistogramU32 }
impl_from! { histogram::Histogram<i64>, HistogramI64 }
impl_from! { histogram::Histogram<u64>, HistogramU64 }
impl_from! { histogram::Histogram<f32>, HistogramF32 }
impl_from! { histogram::Histogram<f64>, HistogramF64 }

impl From<&str> for Datum {
    fn from(value: &str) -> Self {
        Datum::String(value.to_string())
    }
}

impl From<&[u8]> for Datum {
    fn from(value: &[u8]) -> Self {
        Datum::Bytes(Bytes::from(value.to_vec()))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct MissingDatum {
    datum_type: DatumType,
    start_time: Option<DateTime<Utc>>,
}

impl MissingDatum {
    pub fn datum_type(&self) -> DatumType {
        self.datum_type
    }

    pub fn start_time(&self) -> Option<DateTime<Utc>> {
        self.start_time
    }

    pub fn new(
        datum_type: DatumType,
        start_time: Option<DateTime<Utc>>,
    ) -> Result<Self, MetricsError> {
        // See https://github.com/oxidecomputer/omicron/issues/4551.
        if datum_type == DatumType::Bytes {
            return Err(MetricsError::DatumError(String::from(
                "Missing samples from byte array types are not supported",
            )));
        }
        if datum_type.is_cumulative() && start_time.is_none() {
            return Err(MetricsError::MissingDatumRequiresStartTime {
                datum_type,
            });
        }
        if !datum_type.is_cumulative() && start_time.is_some() {
            return Err(MetricsError::MissingDatumCannotHaveStartTime {
                datum_type,
            });
        }
        Ok(Self { datum_type, start_time })
    }
}

impl From<MissingDatum> for Datum {
    fn from(d: MissingDatum) -> Datum {
        Datum::Missing(d)
    }
}

impl<M: oximeter::Metric> From<&M> for MissingDatum {
    fn from(metric: &M) -> Self {
        MissingDatum {
            datum_type: metric.datum_type(),
            start_time: metric.start_time(),
        }
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
    pub fn new<D: Into<Datum>>(timestamp: DateTime<Utc>, datum: D) -> Self {
        Self { timestamp, datum: datum.into() }
    }

    /// Return true if this measurement represents a missing datum.
    pub fn is_missing(&self) -> bool {
        self.datum.is_missing()
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

    /// A field name is duplicated between the target and metric.
    #[error("Field '{name}' is duplicated between the target and metric")]
    DuplicateFieldName { name: String },

    #[error("Missing datum of type {datum_type} requires a start time")]
    MissingDatumRequiresStartTime { datum_type: DatumType },

    #[error("Missing datum of type {datum_type} cannot have a start time")]
    MissingDatumCannotHaveStartTime { datum_type: DatumType },

    #[error("Invalid timeseries name")]
    InvalidTimeseriesName,

    #[error("TOML deserialization error: {0}")]
    Toml(String),

    #[error("Schema definition error: {0}")]
    SchemaDefinition(String),

    #[error("Target version {target} does not match metric version {metric}")]
    TargetMetricVersionMismatch {
        target: std::num::NonZeroU8,
        metric: std::num::NonZeroU8,
    },
}

impl From<MetricsError> for omicron_common::api::external::Error {
    fn from(e: MetricsError) -> Self {
        omicron_common::api::external::Error::internal_error(&e.to_string())
    }
}

/// A cumulative or counter data type.
#[derive(
    Debug,
    Deserialize,
    Clone,
    Copy,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
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

    /// Updates the current value of the counter to a specific value.
    pub fn set(&mut self, value: T) {
        self.value = value
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
    pub fields: BTreeMap<String, Field>,
}

impl FieldSet {
    fn from_target(target: &impl traits::Target) -> Self {
        let fields = target
            .fields()
            .iter()
            .cloned()
            .map(|f| (f.name.clone(), f))
            .collect();
        Self { name: target.name().to_string(), fields }
    }

    fn from_metric(metric: &impl traits::Metric) -> Self {
        let fields = metric
            .fields()
            .iter()
            .cloned()
            .map(|f| (f.name.clone(), f))
            .collect();
        Self { name: metric.name().to_string(), fields }
    }
}

/// A concrete type representing a single, timestamped measurement from a timeseries.
#[derive(Debug, Clone, JsonSchema, Deserialize, Serialize)]
pub struct Sample {
    /// The measured value of the metric at this sample
    pub measurement: Measurement,

    /// The name of the timeseries this sample belongs to
    pub timeseries_name: TimeseriesName,

    /// The version of the timeseries this sample belongs to
    //
    // TODO-cleanup: This should be removed once schema are tracked in CRDB.
    #[serde(default = "crate::schema::default_schema_version")]
    pub timeseries_version: NonZeroU8,

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
        self.timeseries_version.eq(&other.timeseries_version)
            && self.target.eq(&other.target)
            && self.metric.eq(&other.metric)
            && self.measurement.start_time().eq(&other.measurement.start_time())
            && self.measurement.timestamp().eq(&other.measurement.timestamp())
    }
}

impl Sample {
    /// Construct a new sample, recorded at the time of the supplied timestamp.
    ///
    /// This materializes the data from the target and metric, and stores that information along
    /// with the measurement data itself.
    pub fn new_with_timestamp<T, M, D>(
        timestamp: DateTime<Utc>,
        target: &T,
        metric: &M,
    ) -> Result<Self, MetricsError>
    where
        T: traits::Target,
        M: traits::Metric<Datum = D>,
    {
        if target.version() != metric.version() {
            return Err(MetricsError::TargetMetricVersionMismatch {
                target: target.version(),
                metric: metric.version(),
            });
        }
        let target_fields = FieldSet::from_target(target);
        let metric_fields = FieldSet::from_metric(metric);
        Self::verify_field_names(&target_fields, &metric_fields)?;
        let timeseries_name = crate::timeseries_name(target, metric)?;
        Ok(Self {
            timeseries_name,
            timeseries_version: target.version(),
            target: target_fields,
            metric: metric_fields,
            measurement: metric.measure(timestamp),
        })
    }

    /// Construct a new missing sample, recorded at the time of the supplied
    /// timestamp.
    pub fn new_missing_with_timestamp<T, M, D>(
        timestamp: DateTime<Utc>,
        target: &T,
        metric: &M,
    ) -> Result<Self, MetricsError>
    where
        T: traits::Target,
        M: traits::Metric<Datum = D>,
    {
        if target.version() != metric.version() {
            return Err(MetricsError::TargetMetricVersionMismatch {
                target: target.version(),
                metric: metric.version(),
            });
        }
        let target_fields = FieldSet::from_target(target);
        let metric_fields = FieldSet::from_metric(metric);
        Self::verify_field_names(&target_fields, &metric_fields)?;
        let datum = Datum::Missing(MissingDatum::from(metric));
        let timeseries_name = crate::timeseries_name(target, metric)?;
        Ok(Self {
            timeseries_name,
            timeseries_version: target.version(),
            target: target_fields,
            metric: metric_fields,
            measurement: Measurement { timestamp, datum },
        })
    }

    /// Construct a new sample, created at the time the function is called.
    ///
    /// This materializes the data from the target and metric, and stores that information along
    /// with the measurement data itself.
    pub fn new<T, M, D>(target: &T, metric: &M) -> Result<Self, MetricsError>
    where
        T: traits::Target,
        M: traits::Metric<Datum = D>,
    {
        Self::new_with_timestamp(Utc::now(), target, metric)
    }

    /// Construct a new sample with a missing measurement.
    pub fn new_missing<T, M, D>(
        target: &T,
        metric: &M,
    ) -> Result<Self, MetricsError>
    where
        T: traits::Target,
        M: traits::Metric<Datum = D>,
    {
        Self::new_missing_with_timestamp(Utc::now(), target, metric)
    }

    /// Return the fields for this sample.
    ///
    /// This returns the target fields and metric fields, chained, although there is no distinction
    /// between them in this method.
    pub fn fields(&self) -> Vec<Field> {
        let mut out = Vec::with_capacity(
            self.target.fields.len() + self.metric.fields.len(),
        );
        for f in self.target.fields.values().chain(self.metric.fields.values())
        {
            out.push(f.clone())
        }
        out
    }

    /// Return the name of this sample's target.
    pub fn target_name(&self) -> &str {
        &self.target.name
    }

    /// Return the fields of this sample's target.
    pub fn target_fields(&self) -> impl Iterator<Item = &Field> {
        self.target.fields.values()
    }

    /// Return the sorted fields of this sample's target.
    pub fn sorted_target_fields(&self) -> &BTreeMap<String, Field> {
        &self.target.fields
    }

    /// Return the name of this sample's metric.
    pub fn metric_name(&self) -> &str {
        &self.metric.name
    }

    /// Return the fields of this sample's metric.
    pub fn metric_fields(&self) -> impl Iterator<Item = &Field> {
        self.metric.fields.values()
    }

    /// Return the sorted fields of this sample's metric
    pub fn sorted_metric_fields(&self) -> &BTreeMap<String, Field> {
        &self.metric.fields
    }

    // Check validity of field names for the target and metric. Currently this
    // just verifies there are no duplicate names between them.
    fn verify_field_names(
        target: &FieldSet,
        metric: &FieldSet,
    ) -> Result<(), MetricsError> {
        for name in target.fields.keys() {
            if metric.fields.contains_key(name) {
                return Err(MetricsError::DuplicateFieldName {
                    name: name.to_string(),
                });
            }
        }
        Ok(())
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
    use super::Cumulative;
    use super::Datum;
    use super::DatumType;
    use super::Field;
    use super::FieldSet;
    use super::FieldType;
    use super::FieldValue;
    use super::Measurement;
    use super::MetricsError;
    use super::Sample;
    use super::histogram::Histogram;
    use bytes::Bytes;
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

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
        let measurement = Measurement::new(chrono::Utc::now(), 0i64);
        assert_eq!(measurement.datum_type(), DatumType::I64);
        assert!(measurement.start_time().is_none());

        let datum = Cumulative::new(0i64);
        let measurement = Measurement::new(chrono::Utc::now(), datum);
        assert_eq!(measurement.datum(), &Datum::from(datum));
        assert!(measurement.start_time().is_some());
        assert!(measurement.timestamp() >= measurement.start_time().unwrap());
    }

    #[rstest::rstest]
    #[case::as_string("some string", FieldValue::String("some string".into()))]
    #[case::as_i8("2", FieldValue::I8(2))]
    #[case::as_u8("2", FieldValue::U8(2))]
    #[case::as_i16("2", FieldValue::I16(2))]
    #[case::as_u16("2", FieldValue::U16(2))]
    #[case::as_i32("2", FieldValue::I32(2))]
    #[case::as_u32("2", FieldValue::U32(2))]
    #[case::as_i64("2", FieldValue::I64(2))]
    #[case::as_u64("2", FieldValue::U64(2))]
    #[case::as_uuid(
        "684f42af-0500-4fc9-be93-fdf1a7ac36ad",
        FieldValue::Uuid(uuid::uuid!("684f42af-0500-4fc9-be93-fdf1a7ac36ad"))
    )]
    #[case::as_ipv4addr("127.0.0.1", FieldValue::from(Ipv4Addr::LOCALHOST))]
    #[case::as_ipv6addr("::1", FieldValue::from(Ipv6Addr::LOCALHOST))]
    fn test_field_value_parse_as_type(
        #[case] unparsed: &str,
        #[case] expected: FieldValue,
    ) {
        let parsed =
            FieldValue::parse_as_type(unparsed, expected.field_type()).unwrap();
        assert_eq!(parsed, expected);
    }

    #[rstest::rstest]
    #[case::as_u64(FieldType::U64)]
    #[case::as_uuid(FieldType::Uuid)]
    #[case::as_bool(FieldType::Bool)]
    #[case::as_ipaddr(FieldType::IpAddr)]
    #[case::as_uuid(FieldType::Uuid)]
    fn test_field_value_parse_as_wrong_type(#[case] ty: FieldType) {
        assert!(FieldValue::parse_as_type("baddcafe", ty).is_err());
    }

    #[test]
    fn test_verify_field_names() {
        let mut fields = BTreeMap::new();
        let field =
            Field { name: "n".to_string(), value: FieldValue::from(0i64) };
        fields.insert(field.name.clone(), field);
        let fields = FieldSet { name: "t".to_string(), fields };
        assert!(matches!(
            Sample::verify_field_names(&fields, &fields),
            Err(MetricsError::DuplicateFieldName { .. })
        ));
    }
}
