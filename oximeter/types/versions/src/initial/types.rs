// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used to describe targets, metrics, and measurements.

use super::histogram;
use super::schema::TimeseriesName;
use crate::impls::traits::Producer;
use crate::latest::types::MetricsError;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use parse_display::Display;
use parse_display::FromStr;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Cow;
use std::boxed::Box;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::num::NonZeroU8;
use std::sync::Arc;
use std::sync::Mutex;
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

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct MissingDatum {
    pub(crate) datum_type: DatumType,
    pub(crate) start_time: Option<DateTime<Utc>>,
}

/// A `Measurement` is a timestamped datum from a single metric
#[derive(Clone, Debug, PartialEq, JsonSchema, Serialize, Deserialize)]
pub struct Measurement {
    // The timestamp at which the measurement of the metric was taken.
    pub(crate) timestamp: DateTime<Utc>,
    // The underlying data point for the metric
    pub(crate) datum: Datum,
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
    pub(crate) start_time: DateTime<Utc>,
    pub(crate) value: T,
}

// A helper type for representing the name and fields derived from targets and metrics
#[derive(Clone, Debug, PartialEq, JsonSchema, Deserialize, Serialize)]
pub(crate) struct FieldSet {
    pub name: String,
    pub fields: BTreeMap<String, Field>,
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
    #[serde(default = "crate::latest::schema::default_schema_version")]
    pub timeseries_version: NonZeroU8,

    // Target name and fields
    pub(crate) target: FieldSet,

    // Metric name and fields
    pub(crate) metric: FieldSet,
}

type ProducerList = Vec<Box<dyn Producer>>;
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(tag = "status", content = "info", rename_all = "snake_case")]
pub enum ProducerResultsItem {
    Ok(Vec<Sample>),
    Err(MetricsError),
}
pub type ProducerResults = Vec<ProducerResultsItem>;

/// The `ProducerRegistry` is a centralized collection point for metrics in consumer code.
#[derive(Debug, Clone)]
pub struct ProducerRegistry {
    pub(crate) producers: Arc<Mutex<ProducerList>>,
    pub(crate) producer_id: Uuid,
}
