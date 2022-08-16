// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Models for timeseries data in ClickHouse
// Copyright 2021 Oxide Computer Company

use crate::{
    DbFieldSource, FieldSchema, FieldSource, Metric, Target, TimeseriesKey,
    TimeseriesName, TimeseriesSchema,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use oximeter::histogram::Histogram;
use oximeter::traits;
use oximeter::types::{
    Cumulative, Datum, DatumType, Field, FieldType, FieldValue, Measurement,
    Sample,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::net::{IpAddr, Ipv6Addr};
use uuid::Uuid;

// Wrapper type to represent a boolean in the database.
//
// ClickHouse's type system lacks a boolean, and using `u8` to represent them. This a safe wrapper
// type around that for serializing to/from the database.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(transparent)]
struct DbBool {
    inner: u8,
}

impl From<u64> for DbBool {
    fn from(b: u64) -> Self {
        assert!(b < 2, "A boolean can only be represented by 0 or 1 in the database, but found {}", b);
        Self { inner: b as _ }
    }
}

impl From<bool> for DbBool {
    fn from(b: bool) -> Self {
        DbBool { inner: b as _ }
    }
}

impl From<DbBool> for bool {
    fn from(b: DbBool) -> bool {
        match b.inner {
            0 => false,
            1 => true,
            x => {
                unreachable!(
                    "A boolean can only be represented by 0 or 1 in the database, but found {}",
                    x
                );
            }
        }
    }
}

impl From<DbBool> for Datum {
    fn from(b: DbBool) -> Datum {
        Datum::from(bool::from(b))
    }
}

// The list of fields in a schema as represented in the actual schema tables in the database.
//
// Data about the schema in ClickHouse is represented in the `timeseries_schema` table. this
// contains the fields as a nested table, which is stored as a struct of arrays. This type is used
// to convert between that representation in the database, and the representation we prefer, i.e.,
// the `FieldSchema` type. In other words, we prefer to work with an array of structs, but
// ClickHouse requires a struct of arrays. `Field` is the former, `DbFieldList` is the latter.
//
// Note that the fields are renamed so that ClickHouse interprets them as correctly referring to
// the nested column names.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub(crate) struct DbFieldList {
    #[serde(rename = "fields.name")]
    pub names: Vec<String>,
    #[serde(rename = "fields.type")]
    pub types: Vec<DbFieldType>,
    #[serde(rename = "fields.source")]
    pub sources: Vec<DbFieldSource>,
}

impl From<DbFieldList> for Vec<FieldSchema> {
    fn from(list: DbFieldList) -> Self {
        list.names
            .into_iter()
            .zip(list.types.into_iter())
            .zip(list.sources.into_iter())
            .map(|((name, ty), source)| FieldSchema {
                name,
                ty: ty.into(),
                source: source.into(),
            })
            .collect()
    }
}

impl From<Vec<FieldSchema>> for DbFieldList {
    fn from(list: Vec<FieldSchema>) -> Self {
        let mut names = Vec::with_capacity(list.len());
        let mut types = Vec::with_capacity(list.len());
        let mut sources = Vec::with_capacity(list.len());
        for field in list.into_iter() {
            names.push(field.name);
            types.push(field.ty.into());
            sources.push(field.source.into());
        }
        DbFieldList { names, types, sources }
    }
}

// The `DbTimeseriesSchema` type models the `oximeter.timeseries_schema` table.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DbTimeseriesSchema {
    pub timeseries_name: String,
    #[serde(flatten)]
    pub field_schema: DbFieldList,
    pub datum_type: DbDatumType,
    #[serde(with = "serde_timestamp")]
    pub created: DateTime<Utc>,
}

impl From<TimeseriesSchema> for DbTimeseriesSchema {
    fn from(schema: TimeseriesSchema) -> DbTimeseriesSchema {
        DbTimeseriesSchema {
            timeseries_name: schema.timeseries_name.to_string(),
            field_schema: schema.field_schema.into(),
            datum_type: schema.datum_type.into(),
            created: schema.created,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum DbFieldType {
    String,
    I64,
    IpAddr,
    Uuid,
    Bool,
}

impl From<DbFieldType> for FieldType {
    fn from(src: DbFieldType) -> Self {
        match src {
            DbFieldType::String => FieldType::String,
            DbFieldType::I64 => FieldType::I64,
            DbFieldType::IpAddr => FieldType::IpAddr,
            DbFieldType::Uuid => FieldType::Uuid,
            DbFieldType::Bool => FieldType::Bool,
        }
    }
}
impl From<FieldType> for DbFieldType {
    fn from(src: FieldType) -> Self {
        match src {
            FieldType::String => DbFieldType::String,
            FieldType::I64 => DbFieldType::I64,
            FieldType::IpAddr => DbFieldType::IpAddr,
            FieldType::Uuid => DbFieldType::Uuid,
            FieldType::Bool => DbFieldType::Bool,
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum DbDatumType {
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

impl From<DatumType> for DbDatumType {
    fn from(src: DatumType) -> Self {
        match src {
            DatumType::Bool => DbDatumType::Bool,
            DatumType::I64 => DbDatumType::I64,
            DatumType::F64 => DbDatumType::F64,
            DatumType::String => DbDatumType::String,
            DatumType::Bytes => DbDatumType::Bytes,
            DatumType::CumulativeI64 => DbDatumType::CumulativeI64,
            DatumType::CumulativeF64 => DbDatumType::CumulativeF64,
            DatumType::HistogramI64 => DbDatumType::HistogramI64,
            DatumType::HistogramF64 => DbDatumType::HistogramF64,
        }
    }
}

impl From<DbDatumType> for DatumType {
    fn from(src: DbDatumType) -> Self {
        match src {
            DbDatumType::Bool => DatumType::Bool,
            DbDatumType::I64 => DatumType::I64,
            DbDatumType::F64 => DatumType::F64,
            DbDatumType::String => DatumType::String,
            DbDatumType::Bytes => DatumType::Bytes,
            DbDatumType::CumulativeI64 => DatumType::CumulativeI64,
            DbDatumType::CumulativeF64 => DatumType::CumulativeF64,
            DbDatumType::HistogramI64 => DatumType::HistogramI64,
            DbDatumType::HistogramF64 => DatumType::HistogramF64,
        }
    }
}

// Internal module used to serialize datetimes to the database.
//
// Serde by default includes the timezone when serializing at `DateTime`. However, the `DateTime64`
// type in ClickHouse already includes the timezone, and so times are always assumed relative to
// that timezone. So it doesn't accept the default serialization format.
//
// ClickHouse also accepts integers, in the tick resolution of the `DateTime64` type, which is
// nanoseconds in our case. We opt for strings here, since we're using that anyway in the
// input/output format for ClickHouse.
mod serde_timestamp {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(
        date: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(crate::DATABASE_TIMESTAMP_FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, crate::DATABASE_TIMESTAMP_FORMAT)
            .map_err(serde::de::Error::custom)
    }
}

// Generate a method `table_name(&self) -> String`, which returns the name of the ClickHouse table
// that a given type models.
macro_rules! impl_table_name {
    {$name:ident, $table_kind:literal, $data_type:literal} => {
        impl $name {
            fn table_name(&self) -> String {
                format!(
                    "{db_name}.{table_kind}_{data_type}",
                    table_kind = $table_kind,
                    db_name = crate::DATABASE_NAME,
                    data_type = $data_type,
                )
            }
        }
    };
}

// Generate a struct declaration and an implementation that returns the name of the database table
// for that struct, used to define types modeling the rows of each of the `fields_*` tables in
// ClickHouse.
macro_rules! declare_field_row {
    {$name:ident, $value_type:ty, $data_type:literal} => {
        #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
        struct $name {
            timeseries_name: String,
            timeseries_key: TimeseriesKey,
            field_name: String,
            field_value: $value_type,
        }
        impl_table_name!{$name, "fields", $data_type}
    }
}

declare_field_row! {BoolFieldRow, DbBool, "bool"}
declare_field_row! {I64FieldRow, i64, "i64"}
declare_field_row! {StringFieldRow, String, "string"}
declare_field_row! {IpAddrFieldRow, Ipv6Addr, "ipaddr"}
declare_field_row! {UuidFieldRow, Uuid, "uuid"}

macro_rules! declare_measurement_row {
    {$name:ident, $datum_type:ty, $data_type:literal} => {
        #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
        struct $name {
            timeseries_name: String,
            timeseries_key: TimeseriesKey,
            #[serde(with = "serde_timestamp")]
            timestamp: DateTime<Utc>,
            datum: $datum_type,
        }

        impl_table_name!{$name, "measurements", $data_type}
    };
}

declare_measurement_row! { BoolMeasurementRow, DbBool, "bool" }
declare_measurement_row! { I64MeasurementRow, i64, "i64" }
declare_measurement_row! { F64MeasurementRow, f64, "f64" }
declare_measurement_row! { StringMeasurementRow, String, "string" }
declare_measurement_row! { BytesMeasurementRow, Bytes, "bytes" }

macro_rules! declare_cumulative_measurement_row {
    {$name:ident, $datum_type:ty, $data_type:literal} => {
        #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
        struct $name {
            timeseries_name: String,
            timeseries_key: TimeseriesKey,
            #[serde(with = "serde_timestamp")]
            start_time: DateTime<Utc>,
            #[serde(with = "serde_timestamp")]
            timestamp: DateTime<Utc>,
            datum: $datum_type,
        }

        impl_table_name!{$name, "measurements", $data_type}
    };
}

declare_cumulative_measurement_row! { CumulativeI64MeasurementRow, i64, "cumulativei64" }
declare_cumulative_measurement_row! { CumulativeF64MeasurementRow, f64, "cumulativef64" }

// Representation of a histogram in ClickHouse.
//
// The tables storing measurements of a histogram metric use a pair of arrays to represent them,
// for the bins and counts, respectively. This handles conversion between the type used to
// represent histograms in Rust, [`Histogram`], and this in-database representation.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct DbHistogram<T> {
    pub bins: Vec<T>,
    pub counts: Vec<u64>,
}

impl<T> From<&Histogram<T>> for DbHistogram<T>
where
    T: traits::HistogramSupport,
{
    fn from(hist: &Histogram<T>) -> Self {
        let (bins, counts) = hist.to_arrays();
        Self { bins, counts }
    }
}

macro_rules! declare_histogram_measurement_row {
    {$name:ident, $datum_type:ty, $data_type:literal} => {
        #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
        struct $name {
            timeseries_name: String,
            timeseries_key: TimeseriesKey,
            #[serde(with = "serde_timestamp")]
            start_time: DateTime<Utc>,
            #[serde(with = "serde_timestamp")]
            timestamp: DateTime<Utc>,
            #[serde(flatten)]
            datum: $datum_type,
        }

        impl_table_name!{$name, "measurements", $data_type}
    };
}

declare_histogram_measurement_row! { HistogramI64MeasurementRow, DbHistogram<i64>, "histogrami64" }
declare_histogram_measurement_row! { HistogramF64MeasurementRow, DbHistogram<f64>, "histogramf64" }

// Helper to collect the field rows from a sample
fn unroll_from_source(sample: &Sample) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for field in sample.fields() {
        let timeseries_name = sample.timeseries_name.clone();
        let timeseries_key = crate::timeseries_key(sample);
        let field_name = field.name.clone();
        let (table_name, row_string) = match &field.value {
            FieldValue::Bool(inner) => {
                let row = BoolFieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: DbBool::from(*inner),
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::I64(inner) => {
                let row = I64FieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::String(inner) => {
                let row = StringFieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: inner.clone(),
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::IpAddr(inner) => {
                // TODO-completeness Be sure to map IPV6 back to IPV4 if possible when reading.
                //
                // We're using the IPV6 type in ClickHouse to store all addresses. This code maps
                // IPV4 into IPV6 in with an invertible mapping. The inversion method
                // `to_ipv4_mapped` is currently unstable, so when we get to implementing _reading_
                // of these types from the database, we can just copy that implementation. See
                // https://github.com/rust-lang/rust/issues/27709 for the tracking issue for
                // stabilizing that function, which looks like it'll happen in the near future.
                let field_value = match inner {
                    IpAddr::V4(addr) => addr.to_ipv6_mapped(),
                    IpAddr::V6(addr) => *addr,
                };
                let row = IpAddrFieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::Uuid(inner) => {
                let row = UuidFieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
        };
        out.entry(table_name).or_insert_with(Vec::new).push(row_string);
    }
    out
}

/// Collect and serialize the unrolled target and metric rows from a [`Sample`].
///
/// This method performs the main "unrolling" of a `Sample`, generating one record in the field
/// tables for each field in the sample itself. The rows are returned in a map, which stores the
/// table into which the rows should be inserted, and the string (JSON) representation of those
/// rows appropriate for actually sending to ClickHouse.
pub(crate) fn unroll_field_rows(
    sample: &Sample,
) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for (table, rows) in unroll_from_source(sample) {
        out.entry(table).or_insert_with(Vec::new).extend(rows);
    }
    out
}

/// Return the table name and serialized measurement row for a [`Sample`], to insert into
/// ClickHouse.
pub(crate) fn unroll_measurement_row(sample: &Sample) -> (String, String) {
    let timeseries_name = sample.timeseries_name.clone();
    let timeseries_key = crate::timeseries_key(sample);
    let measurement = &sample.measurement;
    let timestamp = measurement.timestamp();
    let extract_start_time = |measurement: &Measurement| {
        measurement
            .start_time()
            .expect("Cumulative measurements must have a start time")
    };
    match measurement.datum() {
        Datum::Bool(inner) => {
            let row = BoolMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum: DbBool::from(*inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::I64(inner) => {
            let row = I64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum: *inner,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::F64(inner) => {
            let row = F64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum: *inner,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::String(ref inner) => {
            let row = StringMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum: inner.clone(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::Bytes(ref inner) => {
            let row = BytesMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum: inner.clone(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::CumulativeI64(inner) => {
            let row = CumulativeI64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum: inner.value(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::CumulativeF64(inner) => {
            let row = CumulativeF64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum: inner.value(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramI64(ref inner) => {
            let row = HistogramI64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum: DbHistogram::from(inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramF64(ref inner) => {
            let row = HistogramF64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum: DbHistogram::from(inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
    }
}

/// Return the schema for a `Sample`.
pub(crate) fn schema_for(sample: &Sample) -> TimeseriesSchema {
    let created = Utc::now();
    let field_schema = sample
        .target_fields()
        .iter()
        .map(|field| FieldSchema {
            name: field.name.clone(),
            ty: field.value.field_type(),
            source: FieldSource::Target,
        })
        .chain(sample.metric_fields().iter().map(|field| FieldSchema {
            name: field.name.clone(),
            ty: field.value.field_type(),
            source: FieldSource::Metric,
        }))
        .collect();
    TimeseriesSchema {
        timeseries_name: TimeseriesName::try_from(
            sample.timeseries_name.as_str(),
        )
        .expect("Failed to parse timeseries name"),
        field_schema,
        datum_type: sample.measurement.datum_type(),
        created,
    }
}

/// Return the schema for a `Target` and `Metric`
pub(crate) fn schema_for_parts<T, M>(target: &T, metric: &M) -> TimeseriesSchema
where
    T: traits::Target,
    M: traits::Metric,
{
    let make_field_schema = |name: &str,
                             value: FieldValue,
                             source: FieldSource| {
        FieldSchema { name: name.to_string(), ty: value.field_type(), source }
    };
    let target_field_schema =
        target.field_names().iter().zip(target.field_values().into_iter());
    let metric_field_schema =
        metric.field_names().iter().zip(metric.field_values().into_iter());
    let field_schema = target_field_schema
        .map(|(name, value)| {
            make_field_schema(name, value, FieldSource::Target)
        })
        .chain(metric_field_schema.map(|(name, value)| {
            make_field_schema(name, value, FieldSource::Metric)
        }))
        .collect();
    TimeseriesSchema {
        timeseries_name: TimeseriesName::try_from(oximeter::timeseries_name(
            target, metric,
        ))
        .expect("Failed to parse timeseries name"),
        field_schema,
        datum_type: metric.datum_type(),
        created: Utc::now(),
    }
}

// A scalar timestamped sample from a gauge timeseries, as extracted from a query to the database.
#[derive(Debug, Clone, Deserialize)]
struct DbTimeseriesScalarGaugeSample<T> {
    timeseries_key: TimeseriesKey,
    #[serde(with = "serde_timestamp")]
    timestamp: DateTime<Utc>,
    datum: T,
}

// A scalar timestamped sample from a cumulative timeseries, as extracted from a query to the
// database.
#[derive(Debug, Clone, Deserialize)]
struct DbTimeseriesScalarCumulativeSample<T> {
    timeseries_key: TimeseriesKey,
    #[serde(with = "serde_timestamp")]
    start_time: DateTime<Utc>,
    #[serde(with = "serde_timestamp")]
    timestamp: DateTime<Utc>,
    datum: T,
}

// A histogram timestamped sample from a timeseries, as extracted from a query to the database.
#[derive(Debug, Clone, Deserialize)]
struct DbTimeseriesHistogramSample<T> {
    timeseries_key: TimeseriesKey,
    #[serde(with = "serde_timestamp")]
    start_time: DateTime<Utc>,
    #[serde(with = "serde_timestamp")]
    timestamp: DateTime<Utc>,
    bins: Vec<T>,
    counts: Vec<u64>,
}

impl<T> From<DbTimeseriesScalarGaugeSample<T>> for Measurement
where
    Datum: From<T>,
{
    fn from(sample: DbTimeseriesScalarGaugeSample<T>) -> Measurement {
        let datum = Datum::from(sample.datum);
        Measurement::with_timestamp(sample.timestamp, datum)
    }
}

impl<T> From<DbTimeseriesScalarCumulativeSample<T>> for Measurement
where
    Datum: From<Cumulative<T>>,
    T: traits::Cumulative,
{
    fn from(sample: DbTimeseriesScalarCumulativeSample<T>) -> Measurement {
        let cumulative =
            Cumulative::with_start_time(sample.start_time, sample.datum);
        let datum = Datum::from(cumulative);
        Measurement::with_timestamp(sample.timestamp, datum)
    }
}

impl<T> From<DbTimeseriesHistogramSample<T>> for Measurement
where
    Datum: From<Histogram<T>>,
    T: traits::HistogramSupport,
{
    fn from(sample: DbTimeseriesHistogramSample<T>) -> Measurement {
        let datum = Datum::from(
            Histogram::from_arrays(
                sample.start_time,
                sample.bins,
                sample.counts,
            )
            .unwrap(),
        );
        Measurement::with_timestamp(sample.timestamp, datum)
    }
}

fn parse_timeseries_scalar_gauge_measurement<'a, T>(
    line: &'a str,
) -> (TimeseriesKey, Measurement)
where
    T: Deserialize<'a> + Into<Datum>,
    Datum: From<T>,
{
    let sample =
        serde_json::from_str::<DbTimeseriesScalarGaugeSample<T>>(line).unwrap();
    (sample.timeseries_key, sample.into())
}

fn parse_timeseries_scalar_cumulative_measurement<'a, T>(
    line: &'a str,
) -> (TimeseriesKey, Measurement)
where
    T: Deserialize<'a> + traits::Cumulative,
    Datum: From<Cumulative<T>>,
{
    let sample =
        serde_json::from_str::<DbTimeseriesScalarCumulativeSample<T>>(line)
            .unwrap();
    (sample.timeseries_key, sample.into())
}

fn parse_timeseries_histogram_measurement<T>(
    line: &str,
) -> (TimeseriesKey, Measurement)
where
    T: Into<Datum> + traits::HistogramSupport,
    Datum: From<Histogram<T>>,
{
    let sample =
        serde_json::from_str::<DbTimeseriesHistogramSample<T>>(line).unwrap();
    (sample.timeseries_key, sample.into())
}

// Parse a line of JSON from the database resulting from `as_select_query`, into a measurement of
// the expected type. Also returns the timeseries key from the line.
pub(crate) fn parse_measurement_from_row(
    line: &str,
    datum_type: DatumType,
) -> (TimeseriesKey, Measurement) {
    match datum_type {
        DatumType::Bool => {
            parse_timeseries_scalar_gauge_measurement::<DbBool>(line)
        }
        DatumType::I64 => {
            parse_timeseries_scalar_gauge_measurement::<i64>(line)
        }
        DatumType::F64 => {
            parse_timeseries_scalar_gauge_measurement::<f64>(line)
        }
        DatumType::String => {
            parse_timeseries_scalar_gauge_measurement::<String>(line)
        }
        DatumType::Bytes => {
            parse_timeseries_scalar_gauge_measurement::<Bytes>(line)
        }
        DatumType::CumulativeI64 => {
            parse_timeseries_scalar_cumulative_measurement::<i64>(line)
        }
        DatumType::CumulativeF64 => {
            parse_timeseries_scalar_cumulative_measurement::<f64>(line)
        }
        DatumType::HistogramI64 => {
            parse_timeseries_histogram_measurement::<i64>(line)
        }
        DatumType::HistogramF64 => {
            parse_timeseries_histogram_measurement::<f64>(line)
        }
    }
}

// A single row from a query selecting timeseries with matching fields.
//
// This is used during querying for timeseries. Given a list of criteria on a timeseries's fields,
// the matching records from the various field tables are selected and JOINed. This gives one
// record per timeseries name/key, with all field values. The set of keys are then used to filter
// the actual measurements tables. This struct represents one row of the field select query.
//
// Note that the key names of `fields` are the selected column names. The actual `field_name`s and
// `field_value`s are in pairs of entries here, like `filter0.field_name`, `filter0.field_value`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FieldSelectRow<'a> {
    timeseries_key: u64,
    #[serde(flatten, borrow)]
    fields: BTreeMap<&'a str, serde_json::Value>,
}

// Convert from a FieldSelectRow to a Target and Metric, using the given schema.
//
// This asserts various conditions to check that the row actually matches the schema, and so should
// only be called after selecting fields with the same schema.
pub(crate) fn parse_field_select_row(
    row: &FieldSelectRow,
    schema: &TimeseriesSchema,
) -> (TimeseriesKey, Target, Metric) {
    assert_eq!(
        row.fields.len(),
        2 * schema.field_schema.len(),
        "Expected pairs of (field_name, field_value) from the field query"
    );
    let (target_name, metric_name) = schema.component_names();
    let mut n_fields = 0;
    let mut target_fields = Vec::new();
    let mut metric_fields = Vec::new();
    let mut actual_fields = row.fields.values();
    while n_fields < schema.field_schema.len() {
        // Extract the field name from the row and find a matching expected field.
        let actual_field_name = actual_fields
            .next()
            .expect("Missing a field name from a field select query");
        let name = actual_field_name
            .as_str()
            .expect("Expected a string field name")
            .to_string();
        let expected_field = schema.field_schema(&name).expect(
            "Found field with name that is not part of the timeseries schema",
        );

        // Parse the field value as the expected type
        let actual_field_value = actual_fields
            .next()
            .expect("Missing a field value from a field select query");
        let value = match expected_field.ty {
            FieldType::Bool => {
                FieldValue::Bool(bool::from(DbBool::from(actual_field_value.as_u64().expect("Expected a u64 for a boolean field from the database"))))
            }
            FieldType::I64 => {
                FieldValue::from(actual_field_value.as_i64().expect("Expected an i64 for an I64 field from the database"))
            }
            FieldType::IpAddr => {
                FieldValue::IpAddr(
                    actual_field_value
                        .as_str()
                        .expect("Expected an IP address string for an IpAddr field from the database")
                        .parse()
                        .expect("Invalid IP address from the database")
                    )
            }
            FieldType::Uuid => {
                FieldValue::Uuid(
                    actual_field_value
                        .as_str()
                        .expect("Expected a UUID string for a Uuid field from the database")
                        .parse()
                        .expect("Invalid UUID from the database")
                    )
            }
            FieldType::String => {
                FieldValue::String(
                    actual_field_value
                        .as_str()
                        .expect("Expected a UUID string for a Uuid field from the database")
                        .to_string()
                    )
            }
        };
        let field = Field { name, value };
        match expected_field.source {
            FieldSource::Target => target_fields.push(field),
            FieldSource::Metric => metric_fields.push(field),
        }
        n_fields += 1;
    }
    (
        row.timeseries_key,
        Target { name: target_name.to_string(), fields: target_fields },
        Metric {
            name: metric_name.to_string(),
            fields: metric_fields,
            datum_type: schema.datum_type,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use oximeter::test_util;
    use oximeter::Datum;

    #[test]
    fn test_db_bool() {
        assert!(matches!(DbBool::from(false), DbBool { inner: 0 }));
        assert!(matches!(DbBool::from(true), DbBool { inner: 1 }));
    }

    #[test]
    #[should_panic]
    fn test_db_bool_bad() {
        let _ = bool::from(DbBool { inner: 10 });
    }

    #[test]
    fn test_db_field_type_conversion() {
        macro_rules! check_conversion {
            ($variant:path, $db_variant:path) => {
                assert_eq!(DbFieldType::from($variant), $db_variant);
                assert_eq!(FieldType::from($db_variant), $variant);
            };
        }
        check_conversion!(FieldType::String, DbFieldType::String);
        check_conversion!(FieldType::I64, DbFieldType::I64);
        check_conversion!(FieldType::IpAddr, DbFieldType::IpAddr);
        check_conversion!(FieldType::Uuid, DbFieldType::Uuid);
        check_conversion!(FieldType::Bool, DbFieldType::Bool);
    }

    #[test]
    fn test_db_datum_type_conversion() {
        macro_rules! check_conversion {
            ($variant:path, $db_variant:path) => {
                assert_eq!(DbDatumType::from($variant), $db_variant);
                assert_eq!(DatumType::from($db_variant), $variant);
            };
        }
        check_conversion!(DatumType::Bool, DbDatumType::Bool);
        check_conversion!(DatumType::I64, DbDatumType::I64);
        check_conversion!(DatumType::F64, DbDatumType::F64);
        check_conversion!(DatumType::String, DbDatumType::String);
        check_conversion!(DatumType::Bytes, DbDatumType::Bytes);
        check_conversion!(DatumType::CumulativeI64, DbDatumType::CumulativeI64);
        check_conversion!(DatumType::CumulativeF64, DbDatumType::CumulativeF64);
        check_conversion!(DatumType::HistogramI64, DbDatumType::HistogramI64);
        check_conversion!(DatumType::HistogramF64, DbDatumType::HistogramF64);
    }

    #[test]
    fn test_db_field_list_conversion() {
        let db_list = DbFieldList {
            names: vec![String::from("field0"), String::from("field1")],
            types: vec![DbFieldType::I64, DbFieldType::IpAddr],
            sources: vec![DbFieldSource::Target, DbFieldSource::Metric],
        };

        let list = vec![
            FieldSchema {
                name: String::from("field0"),
                ty: FieldType::I64,
                source: FieldSource::Target,
            },
            FieldSchema {
                name: String::from("field1"),
                ty: FieldType::IpAddr,
                source: FieldSource::Metric,
            },
        ];

        assert_eq!(DbFieldList::from(list.clone()), db_list);
        assert_eq!(db_list, list.clone().into());
        let round_trip: Vec<FieldSchema> =
            DbFieldList::from(list.clone()).into();
        assert_eq!(round_trip, list);
    }

    #[test]
    fn test_db_histogram() {
        let mut hist = Histogram::new(&[0i64, 10, 20]).unwrap();
        hist.sample(1).unwrap();
        hist.sample(10).unwrap();
        let dbhist = DbHistogram::from(&hist);
        let (bins, counts) = hist.to_arrays();
        assert_eq!(dbhist.bins, bins);
        assert_eq!(dbhist.counts, counts);
    }

    #[test]
    fn test_unroll_from_source() {
        let sample = test_util::make_sample();
        let out = unroll_from_source(&sample);
        assert_eq!(out["oximeter.fields_string"].len(), 2);
        assert_eq!(out["oximeter.fields_i64"].len(), 1);
        let unpacked: StringFieldRow =
            serde_json::from_str(&out["oximeter.fields_string"][0]).unwrap();
        assert_eq!(unpacked.timeseries_name, sample.timeseries_name);
        let field = &sample.target_fields()[0];
        assert_eq!(unpacked.field_name, field.name);
        if let FieldValue::String(v) = &field.value {
            assert_eq!(v, &unpacked.field_value);
        } else {
            panic!("Expected the packed row to have a field_value matching FieldValue::String");
        }
    }

    #[test]
    fn test_unroll_measurement_row() {
        let sample = test_util::make_hist_sample();
        let (table_name, row) = unroll_measurement_row(&sample);
        assert_eq!(table_name, "oximeter.measurements_histogramf64");
        let unpacked: HistogramF64MeasurementRow =
            serde_json::from_str(&row).unwrap();
        let unpacked_hist = Histogram::from_arrays(
            unpacked.start_time,
            unpacked.datum.bins,
            unpacked.datum.counts,
        )
        .unwrap();
        let measurement = &sample.measurement;
        if let Datum::HistogramF64(hist) = measurement.datum() {
            assert_eq!(
                hist, &unpacked_hist,
                "Unpacking histogram from database representation failed"
            );
        } else {
            panic!("Expected a histogram measurement");
        }
        assert_eq!(unpacked.start_time, measurement.start_time().unwrap());
    }

    #[test]
    fn test_parse_timeseries_scalar_gauge_measurement() {
        use chrono::TimeZone;
        let timestamp = Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 123456789);

        fn run_test(line: &str, datum: &Datum, timestamp: DateTime<Utc>) {
            let (key, measurement) =
                parse_measurement_from_row(line, datum.datum_type());
            assert_eq!(key, 12);
            assert!(measurement.start_time().is_none());
            assert_eq!(measurement.timestamp(), timestamp);
            assert_eq!(measurement.datum(), datum);
        }

        let line = r#"{"timeseries_key": 12, "timestamp": "2021-01-01 00:00:00.123456789", "datum": 1 }"#;
        let datum = Datum::from(true);
        run_test(line, &datum, timestamp);

        let line = r#"{"timeseries_key": 12, "timestamp": "2021-01-01 00:00:00.123456789", "datum": 2 }"#;
        let datum = Datum::from(2);
        run_test(line, &datum, timestamp);

        let line = r#"{"timeseries_key": 12, "timestamp": "2021-01-01 00:00:00.123456789", "datum": 3.0 }"#;
        let datum = Datum::from(3.0);
        run_test(line, &datum, timestamp);
    }

    #[test]
    fn test_parse_timeseries_scalar_cumulative_measurement() {
        use chrono::TimeZone;
        let start_time = Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 123456789);
        let timestamp = Utc.ymd(2021, 1, 1).and_hms_nano(1, 0, 0, 123456789);

        fn run_test(
            line: &str,
            datum: &Datum,
            start_time: DateTime<Utc>,
            timestamp: DateTime<Utc>,
        ) {
            let (key, measurement) =
                parse_measurement_from_row(line, datum.datum_type());
            assert_eq!(key, 12);
            assert_eq!(measurement.start_time().unwrap(), start_time);
            assert_eq!(measurement.timestamp(), timestamp);
            assert_eq!(measurement.datum(), datum);
        }

        let line = r#"{"timeseries_key": 12, "start_time": "2021-01-01 00:00:00.123456789", "timestamp": "2021-01-01 01:00:00.123456789", "datum": 2 }"#;
        let cumulative = Cumulative::with_start_time(start_time, 2);
        let datum = Datum::from(cumulative);
        run_test(line, &datum, start_time, timestamp);

        let line = r#"{"timeseries_key": 12, "start_time": "2021-01-01 00:00:00.123456789", "timestamp": "2021-01-01 01:00:00.123456789", "datum": 3.0 }"#;
        let cumulative = Cumulative::with_start_time(start_time, 3.0);
        let datum = Datum::from(cumulative);
        run_test(line, &datum, start_time, timestamp);
    }

    #[test]
    #[should_panic]
    fn test_parse_bad_cumulative_json_data() {
        // Missing `start_time` field
        let line = r#"{"timeseries_key": 12, "timestamp": "2021-01-01 00:00:00.123456789", "datum": 3.0 }"#;
        let (_, _) = parse_measurement_from_row(line, DatumType::CumulativeF64);
    }

    #[test]
    fn test_parse_timeseries_histogram_measurement() {
        use chrono::TimeZone;
        let start_time = Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 123456789);
        let timestamp = Utc.ymd(2021, 1, 1).and_hms_nano(1, 0, 0, 123456789);

        let line = r#"{"timeseries_key": 12, "start_time": "2021-01-01 00:00:00.123456789", "timestamp": "2021-01-01 01:00:00.123456789", "bins": [0, 1], "counts": [1, 1] }"#;
        let (key, measurement) =
            parse_measurement_from_row(line, DatumType::HistogramI64);
        assert_eq!(key, 12);
        assert_eq!(measurement.start_time().unwrap(), start_time);
        assert_eq!(measurement.timestamp(), timestamp);
        if let Datum::HistogramI64(hist) = measurement.datum() {
            assert_eq!(hist.n_bins(), 3);
            assert_eq!(hist.n_samples(), 2);
        } else {
            panic!("Expected a histogram sample");
        }
    }

    #[test]
    fn test_parse_string_datum_requiring_escape() {
        let line = "{\"timeseries_key\": 0, \"timestamp\": \"2021-01-01 01:00:00.123456789\", \"datum\": \"\\/some\\/path\"}";
        let (_, measurement) =
            parse_measurement_from_row(line, DatumType::String);
        assert_eq!(measurement.datum(), &Datum::from("/some/path"));
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
}
