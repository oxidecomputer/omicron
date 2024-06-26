// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Models for timeseries data in ClickHouse

// Copyright 2024 Oxide Computer Company

use crate::DbFieldSource;
use crate::FieldSchema;
use crate::FieldSource;
use crate::Metric;
use crate::Target;
use crate::TimeseriesKey;
use crate::TimeseriesSchema;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use num::traits::Zero;
use oximeter::histogram::Histogram;
use oximeter::traits;
use oximeter::types::Cumulative;
use oximeter::types::Datum;
use oximeter::types::DatumType;
use oximeter::types::Field;
use oximeter::types::FieldType;
use oximeter::types::FieldValue;
use oximeter::types::Measurement;
use oximeter::types::MissingDatum;
use oximeter::types::Sample;
use oximeter::Quantile;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use uuid::Uuid;

/// Describes the version of the Oximeter database.
///
/// For usage and details see:
///
/// - [`crate::Client::initialize_db_with_version`]
/// - [`crate::Client::ensure_schema`]
/// - The `clickhouse-schema-updater` binary in this crate
pub const OXIMETER_VERSION: u64 = 5;

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
        DbBool { inner: u8::from(b) }
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
    // TODO-completeness: Populate the description from the database here. See
    // https://github.com/oxidecomputer/omicron/issues/5942 for more details.
    //#[serde(rename = "fields.description")]
    //pub descriptions: Vec<String>,
}

impl From<DbFieldList> for BTreeSet<FieldSchema> {
    fn from(list: DbFieldList) -> Self {
        list.names
            .into_iter()
            .zip(list.types)
            .zip(list.sources)
            .map(|((name, ty), source)| FieldSchema {
                name,
                field_type: ty.into(),
                source: source.into(),
                description: String::new(),
            })
            .collect()
    }
}

impl From<BTreeSet<FieldSchema>> for DbFieldList {
    fn from(list: BTreeSet<FieldSchema>) -> Self {
        let mut names = Vec::with_capacity(list.len());
        let mut types = Vec::with_capacity(list.len());
        let mut sources = Vec::with_capacity(list.len());
        for field in list.into_iter() {
            names.push(field.name.to_string());
            types.push(field.field_type.into());
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
    // TODO-completeness: Add the authorization scope, version, and units once
    // they are tracked in the database. See
    // https://github.com/oxidecomputer/omicron/issues/5942 for more details.
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

impl From<DbFieldType> for FieldType {
    fn from(src: DbFieldType) -> Self {
        match src {
            DbFieldType::String => FieldType::String,
            DbFieldType::I8 => FieldType::I8,
            DbFieldType::U8 => FieldType::U8,
            DbFieldType::I16 => FieldType::I16,
            DbFieldType::U16 => FieldType::U16,
            DbFieldType::I32 => FieldType::I32,
            DbFieldType::U32 => FieldType::U32,
            DbFieldType::I64 => FieldType::I64,
            DbFieldType::U64 => FieldType::U64,
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
            FieldType::I8 => DbFieldType::I8,
            FieldType::U8 => DbFieldType::U8,
            FieldType::I16 => DbFieldType::I16,
            FieldType::U16 => DbFieldType::U16,
            FieldType::I32 => DbFieldType::I32,
            FieldType::U32 => DbFieldType::U32,
            FieldType::I64 => DbFieldType::I64,
            FieldType::U64 => DbFieldType::U64,
            FieldType::IpAddr => DbFieldType::IpAddr,
            FieldType::Uuid => DbFieldType::Uuid,
            FieldType::Bool => DbFieldType::Bool,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum DbDatumType {
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

impl From<DatumType> for DbDatumType {
    fn from(src: DatumType) -> Self {
        match src {
            DatumType::Bool => DbDatumType::Bool,
            DatumType::I8 => DbDatumType::I8,
            DatumType::U8 => DbDatumType::U8,
            DatumType::I16 => DbDatumType::I16,
            DatumType::U16 => DbDatumType::U16,
            DatumType::I32 => DbDatumType::I32,
            DatumType::U32 => DbDatumType::U32,
            DatumType::I64 => DbDatumType::I64,
            DatumType::U64 => DbDatumType::U64,
            DatumType::F32 => DbDatumType::F32,
            DatumType::F64 => DbDatumType::F64,
            DatumType::String => DbDatumType::String,
            DatumType::Bytes => DbDatumType::Bytes,
            DatumType::CumulativeI64 => DbDatumType::CumulativeI64,
            DatumType::CumulativeU64 => DbDatumType::CumulativeU64,
            DatumType::CumulativeF32 => DbDatumType::CumulativeF32,
            DatumType::CumulativeF64 => DbDatumType::CumulativeF64,
            DatumType::HistogramI8 => DbDatumType::HistogramI8,
            DatumType::HistogramU8 => DbDatumType::HistogramU8,
            DatumType::HistogramI16 => DbDatumType::HistogramI16,
            DatumType::HistogramU16 => DbDatumType::HistogramU16,
            DatumType::HistogramI32 => DbDatumType::HistogramI32,
            DatumType::HistogramU32 => DbDatumType::HistogramU32,
            DatumType::HistogramI64 => DbDatumType::HistogramI64,
            DatumType::HistogramU64 => DbDatumType::HistogramU64,
            DatumType::HistogramF32 => DbDatumType::HistogramF32,
            DatumType::HistogramF64 => DbDatumType::HistogramF64,
        }
    }
}

impl From<DbDatumType> for DatumType {
    fn from(src: DbDatumType) -> Self {
        match src {
            DbDatumType::Bool => DatumType::Bool,
            DbDatumType::I8 => DatumType::I8,
            DbDatumType::U8 => DatumType::U8,
            DbDatumType::I16 => DatumType::I16,
            DbDatumType::U16 => DatumType::U16,
            DbDatumType::I32 => DatumType::I32,
            DbDatumType::U32 => DatumType::U32,
            DbDatumType::I64 => DatumType::I64,
            DbDatumType::U64 => DatumType::U64,
            DbDatumType::F32 => DatumType::F32,
            DbDatumType::F64 => DatumType::F64,
            DbDatumType::String => DatumType::String,
            DbDatumType::Bytes => DatumType::Bytes,
            DbDatumType::CumulativeI64 => DatumType::CumulativeI64,
            DbDatumType::CumulativeU64 => DatumType::CumulativeU64,
            DbDatumType::CumulativeF32 => DatumType::CumulativeF32,
            DbDatumType::CumulativeF64 => DatumType::CumulativeF64,
            DbDatumType::HistogramI8 => DatumType::HistogramI8,
            DbDatumType::HistogramU8 => DatumType::HistogramU8,
            DbDatumType::HistogramI16 => DatumType::HistogramI16,
            DbDatumType::HistogramU16 => DatumType::HistogramU16,
            DbDatumType::HistogramI32 => DatumType::HistogramI32,
            DbDatumType::HistogramU32 => DatumType::HistogramU32,
            DbDatumType::HistogramI64 => DatumType::HistogramI64,
            DbDatumType::HistogramU64 => DatumType::HistogramU64,
            DbDatumType::HistogramF32 => DatumType::HistogramF32,
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
    use chrono::{naive::NaiveDateTime, DateTime, Utc};
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
        NaiveDateTime::parse_from_str(&s, crate::DATABASE_TIMESTAMP_FORMAT)
            .map(|naive_date| naive_date.and_utc())
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
declare_field_row! {I8FieldRow, i8, "i8"}
declare_field_row! {U8FieldRow, u8, "u8"}
declare_field_row! {I16FieldRow, i16, "i16"}
declare_field_row! {U16FieldRow, u16, "u16"}
declare_field_row! {I32FieldRow, i32, "i32"}
declare_field_row! {U32FieldRow, u32, "u32"}
declare_field_row! {I64FieldRow, i64, "i64"}
declare_field_row! {U64FieldRow, u64, "u64"}
declare_field_row! {StringFieldRow, std::borrow::Cow<'static, str>, "string"}
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
            datum: Option<$datum_type>,
        }

        impl_table_name!{$name, "measurements", $data_type}
    };
}

declare_measurement_row! { BoolMeasurementRow, DbBool, "bool" }
declare_measurement_row! { I8MeasurementRow, i8, "i8" }
declare_measurement_row! { U8MeasurementRow, u8, "u8" }
declare_measurement_row! { I16MeasurementRow, i16, "i16" }
declare_measurement_row! { U16MeasurementRow, u16, "u16" }
declare_measurement_row! { I32MeasurementRow, i32, "i32" }
declare_measurement_row! { U32MeasurementRow, u32, "u32" }
declare_measurement_row! { I64MeasurementRow, i64, "i64" }
declare_measurement_row! { U64MeasurementRow, u64, "u64" }
declare_measurement_row! { F32MeasurementRow, f32, "f32" }
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
            datum: Option<$datum_type>,
        }

        impl_table_name!{$name, "measurements", $data_type}
    };
}

declare_cumulative_measurement_row! { CumulativeI64MeasurementRow, i64, "cumulativei64" }
declare_cumulative_measurement_row! { CumulativeU64MeasurementRow, u64, "cumulativeu64" }
declare_cumulative_measurement_row! { CumulativeF32MeasurementRow, f32, "cumulativef32" }
declare_cumulative_measurement_row! { CumulativeF64MeasurementRow, f64, "cumulativef64" }

/// A representation of all quantiles for a histogram.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
struct AllQuantiles {
    p50_marker_heights: [f64; 5],
    p50_marker_positions: [u64; 5],
    p50_desired_marker_positions: [f64; 5],

    p90_marker_heights: [f64; 5],
    p90_marker_positions: [u64; 5],
    p90_desired_marker_positions: [f64; 5],

    p99_marker_heights: [f64; 5],
    p99_marker_positions: [u64; 5],
    p99_desired_marker_positions: [f64; 5],
}

impl AllQuantiles {
    /// Create a flat `AllQuantiles` struct from the given quantiles.
    fn flatten(q50: Quantile, q90: Quantile, q99: Quantile) -> Self {
        Self {
            p50_marker_heights: q50.marker_heights(),
            p50_marker_positions: q50.marker_positions(),
            p50_desired_marker_positions: q50.desired_marker_positions(),

            p90_marker_heights: q90.marker_heights(),
            p90_marker_positions: q90.marker_positions(),
            p90_desired_marker_positions: q90.desired_marker_positions(),

            p99_marker_heights: q99.marker_heights(),
            p99_marker_positions: q99.marker_positions(),
            p99_desired_marker_positions: q99.desired_marker_positions(),
        }
    }

    /// Split the quantiles into separate `Quantile` structs in order of P.
    fn split(&self) -> (Quantile, Quantile, Quantile) {
        (
            Quantile::from_parts(
                0.5,
                self.p50_marker_heights,
                self.p50_marker_positions,
                self.p50_desired_marker_positions,
            ),
            Quantile::from_parts(
                0.9,
                self.p90_marker_heights,
                self.p90_marker_positions,
                self.p90_desired_marker_positions,
            ),
            Quantile::from_parts(
                0.99,
                self.p99_marker_heights,
                self.p99_marker_positions,
                self.p99_desired_marker_positions,
            ),
        )
    }
}

// Representation of a histogram in ClickHouse.
//
// The tables storing measurements of a histogram metric use a set of arrays to
// represent them.  This handles conversion between the type used to represent
// histograms in Rust, [`Histogram`], and this in-database representation.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct DbHistogram<T>
where
    T: traits::HistogramSupport,
{
    pub bins: Vec<T>,
    pub counts: Vec<u64>,
    pub min: T,
    pub max: T,
    pub sum_of_samples: T::Width,
    pub squared_mean: f64,
    #[serde(flatten)]
    pub quantiles: AllQuantiles,
}

// We use an empty histogram to indicate a missing sample.
//
// While ClickHouse supports nullable types, the inner type can't be a
// "composite", which includes arrays. I.e., `Nullable(Array(UInt8))` can't be
// used. This is unfortunate, but we are aided by the fact that it's not
// possible to have an `oximeter` histogram that contains zero bins right now.
// This is checked by a test in `oximeter::histogram`.
//
// That means we can currently use an empty array from the database as a
// sentinel for a missing sample.
impl<T> DbHistogram<T>
where
    T: traits::HistogramSupport,
{
    fn null() -> Self {
        let p50 = Quantile::p50();
        let p90 = Quantile::p90();
        let p99 = Quantile::p99();

        Self {
            bins: vec![],
            counts: vec![],
            min: T::zero(),
            max: T::zero(),
            sum_of_samples: T::Width::zero(),
            squared_mean: 0.0,
            quantiles: AllQuantiles::flatten(p50, p90, p99),
        }
    }
}

impl<T> From<&Histogram<T>> for DbHistogram<T>
where
    T: traits::HistogramSupport,
{
    fn from(hist: &Histogram<T>) -> Self {
        let (bins, counts) = hist.bins_and_counts();
        Self {
            bins,
            counts,
            min: hist.min(),
            max: hist.max(),
            sum_of_samples: hist.sum_of_samples(),
            squared_mean: hist.squared_mean(),
            quantiles: AllQuantiles::flatten(
                hist.p50q(),
                hist.p90q(),
                hist.p99q(),
            ),
        }
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

declare_histogram_measurement_row! { HistogramI8MeasurementRow, DbHistogram<i8>, "histogrami8" }
declare_histogram_measurement_row! { HistogramU8MeasurementRow, DbHistogram<u8>, "histogramu8" }
declare_histogram_measurement_row! { HistogramI16MeasurementRow, DbHistogram<i16>, "histogrami16" }
declare_histogram_measurement_row! { HistogramU16MeasurementRow, DbHistogram<u16>, "histogramu16" }
declare_histogram_measurement_row! { HistogramI32MeasurementRow, DbHistogram<i32>, "histogrami32" }
declare_histogram_measurement_row! { HistogramU32MeasurementRow, DbHistogram<u32>, "histogramu32" }
declare_histogram_measurement_row! { HistogramI64MeasurementRow, DbHistogram<i64>, "histogrami64" }
declare_histogram_measurement_row! { HistogramU64MeasurementRow, DbHistogram<u64>, "histogramu64" }
declare_histogram_measurement_row! { HistogramF32MeasurementRow, DbHistogram<f32>, "histogramf32" }
declare_histogram_measurement_row! { HistogramF64MeasurementRow, DbHistogram<f64>, "histogramf64" }

// Helper to collect the field rows from a sample
fn unroll_from_source(sample: &Sample) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for field in sample.fields() {
        let timeseries_name = sample.timeseries_name.to_string();
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
            FieldValue::I8(inner) => {
                let row = I8FieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::U8(inner) => {
                let row = U8FieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::I16(inner) => {
                let row = I16FieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::U16(inner) => {
                let row = U16FieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::I32(inner) => {
                let row = I32FieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::U32(inner) => {
                let row = U32FieldRow {
                    timeseries_name,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
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
            FieldValue::U64(inner) => {
                let row = U64FieldRow {
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
                // We're using the IPv6 type in ClickHouse to store all
                // addresses. This code maps any IPv4 address to IPv6 using an
                // invertible mapping. We map things back, if possible, on the
                // way out of the database.
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
    unroll_measurement_row_impl(
        timeseries_name.to_string(),
        timeseries_key,
        measurement,
    )
}

/// Given a sample's measurement, return a table name and row to insert.
///
/// This returns a tuple giving the name of the table, and the JSON
/// representation for the serialized row to be inserted into that table,
/// written out as a string.
pub(crate) fn unroll_measurement_row_impl(
    timeseries_name: String,
    timeseries_key: TimeseriesKey,
    measurement: &Measurement,
) -> (String, String) {
    let timestamp = measurement.timestamp();
    let extract_start_time = |measurement: &Measurement| {
        measurement
            .start_time()
            .expect("Cumulative measurements must have a start time")
    };

    match measurement.datum() {
        Datum::Bool(inner) => {
            let datum = Some(DbBool::from(*inner));
            let row = BoolMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::I8(inner) => {
            let datum = Some(*inner);
            let row = I8MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::U8(inner) => {
            let datum = Some(*inner);
            let row = U8MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::I16(inner) => {
            let datum = Some(*inner);
            let row = I16MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::U16(inner) => {
            let datum = Some(*inner);
            let row = U16MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::I32(inner) => {
            let datum = Some(*inner);
            let row = I32MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::U32(inner) => {
            let datum = Some(*inner);
            let row = U32MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::I64(inner) => {
            let datum = Some(*inner);
            let row = I64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::U64(inner) => {
            let datum = Some(*inner);
            let row = U64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::F32(inner) => {
            let datum = Some(*inner);
            let row = F32MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::F64(inner) => {
            let datum = Some(*inner);
            let row = F64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::String(inner) => {
            let datum = Some(inner.clone());
            let row = StringMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::Bytes(inner) => {
            let datum = Some(inner.clone());
            let row = BytesMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::CumulativeI64(inner) => {
            let datum = Some(inner.value());
            let row = CumulativeI64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::CumulativeU64(inner) => {
            let datum = Some(inner.value());
            let row = CumulativeU64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::CumulativeF32(inner) => {
            let datum = Some(inner.value());
            let row = CumulativeF32MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::CumulativeF64(inner) => {
            let datum = Some(inner.value());
            let row = CumulativeF64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramI8(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramI8MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramU8(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramU8MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramI16(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramI16MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramU16(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramU16MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramI32(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramI32MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramU32(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramU32MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramI64(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramI64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramU64(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramU64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramF32(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramF32MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::HistogramF64(inner) => {
            let datum = DbHistogram::from(inner);
            let row = HistogramF64MeasurementRow {
                timeseries_name,
                timeseries_key,
                start_time: extract_start_time(measurement),
                timestamp,
                datum,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Datum::Missing(missing) => {
            match missing.datum_type() {
                DatumType::Bool => {
                    let row = BoolMeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::I8 => {
                    let row = I8MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::U8 => {
                    let row = U8MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::I16 => {
                    let row = I16MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::U16 => {
                    let row = U16MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::I32 => {
                    let row = I32MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::U32 => {
                    let row = U32MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::I64 => {
                    let row = I64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::U64 => {
                    let row = U64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::F32 => {
                    let row = F32MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::F64 => {
                    let row = F64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::String => {
                    let row = StringMeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::Bytes => {
                    // See https://github.com/oxidecomputer/omicron/issues/4551.
                    //
                    // This is actually unreachable today because the constuctor
                    // for `oximeter::types::MissingDatum` fails when using a
                    // `DatumType::Bytes`.
                    unreachable!();
                }
                DatumType::CumulativeI64 => {
                    let row = CumulativeI64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::CumulativeU64 => {
                    let row = CumulativeU64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::CumulativeF32 => {
                    let row = CumulativeF32MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::CumulativeF64 => {
                    let row = CumulativeF64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: None,
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramI8 => {
                    let row = HistogramI8MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramU8 => {
                    let row = HistogramU8MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramI16 => {
                    let row = HistogramI16MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramU16 => {
                    let row = HistogramU16MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramI32 => {
                    let row = HistogramI32MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramU32 => {
                    let row = HistogramU32MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramI64 => {
                    let row = HistogramI64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramU64 => {
                    let row = HistogramU64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramF32 => {
                    let row = HistogramF32MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
                DatumType::HistogramF64 => {
                    let row = HistogramF64MeasurementRow {
                        timeseries_name,
                        timeseries_key,
                        start_time: extract_start_time(measurement),
                        timestamp,
                        datum: DbHistogram::null(),
                    };
                    (row.table_name(), serde_json::to_string(&row).unwrap())
                }
            }
        }
    }
}

// A scalar timestamped sample from a gauge timeseries, as extracted from a query to the database.
#[derive(Debug, Clone, Deserialize)]
struct DbTimeseriesScalarGaugeSample<T> {
    timeseries_key: TimeseriesKey,
    #[serde(with = "serde_timestamp")]
    timestamp: DateTime<Utc>,
    datum: Option<T>,
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
    datum: Option<T>,
}

// A histogram timestamped sample from a timeseries, as extracted from a query to the database.
#[derive(Debug, Clone, Deserialize)]
struct DbTimeseriesHistogramSample<T>
where
    T: traits::HistogramSupport,
{
    timeseries_key: TimeseriesKey,
    #[serde(with = "serde_timestamp")]
    start_time: DateTime<Utc>,
    #[serde(with = "serde_timestamp")]
    timestamp: DateTime<Utc>,
    bins: Vec<T>,
    counts: Vec<u64>,
    min: T,
    max: T,
    sum_of_samples: T::Width,
    squared_mean: f64,
    #[serde(flatten)]
    quantiles: AllQuantiles,
}

impl<T> From<DbTimeseriesScalarGaugeSample<T>> for Measurement
where
    Datum: From<T>,
    T: FromDbScalar,
{
    fn from(sample: DbTimeseriesScalarGaugeSample<T>) -> Measurement {
        let datum = match sample.datum {
            Some(datum) => Datum::from(datum),
            None => {
                Datum::Missing(MissingDatum::new(T::DATUM_TYPE, None).unwrap())
            }
        };
        Measurement::new(sample.timestamp, datum)
    }
}

impl<T> From<DbTimeseriesScalarCumulativeSample<T>> for Measurement
where
    Datum: From<Cumulative<T>>,
    T: traits::Cumulative + FromDbCumulative,
{
    fn from(sample: DbTimeseriesScalarCumulativeSample<T>) -> Measurement {
        let datum = match sample.datum {
            Some(datum) => Datum::from(Cumulative::with_start_time(
                sample.start_time,
                datum,
            )),
            None => Datum::Missing(
                MissingDatum::new(T::DATUM_TYPE, Some(sample.start_time))
                    .unwrap(),
            ),
        };
        Measurement::new(sample.timestamp, datum)
    }
}

impl<T> From<DbTimeseriesHistogramSample<T>> for Measurement
where
    Datum: From<Histogram<T>>,
    T: traits::HistogramSupport + FromDbHistogram,
{
    fn from(sample: DbTimeseriesHistogramSample<T>) -> Measurement {
        let datum = if sample.bins.is_empty() {
            assert!(sample.counts.is_empty());
            Datum::Missing(
                MissingDatum::new(T::DATUM_TYPE, Some(sample.start_time))
                    .unwrap(),
            )
        } else {
            if sample.bins.len() != sample.counts.len() {
                panic!(
                    "Array size mismatch: bins: {}, counts: {}",
                    sample.bins.len(),
                    sample.counts.len()
                );
            }

            let (p50, p90, p99) = sample.quantiles.split();
            let hist = Histogram::from_parts(
                sample.start_time,
                sample.bins,
                sample.counts,
                sample.min,
                sample.max,
                sample.sum_of_samples,
                sample.squared_mean,
                p50,
                p90,
                p99,
            )
            .unwrap();

            Datum::from(hist)
        };
        Measurement::new(sample.timestamp, datum)
    }
}

// Helper trait providing the DatumType for a corresponding scalar DB value.
//
// This is used in `parse_timeseries_scalar_gauge_measurement`.
trait FromDbScalar {
    const DATUM_TYPE: DatumType;
}

impl FromDbScalar for DbBool {
    const DATUM_TYPE: DatumType = DatumType::Bool;
}

impl FromDbScalar for i8 {
    const DATUM_TYPE: DatumType = DatumType::I8;
}

impl FromDbScalar for u8 {
    const DATUM_TYPE: DatumType = DatumType::U8;
}

impl FromDbScalar for i16 {
    const DATUM_TYPE: DatumType = DatumType::I16;
}

impl FromDbScalar for u16 {
    const DATUM_TYPE: DatumType = DatumType::U16;
}

impl FromDbScalar for i32 {
    const DATUM_TYPE: DatumType = DatumType::I32;
}

impl FromDbScalar for u32 {
    const DATUM_TYPE: DatumType = DatumType::U32;
}

impl FromDbScalar for i64 {
    const DATUM_TYPE: DatumType = DatumType::I64;
}

impl FromDbScalar for u64 {
    const DATUM_TYPE: DatumType = DatumType::U64;
}

impl FromDbScalar for f32 {
    const DATUM_TYPE: DatumType = DatumType::F32;
}

impl FromDbScalar for f64 {
    const DATUM_TYPE: DatumType = DatumType::F64;
}

impl FromDbScalar for String {
    const DATUM_TYPE: DatumType = DatumType::String;
}

impl FromDbScalar for Bytes {
    const DATUM_TYPE: DatumType = DatumType::Bytes;
}

trait FromDbCumulative {
    const DATUM_TYPE: DatumType;
}

impl FromDbCumulative for i64 {
    const DATUM_TYPE: DatumType = DatumType::CumulativeI64;
}

impl FromDbCumulative for u64 {
    const DATUM_TYPE: DatumType = DatumType::CumulativeU64;
}

impl FromDbCumulative for f32 {
    const DATUM_TYPE: DatumType = DatumType::CumulativeF32;
}

impl FromDbCumulative for f64 {
    const DATUM_TYPE: DatumType = DatumType::CumulativeF64;
}

trait FromDbHistogram {
    const DATUM_TYPE: DatumType;
}

impl FromDbHistogram for i8 {
    const DATUM_TYPE: DatumType = DatumType::HistogramI8;
}

impl FromDbHistogram for u8 {
    const DATUM_TYPE: DatumType = DatumType::HistogramU8;
}

impl FromDbHistogram for i16 {
    const DATUM_TYPE: DatumType = DatumType::HistogramI16;
}

impl FromDbHistogram for u16 {
    const DATUM_TYPE: DatumType = DatumType::HistogramU16;
}

impl FromDbHistogram for i32 {
    const DATUM_TYPE: DatumType = DatumType::HistogramI32;
}

impl FromDbHistogram for u32 {
    const DATUM_TYPE: DatumType = DatumType::HistogramU32;
}

impl FromDbHistogram for i64 {
    const DATUM_TYPE: DatumType = DatumType::HistogramI64;
}

impl FromDbHistogram for u64 {
    const DATUM_TYPE: DatumType = DatumType::HistogramU64;
}

impl FromDbHistogram for f32 {
    const DATUM_TYPE: DatumType = DatumType::HistogramF32;
}

impl FromDbHistogram for f64 {
    const DATUM_TYPE: DatumType = DatumType::HistogramF64;
}

fn parse_timeseries_scalar_gauge_measurement<'a, T>(
    line: &'a str,
) -> (TimeseriesKey, Measurement)
where
    T: Deserialize<'a> + Into<Datum> + FromDbScalar,
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
    T: Deserialize<'a> + traits::Cumulative + FromDbCumulative,
    Datum: From<Cumulative<T>>,
{
    let sample =
        serde_json::from_str::<DbTimeseriesScalarCumulativeSample<T>>(line)
            .unwrap();
    (sample.timeseries_key, sample.into())
}

fn parse_timeseries_histogram_measurement<'a, T>(
    line: &'a str,
) -> (TimeseriesKey, Measurement)
where
    T: Into<Datum>
        + traits::HistogramSupport
        + FromDbHistogram
        + Deserialize<'a>,
    Datum: From<Histogram<T>>,
    <T as traits::HistogramSupport>::Width: Deserialize<'a>,
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
        DatumType::I8 => parse_timeseries_scalar_gauge_measurement::<i8>(line),
        DatumType::U8 => parse_timeseries_scalar_gauge_measurement::<u8>(line),
        DatumType::I16 => {
            parse_timeseries_scalar_gauge_measurement::<i16>(line)
        }
        DatumType::U16 => {
            parse_timeseries_scalar_gauge_measurement::<u16>(line)
        }
        DatumType::I32 => {
            parse_timeseries_scalar_gauge_measurement::<i32>(line)
        }
        DatumType::U32 => {
            parse_timeseries_scalar_gauge_measurement::<u32>(line)
        }
        DatumType::I64 => {
            parse_timeseries_scalar_gauge_measurement::<i64>(line)
        }
        DatumType::U64 => {
            parse_timeseries_scalar_gauge_measurement::<u64>(line)
        }
        DatumType::F32 => {
            parse_timeseries_scalar_gauge_measurement::<f32>(line)
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
        DatumType::CumulativeU64 => {
            parse_timeseries_scalar_cumulative_measurement::<u64>(line)
        }
        DatumType::CumulativeF32 => {
            parse_timeseries_scalar_cumulative_measurement::<f32>(line)
        }
        DatumType::CumulativeF64 => {
            parse_timeseries_scalar_cumulative_measurement::<f64>(line)
        }
        DatumType::HistogramI8 => {
            parse_timeseries_histogram_measurement::<i8>(line)
        }
        DatumType::HistogramU8 => {
            parse_timeseries_histogram_measurement::<u8>(line)
        }
        DatumType::HistogramI16 => {
            parse_timeseries_histogram_measurement::<i16>(line)
        }
        DatumType::HistogramU16 => {
            parse_timeseries_histogram_measurement::<u16>(line)
        }
        DatumType::HistogramI32 => {
            parse_timeseries_histogram_measurement::<i32>(line)
        }
        DatumType::HistogramU32 => {
            parse_timeseries_histogram_measurement::<u32>(line)
        }
        DatumType::HistogramI64 => {
            parse_timeseries_histogram_measurement::<i64>(line)
        }
        DatumType::HistogramU64 => {
            parse_timeseries_histogram_measurement::<u64>(line)
        }
        DatumType::HistogramF32 => {
            parse_timeseries_histogram_measurement::<f32>(line)
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
        schema.field_schema.len(),
        "Expected the same number of fields in each row as the schema itself",
    );
    let (target_name, metric_name) = schema.component_names();
    let mut target_fields = Vec::new();
    let mut metric_fields = Vec::new();
    let mut actual_fields = row.fields.iter();
    for _ in 0..schema.field_schema.len() {
        // Extract the field name from the row and find a matching expected field.
        let (actual_field_name, actual_field_value) = actual_fields
            .next()
            .expect("Missing a field name from a field select query");
        let expected_field = schema.schema_for_field(actual_field_name).expect(
            "Found field with name that is not part of the timeseries schema",
        );

        // Parse the field value as the expected type
        let value = match expected_field.field_type {
            FieldType::Bool => {
                FieldValue::Bool(bool::from(DbBool::from(
                    actual_field_value
                        .as_u64()
                        .expect("Expected a u64 for a boolean field from the database")
                )))
            }
            FieldType::I8 => {
                let wide = actual_field_value
                    .as_i64()
                    .expect("Expected an i64 from the database for an I8 field");
                let narrow = i8::try_from(wide)
                    .expect("Expected a valid i8 for an I8 field from the database");
                FieldValue::from(narrow)
            }
            FieldType::U8 => {
                let wide = actual_field_value
                    .as_u64()
                    .expect("Expected a u64 from the database for a U8 field");
                let narrow = u8::try_from(wide)
                    .expect("Expected a valid u8 for a U8 field from the database");
                FieldValue::from(narrow)
            }
            FieldType::I16 => {
                let wide = actual_field_value
                    .as_i64()
                    .expect("Expected an i64 from the database for an I16 field");
                let narrow = i16::try_from(wide)
                    .expect("Expected a valid i16 for an I16 field from the database");
                FieldValue::from(narrow)
            }
            FieldType::U16 => {
                let wide = actual_field_value
                    .as_u64()
                    .expect("Expected a u64 from the database for a U16 field");
                let narrow = u16::try_from(wide)
                    .expect("Expected a valid u16 for a U16 field from the database");
                FieldValue::from(narrow)
            }
            FieldType::I32 => {
                let wide = actual_field_value
                    .as_i64()
                    .expect("Expected an i64 from the database for an I32 field");
                let narrow = i32::try_from(wide)
                    .expect("Expected a valid i32 for an I32 field from the database");
                FieldValue::from(narrow)
            }
            FieldType::U32 => {
                let wide = actual_field_value
                    .as_u64()
                    .expect("Expected a u64 from the database for a U16 field");
                let narrow = u32::try_from(wide)
                    .expect("Expected a valid u32 for a U32 field from the database");
                FieldValue::from(narrow)
            }
            FieldType::I64 => {
                FieldValue::from(
                    actual_field_value
                        .as_i64()
                        .expect("Expected an i64 for an I64 field from the database")
                )
            }
            FieldType::U64 => {
                FieldValue::from(
                    actual_field_value
                        .as_u64()
                        .expect("Expected a u64 for a U64 field from the database")
                )
            }
            FieldType::IpAddr => {
                // We store values in the database as IPv6, by mapping IPv4 into
                // that space. This tries to invert the mapping. If that
                // succeeds, we know we stored an IPv4 address in the table.
                let always_v6: Ipv6Addr = actual_field_value
                    .as_str()
                    .expect("Expected an IP address string for an IpAddr field from the database")
                    .parse()
                    .expect("Invalid IP address from the database");
                match always_v6.to_ipv4_mapped() {
                    Some(v4) => FieldValue::IpAddr(IpAddr::V4(v4)),
                    None => FieldValue::IpAddr(IpAddr::V6(always_v6)),
                }
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
                        .into()
                    )
            }
        };
        let field = Field { name: actual_field_name.to_string(), value };
        match expected_field.source {
            FieldSource::Target => target_fields.push(field),
            FieldSource::Metric => metric_fields.push(field),
        }
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
    use chrono::Timelike;
    use oximeter::histogram::Record;
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

        let list: BTreeSet<_> = [
            FieldSchema {
                name: String::from("field0"),
                field_type: FieldType::I64,
                source: FieldSource::Target,
                description: String::new(),
            },
            FieldSchema {
                name: String::from("field1"),
                field_type: FieldType::IpAddr,
                source: FieldSource::Metric,
                description: String::new(),
            },
        ]
        .into_iter()
        .collect();

        assert_eq!(DbFieldList::from(list.clone()), db_list);
        assert_eq!(db_list, list.clone().into());
        let round_trip: BTreeSet<FieldSchema> =
            DbFieldList::from(list.clone()).into();
        assert_eq!(round_trip, list);
    }

    #[test]
    fn test_db_histogram() {
        let mut hist = Histogram::new(&[0i64, 10, 20]).unwrap();
        hist.sample(1).unwrap();
        hist.sample(10).unwrap();
        let dbhist = DbHistogram::from(&hist);
        let (bins, counts) = hist.bins_and_counts();
        assert_eq!(dbhist.bins, bins);
        assert_eq!(dbhist.counts, counts);
        assert_eq!(dbhist.min, hist.min());
        assert_eq!(dbhist.max, hist.max());
        assert_eq!(dbhist.sum_of_samples, hist.sum_of_samples());
        assert_eq!(dbhist.squared_mean, hist.squared_mean());

        let (p50, p90, p99) = dbhist.quantiles.split();
        assert_eq!(p50, hist.p50q());
        assert_eq!(p90, hist.p90q());
        assert_eq!(p99, hist.p99q());
    }

    #[test]
    fn test_unroll_from_source() {
        let sample = test_util::make_sample();
        let out = unroll_from_source(&sample);
        assert_eq!(out["oximeter.fields_string"].len(), 2);
        assert_eq!(out["oximeter.fields_i64"].len(), 1);
        let unpacked: StringFieldRow =
            serde_json::from_str(&out["oximeter.fields_string"][0]).unwrap();
        assert_eq!(sample.timeseries_name, unpacked.timeseries_name);
        let field = sample.target_fields().next().unwrap();
        assert_eq!(unpacked.field_name, field.name);
        if let FieldValue::String(v) = &field.value {
            assert_eq!(v, &unpacked.field_value);
        } else {
            panic!("Expected the packed row to have a field_value matching FieldValue::String");
        }
    }

    // Test that we correctly unroll a row when the measurement is missing its
    // datum.
    #[test]
    fn test_unroll_missing_measurement_row() {
        let sample = test_util::make_sample();
        let missing_sample = test_util::make_missing_sample();
        let (table_name, row) = unroll_measurement_row(&sample);
        let (missing_table_name, missing_row) =
            unroll_measurement_row(&missing_sample);
        let row = serde_json::from_str::<I64MeasurementRow>(&row).unwrap();
        let missing_row =
            serde_json::from_str::<I64MeasurementRow>(&missing_row).unwrap();
        println!("{row:#?}");
        println!("{missing_row:#?}");
        assert_eq!(table_name, missing_table_name);
        assert_eq!(row.timeseries_name, missing_row.timeseries_name);
        assert_eq!(row.timeseries_key, missing_row.timeseries_key);
        assert!(row.datum.is_some());
        assert!(missing_row.datum.is_none());
    }

    #[test]
    fn test_unroll_measurement_row() {
        let sample = test_util::make_hist_sample();
        let (table_name, row) = unroll_measurement_row(&sample);
        assert_eq!(table_name, "oximeter.measurements_histogramf64");
        let unpacked: HistogramF64MeasurementRow =
            serde_json::from_str(&row).unwrap();
        let (unpacked_p50, unpacked_p90, unpacked_p99) =
            unpacked.datum.quantiles.split();

        let unpacked_hist = Histogram::from_parts(
            unpacked.start_time,
            unpacked.datum.bins,
            unpacked.datum.counts,
            unpacked.datum.min,
            unpacked.datum.max,
            unpacked.datum.sum_of_samples,
            unpacked.datum.squared_mean,
            unpacked_p50,
            unpacked_p90,
            unpacked_p99,
        )
        .unwrap();
        let measurement = &sample.measurement;
        let Datum::HistogramF64(hist) = measurement.datum() else {
            panic!("Expected a histogram measurement");
        };
        assert_eq!(
            hist, &unpacked_hist,
            "Unpacking histogram from database representation failed"
        );
        assert_eq!(unpacked.start_time, measurement.start_time().unwrap());
    }

    #[test]
    fn test_parse_timeseries_scalar_gauge_measurement() {
        use chrono::TimeZone;
        let timestamp = Utc
            .with_ymd_and_hms(2021, 1, 1, 0, 0, 0)
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();

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
        let start_time = Utc
            .with_ymd_and_hms(2021, 1, 1, 0, 0, 0)
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();
        let timestamp = Utc
            .with_ymd_and_hms(2021, 1, 1, 1, 0, 0)
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();

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
        let cumulative = Cumulative::with_start_time(start_time, 2u64);
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
        let start_time = Utc
            .with_ymd_and_hms(2021, 1, 1, 0, 0, 0)
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();
        let timestamp = Utc
            .with_ymd_and_hms(2021, 1, 1, 1, 0, 0)
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();

        let line = r#"
        {
            "timeseries_key": 12,
            "start_time": "2021-01-01 00:00:00.123456789",
            "timestamp": "2021-01-01 01:00:00.123456789",
            "bins": [0, 1],
            "counts": [1, 1],
            "min": 0,
            "max": 1,
            "sum_of_samples": 2,
            "squared_mean": 2.0,
            "p50_marker_heights": [0.0, 0.0, 0.0, 0.0, 1.0],
            "p50_marker_positions": [1, 2, 3, 4, 2],
            "p50_desired_marker_positions": [1.0, 3.0, 5.0, 5.0, 5.0],
            "p90_marker_heights": [0.0, 0.0, 0.0, 0.0, 1.0],
            "p90_marker_positions": [1, 2, 3, 4, 2],
            "p90_desired_marker_positions": [1.0, 3.0, 5.0, 5.0, 5.0],
            "p99_marker_heights": [0.0, 0.0, 0.0, 0.0, 1.0],
            "p99_marker_positions": [1, 2, 3, 4, 2],
            "p99_desired_marker_positions": [1.0, 3.0, 5.0, 5.0, 5.0]
        }"#;
        let (key, measurement) =
            parse_measurement_from_row(line, DatumType::HistogramI64);
        assert_eq!(key, 12);
        assert_eq!(measurement.start_time().unwrap(), start_time);
        assert_eq!(measurement.timestamp(), timestamp);
        let Datum::HistogramI64(hist) = measurement.datum() else {
            panic!("Expected a histogram sample");
        };
        assert_eq!(hist.n_bins(), 3);
        assert_eq!(hist.n_samples(), 2);
        assert_eq!(hist.min(), 0);
        assert_eq!(hist.max(), 1);
        assert_eq!(hist.sum_of_samples(), 2);
        assert_eq!(hist.squared_mean(), 2.);
        assert_eq!(
            hist.p50q(),
            Quantile::from_parts(
                0.5,
                [0.0, 0.0, 0.0, 0.0, 1.0],
                [1, 2, 3, 4, 2],
                [1.0, 3.0, 5.0, 5.0, 5.0],
            )
        );
        assert_eq!(
            hist.p90q(),
            Quantile::from_parts(
                0.9,
                [0.0, 0.0, 0.0, 0.0, 1.0],
                [1, 2, 3, 4, 2],
                [1.0, 3.0, 5.0, 5.0, 5.0],
            )
        );

        assert_eq!(
            hist.p99q(),
            Quantile::from_parts(
                0.99,
                [0.0, 0.0, 0.0, 0.0, 1.0],
                [1, 2, 3, 4, 2],
                [1.0, 3.0, 5.0, 5.0, 5.0],
            )
        );
    }

    #[test]
    fn test_parse_string_datum_requiring_escape() {
        let line = "{\"timeseries_key\": 0, \"timestamp\": \"2021-01-01 01:00:00.123456789\", \"datum\": \"\\/some\\/path\"}";
        let (_, measurement) =
            parse_measurement_from_row(line, DatumType::String);
        assert_eq!(measurement.datum(), &Datum::from("/some/path"));
    }

    #[test]
    fn test_parse_bytes_measurement() {
        let s = r#"{"timeseries_key": 101, "timestamp": "2023-11-21 18:25:21.963714255", "datum": "\u0001\u0002\u0003"}"#;
        let (_, meas) = parse_timeseries_scalar_gauge_measurement::<Bytes>(&s);
        println!("{meas:?}");
        let Datum::Bytes(b) = meas.datum() else {
            unreachable!();
        };
        assert_eq!(b.to_vec(), vec![1, 2, 3]);
    }
}
