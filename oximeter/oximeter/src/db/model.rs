//! Models for timeseries data in ClickHouse
// Copyright 2021 Oxide Computer Company

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::histogram;
use crate::types::{
    self, Cumulative, Error, FieldType, FieldValue, Measurement,
    MeasurementType, Sample,
};

// TODO-completeness This module implements and tests paths for inserting data into ClickHouse, but
// not for reading it out. It's not yet clear how we'll be querying the database, but this is a
// large open area.

/// The name of the database storing all metric information.
pub const DATABASE_NAME: &str = "oximeter";

/// Wrapper type to represent a boolean in the database.
///
/// ClickHouse's type system lacks a boolean, and using `u8` to represent them. This a safe wrapper
/// type around that for serializing to/from the database.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct DbBool {
    inner: u8,
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

/// The source, target or metric, from which a field is derived.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub enum FieldSource {
    Target,
    Metric,
}

/// Information about a target or metric field as contained in a schema.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Field {
    pub name: String,
    pub ty: FieldType,
    pub source: FieldSource,
}

// The list of fields in a schema as represented in the actual schema tables in the database.
//
// Data about the schema in ClickHouse is represented in the `timeseries_schema` table. this
// contains the fields as a nested table, which is stored as a struct of arrays. This type is used
// to convert between that representation in the database, and the representation we prefer, i.e.,
// the `Field` type above. In other words, we prefer to work with an array of structs, but
// ClickHouse requires a struct of arrays. `Field` is the former, `DbFieldList` is the latter.
//
// Note that the fields are renamed so that ClickHouse interprets them as correctly referring to
// the nested column names.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub(crate) struct DbFieldList {
    #[serde(rename = "fields.name")]
    pub names: Vec<String>,
    #[serde(rename = "fields.type")]
    pub types: Vec<FieldType>,
    #[serde(rename = "fields.source")]
    pub sources: Vec<FieldSource>,
}

impl From<DbFieldList> for Vec<Field> {
    fn from(list: DbFieldList) -> Self {
        list.names
            .into_iter()
            .zip(list.types.into_iter())
            .zip(list.sources.into_iter())
            .map(|((name, ty), source)| Field { name, ty, source })
            .collect()
    }
}

impl From<Vec<Field>> for DbFieldList {
    fn from(list: Vec<Field>) -> Self {
        let mut names = Vec::with_capacity(list.len());
        let mut types = Vec::with_capacity(list.len());
        let mut sources = Vec::with_capacity(list.len());
        for field in list.into_iter() {
            names.push(field.name);
            types.push(field.ty);
            sources.push(field.source);
        }
        DbFieldList { names, types, sources }
    }
}

/// The `TimeseriesSchema` struct represents the schema of a timeseries.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimeseriesSchema {
    pub timeseries_name: String,
    pub fields: Vec<Field>,
    pub measurement_type: MeasurementType,
    pub created: DateTime<Utc>,
}

impl PartialEq for TimeseriesSchema {
    fn eq(&self, other: &TimeseriesSchema) -> bool {
        self.timeseries_name == other.timeseries_name
            && self.measurement_type == other.measurement_type
            && self.fields == other.fields
    }
}

// The `DbTimeseriesSchema` type models the `oximeter.timeseries_schema` table.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DbTimeseriesSchema {
    pub timeseries_name: String,
    #[serde(flatten)]
    pub fields: DbFieldList,
    pub measurement_type: MeasurementType,
    #[serde(with = "serde_timestamp")]
    pub created: DateTime<Utc>,
}

impl From<DbTimeseriesSchema> for TimeseriesSchema {
    fn from(schema: DbTimeseriesSchema) -> TimeseriesSchema {
        TimeseriesSchema {
            timeseries_name: schema.timeseries_name,
            fields: schema.fields.into(),
            measurement_type: schema.measurement_type,
            created: schema.created,
        }
    }
}

impl From<TimeseriesSchema> for DbTimeseriesSchema {
    fn from(schema: TimeseriesSchema) -> DbTimeseriesSchema {
        DbTimeseriesSchema {
            timeseries_name: schema.timeseries_name,
            fields: schema.fields.into(),
            measurement_type: schema.measurement_type,
            created: schema.created,
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
// microseconds in our case. We opt for strings here, since we're using that anyway in the
// input/output format for ClickHouse.
mod serde_timestamp {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";

    pub fn serialize<S>(
        date: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

macro_rules! define_field_row {
    {$name:ident, $value_type:ty, $table_suffix:literal} => {
        #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
        pub struct $name {
            pub timeseries_name: String,
            pub timeseries_key: String,
            pub field_name: String,
            pub field_value: $value_type,
        }

        impl $name {
            pub fn table_name(&self) -> String {
                format!(
                    "{db_name}.fields_{type_}",
                    db_name = DATABASE_NAME,
                    type_ = $table_suffix,
                )
            }
        }
    }
}

define_field_row! {BoolFieldRow, DbBool, "bool"}
define_field_row! {I64FieldRow, i64, "i64"}
define_field_row! {StringFieldRow, String, "string"}
define_field_row! {IpAddrFieldRow, Ipv6Addr, "ipaddr"}
define_field_row! {UuidFieldRow, Uuid, "uuid"}

macro_rules! define_measurement_row {
    {$name:ident, $value_type:ty, $table_suffix:literal} => {
        #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
        pub struct $name {
            pub timeseries_name: String,
            pub timeseries_key: String,
            #[serde(with = "serde_timestamp")]
            pub timestamp: DateTime<Utc>,
            pub value: $value_type,
        }

        impl $name {
            pub fn table_name(&self) -> String {
                format!(
                    "{db_name}.measurements_{type_}",
                    db_name = DATABASE_NAME,
                    type_ = $table_suffix,
                )
            }
        }
    };
}

define_measurement_row! { BoolMeasurementRow, DbBool, "bool" }
define_measurement_row! { I64MeasurementRow, i64, "i64" }
define_measurement_row! { F64MeasurementRow, f64, "f64" }
define_measurement_row! { StringMeasurementRow, String, "string" }
define_measurement_row! { BytesMeasurementRow, Bytes, "bytes" }
define_measurement_row! { CumulativeI64MeasurementRow, i64, "cumulativei64" }
define_measurement_row! { CumulativeF64MeasurementRow, f64, "cumulativef64" }

/// Representation of a histogram in ClickHouse.
///
/// The tables storing measurements of a histogram metric use a pair of arrays to represent them,
/// for the bins and counts, respectively. This handles conversion between the type used to
/// represent histograms in Rust, [`histogram::Histogram`], and this in-database representation.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct DbHistogram<T> {
    pub bins: Vec<T>,
    pub counts: Vec<u64>,
}

impl<T> From<&histogram::Histogram<T>> for DbHistogram<T>
where
    T: histogram::HistogramSupport,
{
    fn from(hist: &histogram::Histogram<T>) -> Self {
        let (bins, counts) = hist.to_arrays();
        Self { bins, counts }
    }
}

impl<T> From<DbHistogram<T>> for histogram::Histogram<T>
where
    T: histogram::HistogramSupport,
{
    fn from(hist: DbHistogram<T>) -> Self {
        Self::from_arrays(hist.bins, hist.counts).unwrap()
    }
}

macro_rules! define_histogram_measurement_row {
    {$name:ident, $value_type:ty, $table_suffix:literal} => {
        #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
        pub struct $name {
            pub timeseries_name: String,
            pub timeseries_key: String,
            #[serde(with = "serde_timestamp")]
            pub timestamp: DateTime<Utc>,
            #[serde(flatten)]
            pub value: $value_type,
        }

        impl $name {
            pub fn table_name(&self) -> String {
                format!(
                    "{db_name}.measurements_{type_}",
                    db_name = DATABASE_NAME,
                    type_ = $table_suffix,
                )
            }
        }
    };
}

define_histogram_measurement_row! { HistogramI64MeasurementRow, DbHistogram<i64>, "histogrami64" }
define_histogram_measurement_row! { HistogramF64MeasurementRow, DbHistogram<f64>, "histogramf64" }

// Helper to collect the field rows from a sample
fn unroll_from_source(sample: &Sample) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for field in sample.fields() {
        let timeseries_name = sample.timeseries_name.clone();
        let timeseries_key = sample.timeseries_key.clone();
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
    let timeseries_key = sample.timeseries_key.clone();
    let timestamp = sample.timestamp;
    match sample.measurement {
        Measurement::Bool(inner) => {
            let row = BoolMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: DbBool::from(inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::I64(inner) => {
            let row = I64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: inner,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::F64(inner) => {
            let row = F64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: inner,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::String(ref inner) => {
            let row = StringMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: inner.clone(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::Bytes(ref inner) => {
            let row = BytesMeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: inner.clone(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::CumulativeI64(inner) => {
            let row = CumulativeI64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: inner.value(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::CumulativeF64(inner) => {
            let row = CumulativeF64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: inner.value(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::HistogramI64(ref inner) => {
            let row = HistogramI64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: DbHistogram::from(inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::HistogramF64(ref inner) => {
            let row = HistogramF64MeasurementRow {
                timeseries_name,
                timeseries_key,
                timestamp,
                value: DbHistogram::from(inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
    }
}

/// Return the schema for a `Sample`.
pub(crate) fn schema_for(sample: &Sample) -> TimeseriesSchema {
    let created = Utc::now();
    let fields = sample
        .target_fields()
        .iter()
        .map(|field| Field {
            name: field.name.clone(),
            ty: field.value.field_type(),
            source: FieldSource::Target,
        })
        .chain(sample.metric_fields().iter().map(|field| Field {
            name: field.name.clone(),
            ty: field.value.field_type(),
            source: FieldSource::Metric,
        }))
        .collect();
    TimeseriesSchema {
        timeseries_name: sample.timeseries_name.clone(),
        fields,
        measurement_type: sample.measurement.measurement_type(),
        created,
    }
}

// A scalar timestamped sample from a timeseries, as extracted from a query to the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DbTimeseriesScalarSample<'a, T> {
    pub timeseries_key: &'a str,
    #[serde(with = "serde_timestamp")]
    pub timestamp: DateTime<Utc>,
    pub value: T,
}

// A histogram timestamped sample from a timeseries, as extracted from a query to the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DbTimeseriesHistogramSample<'a, T> {
    pub timeseries_key: &'a str,
    #[serde(with = "serde_timestamp")]
    pub timestamp: DateTime<Utc>,
    pub bins: Vec<T>,
    pub counts: Vec<u64>,
}

// Parse a line of JSON from the database resulting from `as_select_query`, into a single sample of
// the expected type. Also returns the timeseries key from the line.
pub(crate) fn parse_timeseries_sample(
    line: &str,
    measurement_type: MeasurementType,
) -> Result<(String, TimeseriesSample), Error> {
    // TODO-cleanup: This is a pretty verbose way to dispatch parsing on the type. Most things are
    // the same -- is there a better way to do this?
    let (key, timestamp, measurement) = match measurement_type {
        MeasurementType::Bool => {
            let sample = serde_json::from_str::<
                DbTimeseriesScalarSample<'_, DbBool>,
            >(line)
            .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(bool::from(sample.value)),
            )
        }
        MeasurementType::I64 => {
            let sample =
                serde_json::from_str::<DbTimeseriesScalarSample<'_, i64>>(line)
                    .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(sample.value),
            )
        }
        MeasurementType::F64 => {
            let sample =
                serde_json::from_str::<DbTimeseriesScalarSample<'_, f64>>(line)
                    .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(sample.value),
            )
        }
        MeasurementType::String => {
            let sample = serde_json::from_str::<
                DbTimeseriesScalarSample<'_, &str>,
            >(line)
            .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(sample.value),
            )
        }
        MeasurementType::Bytes => {
            let sample = serde_json::from_str::<
                DbTimeseriesScalarSample<'_, Bytes>,
            >(line)
            .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(sample.value),
            )
        }
        MeasurementType::CumulativeI64 => {
            let sample = serde_json::from_str::<
                DbTimeseriesScalarSample<'_, Cumulative<i64>>,
            >(line)
            .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(sample.value),
            )
        }
        MeasurementType::CumulativeF64 => {
            let sample = serde_json::from_str::<
                DbTimeseriesScalarSample<'_, Cumulative<f64>>,
            >(line)
            .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(sample.value),
            )
        }
        MeasurementType::HistogramI64 => {
            let sample = serde_json::from_str::<
                DbTimeseriesHistogramSample<'_, i64>,
            >(line)
            .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(
                    histogram::Histogram::from_arrays(
                        sample.bins,
                        sample.counts,
                    )
                    .unwrap(),
                ),
            )
        }
        MeasurementType::HistogramF64 => {
            let sample = serde_json::from_str::<
                DbTimeseriesHistogramSample<'_, f64>,
            >(line)
            .unwrap();
            (
                sample.timeseries_key.to_string(),
                sample.timestamp,
                Measurement::from(
                    histogram::Histogram::from_arrays(
                        sample.bins,
                        sample.counts,
                    )
                    .unwrap(),
                ),
            )
        }
    };
    Ok((key, TimeseriesSample { timestamp, measurement }))
}

/// A single timestamped sample from a timeseries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesSample {
    pub timestamp: DateTime<Utc>,
    pub measurement: Measurement,
}

/// Information about a target, returned to clients in a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target {
    pub name: String,
    pub fields: Vec<types::Field>,
}

/// Information about a metric, returned to clients in a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub fields: Vec<types::Field>,
    pub measurement_type: MeasurementType,
}

/// A list of timestamped samples from a timeseries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timeseries {
    pub timeseries_key: String,
    pub target: Target,
    pub metric: Metric,
    pub samples: Vec<TimeseriesSample>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_util;

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
    fn test_db_histogram() {
        let mut hist = histogram::Histogram::new(&[0i64, 10, 20]).unwrap();
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
        let unpacked_hist = histogram::Histogram::from_arrays(
            unpacked.value.bins,
            unpacked.value.counts,
        )
        .unwrap();
        if let Measurement::HistogramF64(hist) = sample.measurement {
            assert_eq!(
                hist, unpacked_hist,
                "Unpacking histogram from database representation failed"
            );
        } else {
            panic!("Expected a histogram measurement");
        }
    }

    #[test]
    fn test_parse_timeseries_sample_scalar() {
        use chrono::TimeZone;
        let timestamp = Utc.ymd(2021, 1, 1).and_hms_micro(0, 0, 0, 123456);

        let line = r#"{"timeseries_key": "foo:bar", "timestamp": "2021-01-01 00:00:00.123456", "value": 1 }"#;
        let (key, sample) =
            parse_timeseries_sample(line, MeasurementType::Bool).unwrap();
        assert_eq!(key, "foo:bar");
        assert_eq!(sample.timestamp, timestamp);
        assert_eq!(sample.measurement, true.into());

        let line = r#"{"timeseries_key": "foo:bar", "timestamp": "2021-01-01 00:00:00.123456", "value": 2 }"#;
        let (key, sample) =
            parse_timeseries_sample(line, MeasurementType::I64).unwrap();
        assert_eq!(key, "foo:bar");
        assert_eq!(sample.timestamp, timestamp);
        assert_eq!(sample.measurement, 2.into());

        let line = r#"{"timeseries_key": "foo:bar", "timestamp": "2021-01-01 00:00:00.123456", "value": 3.0 }"#;
        let (key, sample) =
            parse_timeseries_sample(line, MeasurementType::F64).unwrap();
        assert_eq!(key, "foo:bar");
        assert_eq!(sample.timestamp, timestamp);
        assert_eq!(sample.measurement, 3.0.into());
    }

    #[test]
    fn test_parse_timeseries_sample_histogram() {
        use chrono::TimeZone;
        let timestamp = Utc.ymd(2021, 1, 1).and_hms_micro(0, 0, 0, 123456);

        let line = r#"{"timeseries_key": "foo:bar", "timestamp": "2021-01-01 00:00:00.123456", "bins": [0, 1], "counts": [1, 1] }"#;
        let (key, sample) =
            parse_timeseries_sample(line, MeasurementType::HistogramI64)
                .unwrap();
        assert_eq!(key, "foo:bar");
        assert_eq!(sample.timestamp, timestamp);
        if let Measurement::HistogramI64(hist) = sample.measurement {
            assert_eq!(hist.n_bins(), 3);
            assert_eq!(hist.n_samples(), 2);
        } else {
            panic!("Expected a histogram sample");
        }
    }
}
