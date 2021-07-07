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
    FieldType, FieldValue, Measurement, MeasurementType, Sample,
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
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
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

/// The source, target or metric, to which a field corresponds.
///
/// Tables with field information, whether from a target or metric, are largely the same. This enum
/// indicates which of these this field row derives from, and is used to populate the
/// `{target,metric}_name` column of the corresponding field tables.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FieldSource {
    #[serde(rename = "target_name")]
    Target(String),
    #[serde(rename = "metric_name")]
    Metric(String),
}

impl FieldSource {
    /// Return the stem of the table name for the corresponding row.
    pub fn table_stem(&self) -> &'static str {
        match self {
            FieldSource::Target(_) => "target_fields",
            FieldSource::Metric(_) => "metric_fields",
        }
    }
}

/// Information about a target or metric field as contained in a schema.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Field {
    /// The field name
    pub name: String,

    /// The field type
    pub ty: FieldType,
}

// The list of fields in a schema as represented in the actual schema tables in the database.
//
// Data about the schema in ClickHouse is represented in the `{target,metric}_schema` tables. These
// contain the fields as a nested table, which is stored as a struct of arrays. This type is used
// to convert between that representation in the database, and the representation we prefer, i.e.,
// the `Field` type above. In other words, we prefer to work with an array of structs, but
// ClickHouse requires a struct of arrays. `Field` is the former, `DbFieldList` is the latter.
//
// Note that the fields are renamed so that ClickHouse interprets them as correctly referring to
// the nested column names.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DbFieldList {
    #[serde(rename = "fields.name")]
    pub names: Vec<String>,
    #[serde(rename = "fields.type")]
    pub types: Vec<FieldType>,
}

impl From<DbFieldList> for Vec<Field> {
    fn from(list: DbFieldList) -> Self {
        list.names
            .into_iter()
            .zip(list.types.into_iter())
            .map(|(name, ty)| Field { name, ty })
            .collect()
    }
}

impl From<Vec<Field>> for DbFieldList {
    fn from(list: Vec<Field>) -> Self {
        let (names, types) =
            list.into_iter().map(|field| (field.name, field.ty)).unzip();
        DbFieldList { names, types }
    }
}

// A trait describing types in one of the database's schema tables.
pub(crate) trait Schematized<'a>:
    PartialEq<Self> + Deserialize<'a> + Serialize + Sized
{
    // The column in the DB with the object's name
    fn column_name(&self) -> &str;
    // The name of the object the schema represents, i.e., the entry in the `column_name` table.
    fn name(&self) -> &str;
    // The list of fields for this schema.
    fn fields(&self) -> &[Field];
}

/// A representation of a row in one of the target_schema table.
///
/// Targets describe the source of telemetry or metric data, i.e., the object that the measurement
/// is derived from. These are described by the [`Target`](crate::traits::Target) trait.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TargetSchema {
    pub target_name: String,
    pub fields: Vec<Field>,
    #[serde(with = "serde_timestamp")]
    pub created: DateTime<Utc>,
}

impl Schematized<'_> for TargetSchema {
    fn column_name(&self) -> &str {
        "target_name"
    }

    fn name(&self) -> &str {
        &self.target_name
    }

    fn fields(&self) -> &[Field] {
        &self.fields
    }
}

impl PartialEq for TargetSchema {
    fn eq(&self, other: &TargetSchema) -> bool {
        self.target_name == other.target_name && self.fields == other.fields
    }
}

// The internal representation of `TargetSchema` used in the database
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DbTargetSchema {
    pub target_name: String,
    #[serde(flatten)]
    pub fields: DbFieldList,
    #[serde(with = "serde_timestamp")]
    pub created: DateTime<Utc>,
}

impl From<DbTargetSchema> for TargetSchema {
    fn from(schema: DbTargetSchema) -> Self {
        TargetSchema {
            target_name: schema.target_name,
            fields: schema.fields.into(),
            created: schema.created,
        }
    }
}

impl From<TargetSchema> for DbTargetSchema {
    fn from(schema: TargetSchema) -> Self {
        DbTargetSchema {
            target_name: schema.target_name,
            fields: schema.fields.into(),
            created: schema.created,
        }
    }
}

/// A representation of a row in one of the metric_schema table.
///
/// Metrics describe the aspect or feature of a target that metric data captures. This is the
/// actual feature being measured, and is described by the [`Metric`](crate::traits::Metric) trait.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricSchema {
    pub metric_name: String,
    pub fields: Vec<Field>,
    pub measurement_type: MeasurementType,
    #[serde(with = "serde_timestamp")]
    pub created: DateTime<Utc>,
}

impl PartialEq for MetricSchema {
    fn eq(&self, other: &MetricSchema) -> bool {
        self.metric_name == other.metric_name
            && self.fields == other.fields
            && self.measurement_type == other.measurement_type
    }
}

impl Schematized<'_> for MetricSchema {
    fn column_name(&self) -> &str {
        "metric_name"
    }

    fn name(&self) -> &str {
        &self.metric_name
    }

    fn fields(&self) -> &[Field] {
        &self.fields
    }
}

// The internal representation of `MetricSchema` used in the database
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DbMetricSchema {
    pub metric_name: String,
    #[serde(flatten)]
    pub fields: DbFieldList,
    pub measurement_type: MeasurementType,
    #[serde(with = "serde_timestamp")]
    pub created: DateTime<Utc>,
}

impl From<DbMetricSchema> for MetricSchema {
    fn from(schema: DbMetricSchema) -> Self {
        MetricSchema {
            metric_name: schema.metric_name,
            fields: schema.fields.into(),
            measurement_type: schema.measurement_type,
            created: schema.created,
        }
    }
}

impl From<MetricSchema> for DbMetricSchema {
    fn from(schema: MetricSchema) -> Self {
        DbMetricSchema {
            metric_name: schema.metric_name,
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
        #[derive(Clone, Debug, Deserialize, Serialize)]
        pub struct $name {
            #[serde(flatten)]
            pub source: FieldSource,
            pub timeseries_key: String,
            pub field_name: String,
            pub field_value: $value_type,
            #[serde(with = "serde_timestamp")]
            pub timestamp: DateTime<Utc>,
        }

        impl $name {
            pub fn table_name(&self) -> String {
                format!(
                    "{db_name}.{stem}_{type_}",
                    db_name = DATABASE_NAME,
                    stem = self.source.table_stem(),
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
        #[derive(Clone, Debug, Deserialize, Serialize)]
        pub struct $name {
            pub target_name: String,
            pub metric_name: String,
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
#[derive(Clone, Debug, Deserialize, Serialize)]
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
        #[derive(Clone, Debug, Deserialize, Serialize)]
        pub struct $name {
            pub target_name: String,
            pub metric_name: String,
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

// Helper to collect the field rows, for either the targets or metrics, from a sample.
fn unroll_from_source(
    sample: &Sample,
    source: FieldSource,
) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    let fields = match source {
        FieldSource::Target(_) => &sample.target.fields,
        FieldSource::Metric(_) => &sample.metric.fields,
    };
    for field in fields.iter() {
        let source = source.clone();
        let timeseries_key = sample.key.clone();
        let field_name = field.name.clone();
        let timestamp = sample.timestamp;

        let (table_name, row_string) = match &field.value {
            FieldValue::Bool(inner) => {
                let row = BoolFieldRow {
                    source,
                    timeseries_key,
                    field_name,
                    field_value: DbBool::from(*inner),
                    timestamp,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::I64(inner) => {
                let row = I64FieldRow {
                    source,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                    timestamp,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::String(inner) => {
                let row = StringFieldRow {
                    source,
                    timeseries_key,
                    field_name,
                    field_value: inner.clone(),
                    timestamp,
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
                    source,
                    timeseries_key,
                    field_name,
                    field_value,
                    timestamp,
                };
                (row.table_name(), serde_json::to_string(&row).unwrap())
            }
            FieldValue::Uuid(inner) => {
                let row = UuidFieldRow {
                    source,
                    timeseries_key,
                    field_name,
                    field_value: *inner,
                    timestamp,
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
    for (table, rows) in unroll_from_source(
        sample,
        FieldSource::Target(sample.target.name.clone()),
    ) {
        out.entry(table).or_insert_with(Vec::new).extend(rows);
    }
    for (table, rows) in unroll_from_source(
        sample,
        FieldSource::Metric(sample.metric.name.clone()),
    ) {
        out.entry(table).or_insert_with(Vec::new).extend(rows);
    }
    out
}

/// Return the table name and serialized measurement row for a [`Sample`], to insert into
/// ClickHouse.
pub(crate) fn unroll_measurement_row(sample: &Sample) -> (String, String) {
    let target_name = sample.target.name.clone();
    let metric_name = sample.metric.name.clone();
    let timeseries_key = sample.key.clone();
    let timestamp = sample.timestamp;
    match sample.metric.measurement {
        Measurement::Bool(inner) => {
            let row = BoolMeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: DbBool::from(inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::I64(inner) => {
            let row = I64MeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: inner,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::F64(inner) => {
            let row = F64MeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: inner,
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::String(ref inner) => {
            let row = StringMeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: inner.clone(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::Bytes(ref inner) => {
            let row = BytesMeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: inner.clone(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::CumulativeI64(inner) => {
            let row = CumulativeI64MeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: inner.value(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::CumulativeF64(inner) => {
            let row = CumulativeF64MeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: inner.value(),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::HistogramI64(ref inner) => {
            let row = HistogramI64MeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: DbHistogram::from(inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
        Measurement::HistogramF64(ref inner) => {
            let row = HistogramF64MeasurementRow {
                target_name,
                metric_name,
                timeseries_key,
                timestamp,
                value: DbHistogram::from(inner),
            };
            (row.table_name(), serde_json::to_string(&row).unwrap())
        }
    }
}

/// Return the schema for the target and metric, from a sample containing them
pub(crate) fn schema_for(sample: &Sample) -> (TargetSchema, MetricSchema) {
    let created = Utc::now();
    let target_schema = TargetSchema {
        target_name: sample.target.name.clone(),
        fields: sample
            .target
            .fields
            .iter()
            .map(|field| {
                let name = field.name.clone();
                let ty = field.value.field_type();
                Field { name, ty }
            })
            .collect(),
        created,
    };
    let metric_schema = MetricSchema {
        metric_name: sample.metric.name.clone(),
        fields: sample
            .metric
            .fields
            .iter()
            .map(|field| {
                let name = field.name.clone();
                let ty = field.value.field_type();
                Field { name, ty }
            })
            .collect(),
        measurement_type: sample.metric.measurement.measurement_type(),
        created,
    };
    (target_schema, metric_schema)
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
        let out = unroll_from_source(
            &sample,
            FieldSource::Target(sample.target.name.clone()),
        );
        assert_eq!(out["oximeter.target_fields_string"].len(), 2);
        assert_eq!(out["oximeter.target_fields_i64"].len(), 1);
        let unpacked: StringFieldRow =
            serde_json::from_str(&out["oximeter.target_fields_string"][0])
                .unwrap();
        if let FieldSource::Target(name) = unpacked.source {
            assert_eq!(name, sample.target.name);
        } else {
            panic!("Expected the packed row to have a source matching FieldSource::Target");
        }
        let field = &sample.target.fields[0];
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
        if let Measurement::HistogramF64(hist) = sample.metric.measurement {
            assert_eq!(
                hist, unpacked_hist,
                "Unpacking histogram from database representation failed"
            );
        } else {
            panic!("Expected a histogram measurement");
        }
    }

    #[test]
    fn test_target_schema_partial_eq() {
        let first = TargetSchema {
            target_name: "target_name".to_string(),
            fields: vec![Field {
                name: "field_name".to_string(),
                ty: FieldType::I64,
            }],
            created: Utc::now(),
        };
        assert_eq!(first, first);

        let mut bad_target_name = first.clone();
        bad_target_name.target_name = "another_target_name".into();
        assert_ne!(first, bad_target_name);

        let mut bad_field_name = first.clone();
        bad_field_name.fields[0].name = "another_field_name".into();
        assert_ne!(first, bad_field_name);

        let mut bad_field_type = first.clone();
        bad_field_type.fields[0].ty = FieldType::Bool;
        assert_ne!(first, bad_field_type);

        let mut different_timestamp = first.clone();
        different_timestamp.created = Utc::now();
        assert_eq!(first, different_timestamp);
    }

    #[test]
    fn test_metric_schema_partial_eq() {
        let first = MetricSchema {
            metric_name: "metric_name".to_string(),
            fields: vec![Field {
                name: "field_name".to_string(),
                ty: FieldType::I64,
            }],
            measurement_type: MeasurementType::F64,
            created: Utc::now(),
        };
        assert_eq!(first, first);

        let mut bad_metric_name = first.clone();
        bad_metric_name.metric_name = "another_metric_name".into();
        assert_ne!(first, bad_metric_name);

        let mut bad_field_name = first.clone();
        bad_field_name.fields[0].name = "another_field_name".into();
        assert_ne!(first, bad_field_name);

        let mut bad_field_type = first.clone();
        bad_field_type.fields[0].ty = FieldType::Bool;
        assert_ne!(first, bad_field_type);

        let mut bad_measurement_type = first.clone();
        bad_measurement_type.measurement_type = MeasurementType::I64;
        assert_ne!(first, bad_measurement_type);

        let mut different_timestamp = first.clone();
        different_timestamp.created = Utc::now();
        assert_eq!(first, different_timestamp);
    }
}
