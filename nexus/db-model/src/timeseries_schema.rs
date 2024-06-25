// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types modeling the timeseries schema tables.

use std::collections::BTreeSet;
use std::num::NonZeroU8;

use crate::impl_enum_type;
use crate::schema::timeseries_field;
use crate::schema::timeseries_field_by_version;
use crate::schema::timeseries_schema;
use crate::Generation;
use crate::SqlU8;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::external::Error;

impl_enum_type! {
    #[derive(SqlType, QueryId, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "timeseries_authz_scope", schema = "public"))]
    pub struct TimeseriesAuthzScopeEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = TimeseriesAuthzScopeEnum)]
    pub enum TimeseriesAuthzScope;

    Fleet => b"fleet"
    Silo => b"silo"
    Project => b"project"
    ViewableToAll => b"viewable_to_all"
}

impl From<oximeter::schema::AuthzScope> for TimeseriesAuthzScope {
    fn from(value: oximeter::schema::AuthzScope) -> Self {
        match value {
            oximeter::schema::AuthzScope::Fleet => Self::Fleet,
            oximeter::schema::AuthzScope::Silo => Self::Silo,
            oximeter::schema::AuthzScope::Project => Self::Project,
            oximeter::schema::AuthzScope::ViewableToAll => Self::ViewableToAll,
        }
    }
}

impl From<TimeseriesAuthzScope> for oximeter::schema::AuthzScope {
    fn from(value: TimeseriesAuthzScope) -> Self {
        match value {
            TimeseriesAuthzScope::Fleet => Self::Fleet,
            TimeseriesAuthzScope::Silo => Self::Silo,
            TimeseriesAuthzScope::Project => Self::Project,
            TimeseriesAuthzScope::ViewableToAll => Self::ViewableToAll,
        }
    }
}

impl_enum_type! {
    #[derive(SqlType, QueryId, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "timeseries_field_source", schema = "public"))]
    pub struct TimeseriesFieldSourceEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = TimeseriesFieldSourceEnum)]
    pub enum TimeseriesFieldSource;

    Target => b"target"
    Metric => b"metric"
}

impl From<oximeter::schema::FieldSource> for TimeseriesFieldSource {
    fn from(value: oximeter::schema::FieldSource) -> Self {
        match value {
            oximeter::schema::FieldSource::Target => Self::Target,
            oximeter::schema::FieldSource::Metric => Self::Metric,
        }
    }
}

impl From<TimeseriesFieldSource> for oximeter::schema::FieldSource {
    fn from(value: TimeseriesFieldSource) -> Self {
        match value {
            TimeseriesFieldSource::Target => Self::Target,
            TimeseriesFieldSource::Metric => Self::Metric,
        }
    }
}

impl_enum_type! {
    #[derive(SqlType, QueryId, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "timeseries_field_type", schema = "public"))]
    pub struct TimeseriesFieldTypeEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = TimeseriesFieldTypeEnum)]
    pub enum TimeseriesFieldType;

    String => b"string"
    I8 => b"i8"
    U8 => b"u8"
    I16 => b"i16"
    U16 => b"u16"
    I32 => b"i32"
    U32 => b"u32"
    I64 => b"i64"
    U64 => b"u64"
    IpAddr => b"ip_addr"
    Uuid => b"uuid"
    Bool => b"bool"
}

impl From<oximeter::FieldType> for TimeseriesFieldType {
    fn from(value: oximeter::FieldType) -> Self {
        match value {
            oximeter::FieldType::String => Self::String,
            oximeter::FieldType::I8 => Self::I8,
            oximeter::FieldType::U8 => Self::U8,
            oximeter::FieldType::I16 => Self::I16,
            oximeter::FieldType::U16 => Self::U16,
            oximeter::FieldType::I32 => Self::I32,
            oximeter::FieldType::U32 => Self::U32,
            oximeter::FieldType::I64 => Self::I64,
            oximeter::FieldType::U64 => Self::U64,
            oximeter::FieldType::IpAddr => Self::IpAddr,
            oximeter::FieldType::Uuid => Self::Uuid,
            oximeter::FieldType::Bool => Self::Bool,
        }
    }
}

impl From<TimeseriesFieldType> for oximeter::FieldType {
    fn from(value: TimeseriesFieldType) -> Self {
        match value {
            TimeseriesFieldType::String => Self::String,
            TimeseriesFieldType::I8 => Self::I8,
            TimeseriesFieldType::U8 => Self::U8,
            TimeseriesFieldType::I16 => Self::I16,
            TimeseriesFieldType::U16 => Self::U16,
            TimeseriesFieldType::I32 => Self::I32,
            TimeseriesFieldType::U32 => Self::U32,
            TimeseriesFieldType::I64 => Self::I64,
            TimeseriesFieldType::U64 => Self::U64,
            TimeseriesFieldType::IpAddr => Self::IpAddr,
            TimeseriesFieldType::Uuid => Self::Uuid,
            TimeseriesFieldType::Bool => Self::Bool,
        }
    }
}

impl_enum_type! {
    #[derive(SqlType, QueryId, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "timeseries_datum_type", schema = "public"))]
    pub struct TimeseriesDatumTypeEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = TimeseriesDatumTypeEnum)]
    pub enum TimeseriesDatumType;

    Bool => b"bool"
    I8 => b"i8"
    U8 => b"u8"
    I16 => b"i16"
    U16 => b"u16"
    I32 => b"i32"
    U32 => b"u32"
    I64 => b"i64"
    U64 => b"u64"
    F32 => b"f32"
    F64 => b"f64"
    String => b"string"
    Bytes => b"bytes"
    CumulativeI64 => b"cumulative_i64"
    CumulativeU64 => b"cumulative_u64"
    CumulativeF32 => b"cumulative_f32"
    CumulativeF64 => b"cumulative_f64"
    HistogramI8 => b"histogram_i8"
    HistogramU8 => b"histogram_u8"
    HistogramI16 => b"histogram_i16"
    HistogramU16 => b"histogram_u16"
    HistogramI32 => b"histogram_i32"
    HistogramU32 => b"histogram_u32"
    HistogramI64 => b"histogram_i64"
    HistogramU64 => b"histogram_u64"
    HistogramF32 => b"histogram_f32"
    HistogramF64 => b"histogram_f64"
}

impl From<oximeter::DatumType> for TimeseriesDatumType {
    fn from(value: oximeter::DatumType) -> Self {
        match value {
            oximeter::DatumType::Bool => Self::Bool,
            oximeter::DatumType::I8 => Self::I8,
            oximeter::DatumType::U8 => Self::U8,
            oximeter::DatumType::I16 => Self::I16,
            oximeter::DatumType::U16 => Self::U16,
            oximeter::DatumType::I32 => Self::I32,
            oximeter::DatumType::U32 => Self::U32,
            oximeter::DatumType::I64 => Self::I64,
            oximeter::DatumType::U64 => Self::U64,
            oximeter::DatumType::F32 => Self::F32,
            oximeter::DatumType::F64 => Self::F64,
            oximeter::DatumType::String => Self::String,
            oximeter::DatumType::Bytes => Self::Bytes,
            oximeter::DatumType::CumulativeI64 => Self::CumulativeI64,
            oximeter::DatumType::CumulativeU64 => Self::CumulativeU64,
            oximeter::DatumType::CumulativeF32 => Self::CumulativeF32,
            oximeter::DatumType::CumulativeF64 => Self::CumulativeF64,
            oximeter::DatumType::HistogramI8 => Self::HistogramI8,
            oximeter::DatumType::HistogramU8 => Self::HistogramU8,
            oximeter::DatumType::HistogramI16 => Self::HistogramI16,
            oximeter::DatumType::HistogramU16 => Self::HistogramU16,
            oximeter::DatumType::HistogramI32 => Self::HistogramI32,
            oximeter::DatumType::HistogramU32 => Self::HistogramU32,
            oximeter::DatumType::HistogramI64 => Self::HistogramI64,
            oximeter::DatumType::HistogramU64 => Self::HistogramU64,
            oximeter::DatumType::HistogramF32 => Self::HistogramF32,
            oximeter::DatumType::HistogramF64 => Self::HistogramF64,
        }
    }
}

impl From<TimeseriesDatumType> for oximeter::DatumType {
    fn from(value: TimeseriesDatumType) -> Self {
        match value {
            TimeseriesDatumType::Bool => Self::Bool,
            TimeseriesDatumType::I8 => Self::I8,
            TimeseriesDatumType::U8 => Self::U8,
            TimeseriesDatumType::I16 => Self::I16,
            TimeseriesDatumType::U16 => Self::U16,
            TimeseriesDatumType::I32 => Self::I32,
            TimeseriesDatumType::U32 => Self::U32,
            TimeseriesDatumType::I64 => Self::I64,
            TimeseriesDatumType::U64 => Self::U64,
            TimeseriesDatumType::F32 => Self::F32,
            TimeseriesDatumType::F64 => Self::F64,
            TimeseriesDatumType::String => Self::String,
            TimeseriesDatumType::Bytes => Self::Bytes,
            TimeseriesDatumType::CumulativeI64 => Self::CumulativeI64,
            TimeseriesDatumType::CumulativeU64 => Self::CumulativeU64,
            TimeseriesDatumType::CumulativeF32 => Self::CumulativeF32,
            TimeseriesDatumType::CumulativeF64 => Self::CumulativeF64,
            TimeseriesDatumType::HistogramI8 => Self::HistogramI8,
            TimeseriesDatumType::HistogramU8 => Self::HistogramU8,
            TimeseriesDatumType::HistogramI16 => Self::HistogramI16,
            TimeseriesDatumType::HistogramU16 => Self::HistogramU16,
            TimeseriesDatumType::HistogramI32 => Self::HistogramI32,
            TimeseriesDatumType::HistogramU32 => Self::HistogramU32,
            TimeseriesDatumType::HistogramI64 => Self::HistogramI64,
            TimeseriesDatumType::HistogramU64 => Self::HistogramU64,
            TimeseriesDatumType::HistogramF32 => Self::HistogramF32,
            TimeseriesDatumType::HistogramF64 => Self::HistogramF64,
        }
    }
}

impl_enum_type! {
    #[derive(SqlType, QueryId, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "timeseries_units", schema = "public"))]
    pub struct TimeseriesUnitsEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = TimeseriesUnitsEnum)]
    pub enum TimeseriesUnits;

    Count => b"count"
    Bytes => b"bytes"
}

impl From<oximeter::schema::Units> for TimeseriesUnits {
    fn from(value: oximeter::schema::Units) -> Self {
        match value {
            oximeter::schema::Units::Count => Self::Count,
            oximeter::schema::Units::Bytes => Self::Bytes,
        }
    }
}

impl From<TimeseriesUnits> for oximeter::schema::Units {
    fn from(value: TimeseriesUnits) -> Self {
        match value {
            TimeseriesUnits::Count => Self::Count,
            TimeseriesUnits::Bytes => Self::Bytes,
        }
    }
}

/// The schema of a timeseries.
#[derive(Insertable, Selectable, Queryable, Clone, Debug)]
#[diesel(table_name = timeseries_schema)]
pub struct TimeseriesSchema {
    /// The name of the timeseries.
    pub timeseries_name: String,
    /// The authorization scope of the timeseries.
    pub authz_scope: TimeseriesAuthzScope,
    /// The description of the timeseries's target.
    pub target_description: String,
    /// The description of the timeseries's metric.
    pub metric_description: String,
    /// The type of the timeseries's datum.
    pub datum_type: TimeseriesDatumType,
    /// The units of the timeseries's datum.
    pub units: TimeseriesUnits,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    /// Generation number of the timeseries schema, shared by all records for a
    /// single schema. Used for OCC.
    pub generation: Generation,
}

impl TimeseriesSchema {
    pub fn into_bare_schema(
        self,
        version: NonZeroU8,
    ) -> Result<oximeter::TimeseriesSchema, Error> {
        let Ok(timeseries_name) = self.timeseries_name.as_str().try_into()
        else {
            return Err(Error::internal_error(&format!(
                "Invalid timeseries name in database: '{}'",
                self.timeseries_name
            )));
        };
        Ok(oximeter::TimeseriesSchema {
            timeseries_name,
            description: oximeter::schema::TimeseriesDescription {
                target: self.target_description,
                metric: self.metric_description,
            },
            field_schema: BTreeSet::new(),
            datum_type: self.datum_type.into(),
            version,
            authz_scope: self.authz_scope.into(),
            units: self.units.into(),
            created: self.time_created,
        })
    }
}

impl TimeseriesSchema {
    /// Create a schema row with the provided generation.
    pub fn new(
        schema: &oximeter::TimeseriesSchema,
        generation: Generation,
    ) -> Self {
        Self {
            timeseries_name: schema.timeseries_name.to_string(),
            authz_scope: schema.authz_scope.into(),
            target_description: schema.description.target.clone(),
            metric_description: schema.description.metric.clone(),
            datum_type: schema.datum_type.into(),
            units: schema.units.into(),
            time_created: schema.created,
            time_modified: schema.created,
            generation,
        }
    }
}

#[derive(Insertable, Selectable, Queryable, Clone, Debug)]
#[diesel(table_name = timeseries_field)]
pub struct TimeseriesField {
    /// The name of the timeseries this field belongs to.
    pub timeseries_name: String,
    /// The name of the field.
    pub name: String,
    /// The source of the field.
    pub source: TimeseriesFieldSource,
    /// The type of the field.
    pub type_: TimeseriesFieldType,
    /// The description of the field.
    pub description: String,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    /// Generation number of the timeseries schema, shared by all records for a
    /// single schema. Used for OCC.
    pub generation: Generation,
}

impl From<TimeseriesField> for oximeter::FieldSchema {
    fn from(value: TimeseriesField) -> Self {
        Self {
            name: value.name,
            field_type: value.type_.into(),
            source: value.source.into(),
            description: value.description,
        }
    }
}

impl TimeseriesField {
    pub fn for_schema(
        schema: &oximeter::TimeseriesSchema,
        generation: Generation,
    ) -> Vec<Self> {
        schema
            .field_schema
            .iter()
            .map(|field| Self {
                timeseries_name: schema.timeseries_name.to_string(),
                name: field.name.clone(),
                source: field.source.into(),
                type_: field.field_type.into(),
                description: field.description.clone(),
                time_created: schema.created,
                time_modified: schema.created,
                generation,
            })
            .collect()
    }
}

/// This type models the mapping from each version of a timeseries schema to a
/// row it contains.
#[derive(Insertable, Selectable, Queryable, Clone, Debug)]
#[diesel(table_name = timeseries_field_by_version)]
pub struct TimeseriesFieldByVersion {
    /// The name of the timeseries this field belongs to.
    pub timeseries_name: String,
    /// The version of the timeseries this field belongs to.
    pub version: SqlU8,
    /// The name of the field.
    pub field_name: String,
    /// Generation number of the timeseries schema, shared by all records for a
    /// single schema. Used for OCC.
    pub generation: Generation,
}

impl TimeseriesFieldByVersion {
    pub fn for_schema(
        schema: &oximeter::TimeseriesSchema,
        generation: Generation,
    ) -> Vec<Self> {
        schema
            .field_schema
            .iter()
            .map(|field| Self {
                timeseries_name: schema.timeseries_name.to_string(),
                version: schema.version.get().into(),
                field_name: field.name.clone(),
                generation,
            })
            .collect()
    }
}
