// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Schema for tables in OxQL queries.

// Copyright 2024 Oxide Computer Company

use std::collections::BTreeMap;

use oximeter::DatumType;
use oximeter::FieldType;
use oximeter::TimeseriesSchema;
use oxql_types::point::DataType;
use oxql_types::point::MetricType;
use serde::Deserialize;
use serde::Serialize;

// TODO(ben) Move MetricType and DataType enums here, or probably this type
// there.
// TODO(ben) Consider biting the bullet and supporting every data type now. Why
// not? Lots more code, since we need to deal with type promotion rules and
// such.

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TableSchema {
    /// The name of the table.
    pub name: String,
    /// The mapping from field names to their types.
    pub fields: BTreeMap<String, FieldType>,
    /// The metric types of the contained data, one for each dimension.
    ///
    /// There can be more than one in the case of joins.
    pub metric_types: Vec<MetricType>,
    /// The types of the raw data, one for each dimension.
    ///
    /// There can be more than one in the case of joins.
    pub data_types: Vec<DataType>,
}

impl TableSchema {
    /// Construct a new table schema from a schema in the database.
    ///
    /// This builds an initial table schema for an OxQL query. Queries always
    /// start by referring to one or more schema in the database itself. They
    /// can change as they are processed by the query.
    pub fn new(timeseries_schema: &TimeseriesSchema) -> Self {
        let (metric_type, data_type) = match timeseries_schema.datum_type {
            DatumType::Bool => (MetricType::Gauge, DataType::Boolean),
            DatumType::I8 => (MetricType::Gauge, DataType::Integer),
            DatumType::U8 => (MetricType::Gauge, DataType::Integer),
            DatumType::I16 => (MetricType::Gauge, DataType::Integer),
            DatumType::U16 => (MetricType::Gauge, DataType::Integer),
            DatumType::I32 => (MetricType::Gauge, DataType::Integer),
            DatumType::U32 => (MetricType::Gauge, DataType::Integer),
            DatumType::I64 => (MetricType::Gauge, DataType::Integer),
            DatumType::U64 => (MetricType::Gauge, DataType::Integer),
            DatumType::F32 => (MetricType::Gauge, DataType::Double),
            DatumType::F64 => (MetricType::Gauge, DataType::Double),
            DatumType::String => (MetricType::Gauge, DataType::String),
            DatumType::Bytes => (MetricType::Gauge, DataType::String),
            DatumType::CumulativeI64 => {
                (MetricType::Cumulative, DataType::Integer)
            }
            DatumType::CumulativeU64 => {
                (MetricType::Cumulative, DataType::Integer)
            }
            DatumType::CumulativeF32 => {
                (MetricType::Cumulative, DataType::Double)
            }
            DatumType::CumulativeF64 => {
                (MetricType::Cumulative, DataType::Double)
            }
            DatumType::HistogramI8 => {
                (MetricType::Cumulative, DataType::IntegerDistribution)
            }
            DatumType::HistogramU8 => {
                (MetricType::Cumulative, DataType::IntegerDistribution)
            }
            DatumType::HistogramI16 => {
                (MetricType::Cumulative, DataType::DoubleDistribution)
            }
            DatumType::HistogramU16 => {
                (MetricType::Cumulative, DataType::DoubleDistribution)
            }
            DatumType::HistogramI32 => {
                (MetricType::Cumulative, DataType::DoubleDistribution)
            }
            DatumType::HistogramU32 => {
                (MetricType::Cumulative, DataType::DoubleDistribution)
            }
            DatumType::HistogramI64 => {
                (MetricType::Cumulative, DataType::DoubleDistribution)
            }
            DatumType::HistogramU64 => {
                (MetricType::Cumulative, DataType::DoubleDistribution)
            }
            DatumType::HistogramF32 => {
                (MetricType::Cumulative, DataType::DoubleDistribution)
            }
            DatumType::HistogramF64 => {
                (MetricType::Cumulative, DataType::DoubleDistribution)
            }
        };
        Self {
            name: timeseries_schema.timeseries_name.to_string(),
            fields: timeseries_schema
                .field_schema
                .iter()
                .map(|field| (field.name.clone(), field.field_type))
                .collect(),
            metric_types: vec![metric_type],
            data_types: vec![data_type],
        }
    }

    /// Return the type of the named field, if it is part of the schema.
    pub(crate) fn field_type(&self, name: &str) -> Option<&FieldType> {
        self.fields.get(name)
    }
}
