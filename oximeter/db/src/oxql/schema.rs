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

/// The schema for a table in an OxQL query.
///
/// This type is used in OxQL query planning and execution. It describes the
/// schema of the data that is sent into or emitted from any table operation.
/// These are initially built directly from the schema of the raw data stored in
/// the database, which is a [`TimeseriesSchema`]. The query planner generates
/// new schema as it plans a query, representing the expected data
/// transformation that table operation performs. The planner also uses these to
/// validate the query, such as ensuring that identifiers in the query actually
/// appear on the tables they're operating on, or that data types are correct,
/// for example.
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
                (MetricType::Cumulative, DataType::IntegerDistribution)
            }
            DatumType::HistogramU16 => {
                (MetricType::Cumulative, DataType::IntegerDistribution)
            }
            DatumType::HistogramI32 => {
                (MetricType::Cumulative, DataType::IntegerDistribution)
            }
            DatumType::HistogramU32 => {
                (MetricType::Cumulative, DataType::IntegerDistribution)
            }
            DatumType::HistogramI64 => {
                (MetricType::Cumulative, DataType::IntegerDistribution)
            }
            DatumType::HistogramU64 => {
                (MetricType::Cumulative, DataType::IntegerDistribution)
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

    /// Return the number of dimensions in the table.
    ///
    /// This is the number of data values in each measurement.
    pub fn n_dims(&self) -> usize {
        self.metric_types.len()
    }
}
