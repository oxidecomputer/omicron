// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL query plan node for aligning tables.

use oxql_types::point::DataType;
use oxql_types::point::MetricType;

use crate::oxql::ast::table_ops::align;
use crate::oxql::plan::plan::TableOpData;
use crate::oxql::plan::plan::TableOpInput;
use crate::oxql::plan::plan::TableOpOutput;
use crate::oxql::schema::TableSchema;

/// A node that aligns its input tables.
#[derive(Clone, Debug, PartialEq)]
pub struct Align {
    pub output: TableOpOutput,
    pub alignment: align::Align,
}

impl Align {
    /// Plan the application of the alignment operation to the input tables
    pub fn new(
        alignment: align::Align,
        input: TableOpInput,
    ) -> anyhow::Result<Self> {
        let tables = input
            .tables
            .into_iter()
            .map(|TableOpData { schema, .. }| {
                align_input_schema(schema, alignment.method).map(|schema| {
                    TableOpData { schema, alignment: Some(alignment) }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output = TableOpOutput { tables };
        Ok(Align { output, alignment })
    }

    /// Print this plan node as a plan tree entry.
    pub fn plan_tree_entry(&self) -> termtree::Tree<String> {
        termtree::Tree::new(format!(
            "align: method={}, period={:?}",
            self.alignment.method, self.alignment.period
        ))
    }
}

// Align the input schema, returning the output schema it will produce.
fn align_input_schema(
    schema: TableSchema,
    method: align::AlignmentMethod,
) -> anyhow::Result<TableSchema> {
    for metric_type in schema.metric_types.iter() {
        anyhow::ensure!(
            metric_type != &MetricType::Cumulative,
            "Only gauge or delta metric types may be aligned, \
            but table '{}' has cumulative metric type",
            schema.name,
        );
    }
    let mut data_types = Vec::with_capacity(schema.data_types.len());
    for data_type in schema.data_types.iter() {
        match (data_type, method) {
            (
                DataType::Integer | DataType::Double,
                align::AlignmentMethod::MeanWithin,
            ) => {
                data_types.push(DataType::Double);
            }
            (
                DataType::Integer | DataType::Double,
                align::AlignmentMethod::Interpolate,
            ) => {
                anyhow::bail!(
                    "Alignment via interpolation is not yet implemented"
                );
            }
            (_, _) => anyhow::bail!(
                "Tables with '{}' data types cannot be aligned",
                data_type,
            ),
        }
    }
    // Report the metric type each method actually emits, so that later table
    // ops -- including a second alignment -- plan against alignment's output
    // rather than against the metric type of its input. Every method today
    // emits a gauge, but that is a property of the methods rather than of
    // alignment, so ask each one. See `AlignmentMethod::output_metric_type()`.
    let metric_types = schema
        .metric_types
        .iter()
        .map(|metric_type| method.output_metric_type(*metric_type))
        .collect();
    Ok(TableSchema { metric_types, data_types, ..schema })
}

#[cfg(test)]
mod test {
    use super::TableSchema;
    use super::align;
    use super::align_input_schema;
    use oximeter::FieldType;
    use oxql_types::point::DataType;
    use oxql_types::point::MetricType;
    use std::collections::BTreeMap;

    #[test]
    fn test_align_input_schema() {
        let schema = TableSchema {
            name: String::from("foo:bar"),
            fields: BTreeMap::from([
                (String::from("a"), FieldType::Bool),
                (String::from("b"), FieldType::String),
            ]),
            metric_types: vec![MetricType::Gauge],
            data_types: vec![DataType::Integer],
        };
        let method = align::AlignmentMethod::MeanWithin;

        let out = align_input_schema(schema.clone(), method).unwrap();
        assert_eq!(out.name, schema.name);
        assert_eq!(out.fields, schema.fields);
        assert_eq!(out.metric_types, vec![MetricType::Gauge]);
        assert_eq!(out.data_types[0], DataType::Double);
    }

    #[test]
    fn test_align_input_schema_reports_the_method_output_metric_type() {
        let schema = TableSchema {
            name: String::from("foo:bar"),
            fields: BTreeMap::from([(String::from("a"), FieldType::Bool)]),
            metric_types: vec![MetricType::Delta],
            data_types: vec![DataType::Integer],
        };

        let out = align_input_schema(
            schema.clone(),
            align::AlignmentMethod::MeanWithin,
        )
        .unwrap();
        assert_eq!(
            out.metric_types,
            vec![MetricType::Gauge],
            "Aligning a delta by mean must report a gauge output, since that \
            is what `align_and_aggregate()` emits. Reporting the input metric \
            type here would reject a following table op that only accepts \
            gauges, such as a second `align min(..)` or `align max(..)`.",
        );
        assert_eq!(
            out.metric_types,
            vec![
                align::AlignmentMethod::MeanWithin
                    .output_metric_type(MetricType::Delta)
            ],
            "The planned metric type must come from the alignment method \
            itself. A method that re-buckets deltas onto the output windows, \
            as PromQL's `increase()` does, emits a delta rather than a gauge, \
            and this function must not assume otherwise.",
        );
    }
}
