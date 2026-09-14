// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL query plan node for aligning tables.

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
    // Not every method accepts every metric type. The check lives on the
    // method itself, so that this and `align_and_aggregate()` cannot disagree
    // about what is allowed.
    for metric_type in schema.metric_types.iter() {
        method.check_metric_type(*metric_type, &schema.name)?;
    }
    // Check the data type and the method separately, rather than matching on
    // the pair of them. A combined match needs a `(_, _)` arm to cover the
    // non-numeric data types, and that arm silently swallows any alignment
    // method not listed above it -- reporting an unhandled method as a bad
    // *data type*, which is both wrong and hard to chase down. Matching the
    // method on its own means a new variant fails to compile instead.
    let mut data_types = Vec::with_capacity(schema.data_types.len());
    for data_type in schema.data_types.iter() {
        anyhow::ensure!(
            data_type.is_numeric(),
            "Tables with '{}' data types cannot be aligned",
            data_type,
        );
        match method {
            align::AlignmentMethod::MeanWithin
            | align::AlignmentMethod::Rate
            | align::AlignmentMethod::Min
            | align::AlignmentMethod::Max => {
                data_types.push(method.output_data_type(*data_type));
            }
            align::AlignmentMethod::Interpolate => {
                anyhow::bail!(
                    "Alignment via interpolation is not yet implemented"
                );
            }
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

    // A gauge table with the provided data type, which every alignment method
    // accepts.
    fn gauge_schema(data_type: DataType) -> TableSchema {
        TableSchema {
            name: String::from("foo:bar"),
            fields: BTreeMap::from([(String::from("a"), FieldType::Bool)]),
            metric_types: vec![MetricType::Gauge],
            data_types: vec![data_type],
        }
    }

    #[test]
    fn test_selectors_preserve_the_input_data_type() {
        for method in [align::AlignmentMethod::Min, align::AlignmentMethod::Max]
        {
            let out =
                align_input_schema(gauge_schema(DataType::Integer), method)
                    .unwrap();
            assert_eq!(
                out.data_types,
                vec![DataType::Integer],
                "`align {method}(..)` selects one of the input points rather \
                than computing a new value, so an integer metric must stay an \
                integer. Widening to a double would lose precision on the way \
                out for no reason.",
            );

            let out =
                align_input_schema(gauge_schema(DataType::Double), method)
                    .unwrap();
            assert_eq!(out.data_types, vec![DataType::Double]);
        }
    }

    #[test]
    fn test_aggregates_widen_to_a_double() {
        // Each method is paired with a metric type it accepts: rate is
        // defined only over deltas.
        let cases = [
            (align::AlignmentMethod::MeanWithin, MetricType::Gauge),
            (align::AlignmentMethod::MeanWithin, MetricType::Delta),
            (align::AlignmentMethod::Rate, MetricType::Delta),
        ];
        for (method, metric_type) in cases {
            for input in [DataType::Integer, DataType::Double] {
                let schema = TableSchema {
                    data_types: vec![input],
                    ..schema_with_metric_type(metric_type)
                };
                let out = align_input_schema(schema, method).unwrap();
                assert_eq!(
                    out.data_types,
                    vec![DataType::Double],
                    "`align {method}(..)` computes a value that was not in \
                    the input, in floating point, so it widens to a double",
                );
            }
        }
    }

    #[test]
    fn test_non_numeric_data_types_cannot_be_aligned() {
        for data_type in [
            DataType::Boolean,
            DataType::String,
            DataType::IntegerDistribution,
            DataType::DoubleDistribution,
        ] {
            let err = align_input_schema(
                gauge_schema(data_type),
                align::AlignmentMethod::MeanWithin,
            )
            .expect_err("Non-numeric data types cannot be aligned");
            assert!(
                err.to_string().contains("cannot be aligned"),
                "Unexpected error for data type {data_type}: {err}",
            );
        }
    }

    // A table with the provided metric type, and a numeric data type that
    // every alignment method accepts.
    fn schema_with_metric_type(metric_type: MetricType) -> TableSchema {
        TableSchema {
            name: String::from("foo:bar"),
            fields: BTreeMap::from([(String::from("a"), FieldType::Bool)]),
            metric_types: vec![metric_type],
            data_types: vec![DataType::Integer],
        }
    }

    #[test]
    fn test_rate_rejects_a_gauge() {
        let err = align_input_schema(
            schema_with_metric_type(MetricType::Gauge),
            align::AlignmentMethod::Rate,
        )
        .expect_err(
            "`align rate(..)` sums the amounts in each delta. Applied to a \
            gauge it would sum levels instead, which is not an approximation \
            but a category error, so it must be refused rather than silently \
            producing a number that scales with the size of the gauge.",
        );
        assert!(
            err.to_string()
                .contains("rate alignment does not yet support gauge metrics"),
            "Unexpected error: {err}",
        );
    }

    #[test]
    fn test_rate_accepts_a_delta() {
        let out = align_input_schema(
            schema_with_metric_type(MetricType::Delta),
            align::AlignmentMethod::Rate,
        )
        .expect("A delta is what `align rate(..)` is defined over");
        assert_eq!(out.metric_types, vec![MetricType::Gauge]);
        assert_eq!(out.data_types, vec![DataType::Double]);
    }

    #[test]
    fn test_min_and_max_reject_a_delta() {
        for method in [align::AlignmentMethod::Min, align::AlignmentMethod::Max]
        {
            let err = align_input_schema(
                schema_with_metric_type(MetricType::Delta),
                method,
            )
            .expect_err(
                "Producers pick their own sample intervals, so the largest of \
                a run of deltas is whichever covered the longest span rather \
                than whichever was busiest. The first delta of each epoch is \
                worse still: it carries a whole cumulative value rather than \
                an increment, so an extremum would reliably select it.",
            );
            let err = err.to_string();
            assert!(
                err.contains("min and max alignment require a gauge metric"),
                "Unexpected error: {err}",
            );
            assert!(
                err.contains("align rate(10s) | align max(1m)"),
                "The error must point at the spelling that does work, since \
                nearly every counter in the system reaches alignment as a \
                delta and would otherwise look arbitrarily forbidden: {err}",
            );
        }
    }

    #[test]
    fn test_min_and_max_accept_a_gauge() {
        for method in [align::AlignmentMethod::Min, align::AlignmentMethod::Max]
        {
            let out = align_input_schema(
                schema_with_metric_type(MetricType::Gauge),
                method,
            )
            .expect("A gauge is what the selectors are defined over");
            assert_eq!(out.metric_types, vec![MetricType::Gauge]);
        }
    }

    #[test]
    fn test_every_method_rejects_a_cumulative() {
        for method in [
            align::AlignmentMethod::MeanWithin,
            align::AlignmentMethod::Rate,
            align::AlignmentMethod::Min,
            align::AlignmentMethod::Max,
        ] {
            let err = align_input_schema(
                schema_with_metric_type(MetricType::Cumulative),
                method,
            )
            .expect_err("Cumulative metrics must be converted to deltas first");
            assert!(
                err.to_string().contains("cumulative metric type"),
                "Unexpected error for {method}: {err}",
            );
        }
    }

    #[test]
    fn test_interpolation_is_still_rejected() {
        let err = align_input_schema(
            gauge_schema(DataType::Double),
            align::AlignmentMethod::Interpolate,
        )
        .expect_err("Alignment by interpolation is not implemented");
        assert!(
            err.to_string().contains("not yet implemented"),
            "Unexpected error: {err}",
        );
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
