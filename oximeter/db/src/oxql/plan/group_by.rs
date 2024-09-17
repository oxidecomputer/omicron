// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL query plan node for grouping data by fields.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::table_ops::group_by;
use crate::oxql::plan::plan::TableOpData;
use crate::oxql::plan::plan::TableOpInput;
use crate::oxql::schema::TableSchema;
use oxql_types::point::DataType;
use oxql_types::point::MetricType;
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// A node that groups input tables with the same values for the listed field.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupBy {
    pub output: TableOpData,
    pub group_by: group_by::GroupBy,
}

impl GroupBy {
    /// Plan the application of a group_by operation to the input tables.
    pub fn new(
        group_by: &group_by::GroupBy,
        input: TableOpInput,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            input.tables.len() == 1,
            "`group_by` table operations require exactly one input \
            table, but found {}",
            input.tables.len(),
        );
        let table = input
            .tables
            .into_iter()
            .next()
            .expect("table operations must have at least one schema");
        anyhow::ensure!(
            table.alignment.is_some(),
            "Input table to a `group_by` table operation must be aligned, \
            but table '{}' is not aligned",
            table.schema.name,
        );
        anyhow::ensure!(
            table.schema.metric_types.len() == 1,
            "`group_by` table operations require 1-dimensional tables, but \
            table '{}' has {} dimensions",
            table.schema.name,
            table.schema.metric_types.len(),
        );
        anyhow::ensure!(
            table.schema.metric_types[0] != MetricType::Cumulative,
            "`group_by` table operations require delta or gauge metric \
            types, but table '{}' is cumulative",
            table.schema.name,
        );
        anyhow::ensure!(
            table.schema.data_types[0].is_numeric(),
            "`group_by` table operations require numeric data types, \
            but table '{}' has data type '{}'",
            table.schema.name,
            table.schema.data_types[0],
        );
        let output_data_type =
            match (&table.schema.data_types[0], group_by.reducer) {
                (DataType::Integer, group_by::Reducer::Sum) => {
                    DataType::Integer
                }
                (
                    DataType::Double,
                    group_by::Reducer::Mean | group_by::Reducer::Sum,
                )
                | (DataType::Integer, group_by::Reducer::Mean) => {
                    DataType::Double
                }
                (ty, _) => anyhow::bail!(
                    "`group_by` table operations require numeric data types, \
                    but table '{}' has data type '{}'",
                    table.schema.name,
                    ty,
                ),
            };
        let mut output_fields = BTreeMap::new();
        for ident in group_by.identifiers.iter() {
            let Some(type_) = table.schema.fields.get(ident.as_str()) else {
                anyhow::bail!(
                    "Cannot group by field '{}', which does not appear in the \
                    input tables. Valid fields are: {:?}",
                    ident.as_str(),
                    table
                        .schema
                        .fields
                        .keys()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                );
            };
            output_fields.insert(ident.to_string(), *type_);
        }
        let output_schema = TableSchema {
            data_types: vec![output_data_type],
            fields: output_fields,
            ..table.schema
        };
        let output =
            TableOpData { schema: output_schema, alignment: table.alignment };
        Ok(GroupBy { output, group_by: group_by.clone() })
    }

    fn output_data_type(&self) -> DataType {
        self.output.schema.data_types[0]
    }

    /// Print this node as a plan tree entry.
    pub fn plan_tree_entry(&self) -> termtree::Tree<String> {
        let mut out = String::from("group_by: fields=[");
        let n_fields = self.group_by.identifiers.len();
        for (i, field) in self.group_by.identifiers.iter().enumerate() {
            out.push_str(field.as_str());
            if i < n_fields - 1 {
                out.push(',');
            }
        }
        out.push_str("], reducer=");
        write!(out, "{}, ", self.group_by.reducer).unwrap();
        out.push_str("output type=");
        write!(out, "{}", self.output_data_type()).unwrap();
        termtree::Tree::new(out)
    }
}
