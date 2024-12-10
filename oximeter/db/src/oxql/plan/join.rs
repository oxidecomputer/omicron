// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL query plan node for joining tables.

// Copyright 2024 Oxide Computer Company

use crate::oxql::plan::plan::TableOpData;
use crate::oxql::plan::plan::TableOpInput;
use crate::oxql::schema::TableSchema;

/// A node that joins input tables with the same field values.
#[derive(Clone, Debug, PartialEq)]
pub struct Join {
    pub output: TableOpData,
}

impl Join {
    /// Plan the application of a join node.
    pub fn new(input: TableOpInput) -> anyhow::Result<Self> {
        anyhow::ensure!(
            input.tables.len() >= 2,
            "`join` table operations require at least 2 tables",
        );
        let first = input
            .tables
            .first()
            .expect("table operations must have at least one schema");
        let Some(alignment) = first.alignment else {
            anyhow::bail!(
                "All input tables to a `join` operation must \
                be aligned, but table '{}' is not aligned",
                first.schema.name,
            );
        };
        let fields = &first.schema.fields;
        let mut names = Vec::with_capacity(input.tables.len());
        let mut metric_types = Vec::with_capacity(input.tables.len());
        let mut data_types = Vec::with_capacity(input.tables.len());
        for table in input.tables.iter() {
            let Some(this_alignment) = table.alignment else {
                anyhow::bail!(
                    "All input tables to a `join` operation must \
                    be aligned, but table '{}' is not aligned",
                    table.schema.name,
                );
            };
            anyhow::ensure!(
                this_alignment == alignment,
                "All input tables to a `join` operation must have the \
                same alignment, table '{}' was expected to be aligned \
                with {}, but found {}",
                table.schema.name,
                alignment,
                this_alignment,
            );
            anyhow::ensure!(
                table.schema.metric_types.len() == 1,
                "All input tables to `join` operation must be \
                1-dimensional, but table '{}' has {} dimensions",
                table.schema.name,
                table.schema.metric_types.len(),
            );
            anyhow::ensure!(
                &table.schema.fields == fields,
                "All input tables to `join` operation must have \
                the same field names and types, but table '{}' \
                has fields [{}] and table '{}' has fields [{}]",
                table.schema.name,
                table
                    .schema
                    .fields
                    .iter()
                    .map(|(name, typ)| format!("\"{name}\" ({typ})"))
                    .collect::<Vec<_>>()
                    .join(", "),
                first.schema.name,
                fields
                    .iter()
                    .map(|(name, typ)| format!("\"{name}\" ({typ})"))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            names.push(table.schema.name.as_str());
            metric_types.push(table.schema.metric_types[0]);
            data_types.push(table.schema.data_types[0]);
        }
        let name = names.join(",");
        let output = TableOpData {
            schema: TableSchema {
                name,
                fields: fields.clone(),
                metric_types,
                data_types,
            },
            alignment: Some(alignment),
        };
        Ok(Self { output })
    }
}
