// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL query plan node for representing a filtering operation.

// Copyright 2024 Oxide Computer Company

use std::collections::BTreeSet;

use crate::oxql::ast::literal::Literal;
use crate::oxql::ast::table_ops::filter::{
    self, implicit_field_names_for_table_schema,
};
use crate::oxql::ast::table_ops::limit;
use crate::oxql::plan::plan::TableOpInput;
use crate::oxql::plan::predicates::Predicates;
use crate::oxql::schema::TableSchema;
use crate::shells::special_idents;

/// A node that filters its input tables.
#[derive(Clone, Debug, PartialEq)]
pub struct Filter {
    /// The input tables to the filter.
    ///
    /// Note that these are also the output tables, since a filter does not
    /// change the schema of its inputs.
    pub input: TableOpInput,

    /// The predicates associated with this filter plan node.
    ///
    /// See the documentation of `Predicates` for details.
    pub predicates: Predicates,
}

impl Filter {
    /// Plan the application of the filter to the input tables.
    pub fn new(
        filter: &filter::Filter,
        input: TableOpInput,
    ) -> anyhow::Result<Self> {
        Self::new_owned(filter.clone(), input)
    }

    /// Same a `Self::new`, but from an owned filter
    pub fn new_owned(
        filter: filter::Filter,
        input: TableOpInput,
    ) -> anyhow::Result<Self> {
        for table in input.tables.iter() {
            Self::ensure_filter_expr_application_is_valid(
                &filter.expr,
                &table.schema,
            )?;
        }
        Ok(Filter { input, predicates: Predicates::Single(filter) })
    }

    pub fn plan_tree_entry(&self) -> termtree::Tree<String> {
        let mut out =
            termtree::Tree::new(String::from("filter")).with_multiline(true);
        out.extend(self.predicates.plan_tree_entries());
        out
    }

    pub fn from_predicates(
        predicates: Predicates,
        input: TableOpInput,
    ) -> anyhow::Result<Self> {
        for table in input.tables.iter() {
            match &predicates {
                Predicates::Single(filter) => {
                    Self::ensure_filter_expr_application_is_valid(
                        &filter.expr,
                        &table.schema,
                    )?;
                }
                Predicates::Disjunctions(disjunctions) => {
                    for filter in
                        disjunctions.iter().filter_map(|dis| dis.as_ref())
                    {
                        Self::ensure_filter_expr_application_is_valid(
                            &filter.expr,
                            &table.schema,
                        )?;
                    }
                }
            }
        }
        Ok(Filter { input, predicates })
    }

    /// Check that the provided filter is valid for a schema.
    pub fn ensure_filter_expr_application_is_valid(
        expr: &filter::FilterExpr,
        schema: &TableSchema,
    ) -> anyhow::Result<()> {
        match expr {
            filter::FilterExpr::Simple(simple) => {
                let implicit_fields =
                    implicit_field_names_for_table_schema(schema);
                // Check that the identifier in the filter is in the table.
                if let Some(type_) = schema.fields.get(simple.ident.as_str()) {
                    // If it names a field, make sure we can apply the filter to
                    // the field as well.
                    anyhow::ensure!(
                        simple.value.is_compatible_with_field(*type_),
                        "Filter expression for field '{}' has type '{}' \
                        which is not compatible with the field of \
                        type '{}'",
                        simple.ident,
                        simple.value.type_name(),
                        type_,
                    );
                } else if implicit_fields.contains(simple.ident.as_str()) {
                    // And check that the filter expression is compatible with the
                    // provided identifier type.
                    Self::ensure_special_ident_is_compatible(
                        simple.ident.as_str(),
                        &simple.value,
                        schema,
                    )?;
                } else {
                    anyhow::bail!(
                        "The filter expression refers to identifiers \
                        that are not valid for its input tables. \
                        Invalid identifiers: [{:?}], valid fields: {:?}",
                        simple.ident.as_str(),
                        schema
                            .fields
                            .keys()
                            .map(String::as_str)
                            .collect::<BTreeSet<_>>()
                            .union(&implicit_fields)
                            .collect::<BTreeSet<_>>()
                    );
                }
            }
            filter::FilterExpr::Compound(compound) => {
                Self::ensure_filter_expr_application_is_valid(
                    &compound.left.expr,
                    &schema,
                )?;
                Self::ensure_filter_expr_application_is_valid(
                    &compound.right.expr,
                    &schema,
                )?;
            }
        }
        Ok(())
    }

    // Check that the provided lteral is valid for a special identifier.
    fn ensure_special_ident_is_compatible(
        ident: &str,
        value: &Literal,
        schema: &TableSchema,
    ) -> anyhow::Result<()> {
        if ident == special_idents::TIMESTAMP
            || ident == special_idents::START_TIME
        {
            anyhow::ensure!(
                matches!(value, Literal::Timestamp(_)),
                "Filter expression must evaluate to a timestamp, \
                but found type '{}'",
                value.type_name(),
            );
        } else if ident == special_idents::DATUM {
            // TODO-completeness: Support writing comparisons on the datum's value
            // by _naming_ the metric, e.g., for `hardware_component:temperature`,
            // writing `filter temperature > 30.0`. That's a bit tricky, since we
            // need to alias the datum column in the SQL queries, or rewrite the
            // filter to refer to the `datum` automatically.
            anyhow::ensure!(
                schema.data_types.len() == 1,
                "The `datum` special identifier may only be used \
                for 1-dimensional tables, but table '{}' has {} dimensions",
                schema.name,
                schema.data_types.len(),
            );
            let data_type = schema.data_types[0];
            anyhow::ensure!(
                value.is_compatible_with_datum(data_type),
                "Filter expression with type '{}' is not comparable \
                with datum of type '{}'",
                value.type_name(),
                data_type,
            );
        } else {
            // TODO-completeness: Support comparison for special distribution
            // identifiers: min, max, mean, p50, p90, p99, and possibly bins and
            // counts.
            anyhow::bail!(
                "Filtering on the special identifier '{}' \
                is not yet supported",
                ident,
            );
        }
        Ok(())
    }

    // Return true if we can reorder a limit around the provided filter.
    pub fn can_reorder_around(&self, limit: &limit::Limit) -> bool {
        match &self.predicates {
            Predicates::Single(f) => f.can_reorder_around(limit),
            Predicates::Disjunctions(disjunctions) => {
                disjunctions.iter().all(|maybe_disjunction| {
                    maybe_disjunction
                        .as_ref()
                        .map(|filter| filter.can_reorder_around(limit))
                        .unwrap_or(true)
                })
            }
        }
    }
}
