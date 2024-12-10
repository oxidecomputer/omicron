// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL query plan node for representing a filtering operation.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::literal::Literal;
use crate::oxql::ast::table_ops::filter;
use crate::oxql::ast::table_ops::filter::implicit_field_names_for_table_schema;
use crate::oxql::ast::table_ops::limit;
use crate::oxql::plan::plan::TableOpInput;
use crate::oxql::plan::predicates::Predicates;
use crate::oxql::schema::TableSchema;
use crate::shells::special_idents;
use std::collections::BTreeSet;
use std::fmt;

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

    // Check that the provided filter expression is valid for a schema, trying
    // to catch as many errors as possible by pushing them onto the input error
    // list.
    fn ensure_filter_expr_application_is_valid_impl(
        expr: &filter::FilterExpr,
        schema: &TableSchema,
        errors: &mut Vec<FilterErrorKind>,
    ) {
        match expr {
            filter::FilterExpr::Simple(simple) => {
                let implicit_fields =
                    implicit_field_names_for_table_schema(schema);
                // Check that the identifier in the filter is in the table.
                if let Some(type_) = schema.fields.get(simple.ident.as_str()) {
                    if !simple.value.is_compatible_with_field(*type_) {
                        let err = FilterErrorKind::IncompatibleExpression {
                            identifier: simple.ident.to_string(),
                            expression_type: simple.value.type_name(),
                            expected_type: type_.to_string(),
                        };
                        errors.push(err);
                    }
                } else if implicit_fields.contains(simple.ident.as_str()) {
                    // And check that the filter expression is compatible with the
                    // provided identifier type.
                    if let Err(e) = Self::ensure_special_ident_is_compatible(
                        simple.ident.as_str(),
                        &simple.value,
                        schema,
                    ) {
                        errors.push(e);
                    }
                } else {
                    let err = FilterErrorKind::InvalidIdentifier {
                        identifier: simple.ident.to_string(),
                        table: schema.name.clone(),
                        valid_identifiers: schema
                            .fields
                            .keys()
                            .map(String::as_str)
                            .collect::<BTreeSet<_>>()
                            .union(&implicit_fields)
                            .map(|s| s.to_string())
                            .collect::<BTreeSet<_>>(),
                    };
                    errors.push(err);
                }
            }
            filter::FilterExpr::Compound(compound) => {
                Self::ensure_filter_expr_application_is_valid_impl(
                    &compound.left.expr,
                    &schema,
                    errors,
                );
                Self::ensure_filter_expr_application_is_valid_impl(
                    &compound.right.expr,
                    &schema,
                    errors,
                );
            }
        }
    }

    /// Check that the provided filter is valid for a schema.
    pub fn ensure_filter_expr_application_is_valid(
        expr: &filter::FilterExpr,
        schema: &TableSchema,
    ) -> anyhow::Result<()> {
        let mut errors = Vec::new();
        Self::ensure_filter_expr_application_is_valid_impl(
            expr,
            schema,
            &mut errors,
        );
        anyhow::ensure!(
            errors.is_empty(),
            "The filter expression \"{}\" is not valid, \
            the following errors were encountered\n{}",
            expr,
            FilterErrorKind::format_list(errors)
        );
        Ok(())
    }

    // Check that the provided lteral is valid for a special identifier.
    fn ensure_special_ident_is_compatible(
        ident: &str,
        value: &Literal,
        schema: &TableSchema,
    ) -> Result<(), FilterErrorKind> {
        if ident == special_idents::TIMESTAMP
            || ident == special_idents::START_TIME
        {
            if matches!(value, Literal::Timestamp(_)) {
                Ok(())
            } else {
                Err(FilterErrorKind::IncompatibleExpression {
                    identifier: ident.to_string(),
                    expression_type: value.type_name(),
                    expected_type: String::from("timestamp"),
                })
            }
        } else if ident == special_idents::DATUM {
            // TODO-completeness: Support writing comparisons on the datum's value
            // by _naming_ the metric, e.g., for `hardware_component:temperature`,
            // writing `filter temperature > 30.0`. That's a bit tricky, since we
            // need to alias the datum column in the SQL queries, or rewrite the
            // filter to refer to the `datum` automatically.
            if schema.data_types.len() != 1 {
                return Err(FilterErrorKind::IdentifierInvalidForNDTables {
                    identifier: ident.to_string(),
                    table: schema.name.clone(),
                    n_dims: schema.data_types.len(),
                });
            }
            let data_type = schema.data_types[0];
            if value.is_compatible_with_datum(data_type) {
                Ok(())
            } else {
                Err(FilterErrorKind::IncompatibleExpression {
                    identifier: ident.to_string(),
                    expected_type: data_type.to_string(),
                    expression_type: value.type_name(),
                })
            }
        } else {
            // TODO-completeness: Support comparison for special distribution
            // identifiers: min, max, mean, p50, p90, p99, and possibly bins and
            // counts.
            Err(FilterErrorKind::FilteringNotSupported {
                identifier: ident.to_string(),
            })
        }
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

// Helper error enum to catch all errors applying a filter expression to a
// table.
#[derive(Clone, Debug)]
enum FilterErrorKind {
    // The filter expression itself isn't compatible with the type of the field
    // the filter applies to.
    IncompatibleExpression {
        identifier: String,
        expected_type: String,
        expression_type: &'static str,
    },
    // The identifier isn't one of the input tables' identifiers.
    InvalidIdentifier {
        identifier: String,
        table: String,
        valid_identifiers: BTreeSet<String>,
    },
    // Special identifier only valid for 1-D tables
    IdentifierInvalidForNDTables {
        identifier: String,
        table: String,
        n_dims: usize,
    },
    // Filter is not yet supported on this identifier.
    FilteringNotSupported {
        identifier: String,
    },
}

impl FilterErrorKind {
    fn format_list(errors: Vec<Self>) -> String {
        assert!(!errors.is_empty());
        // Pull together the errors about invalid idents, since those all share
        // the same _valid_ identifiers.
        let (invalid_idents, others): (Vec<_>, Vec<_>) = errors
            .into_iter()
            .partition(|err| matches!(err, Self::InvalidIdentifier { .. }));
        let maybe_invalid_ident_message =
            Self::invalid_identifier_message(invalid_idents);
        maybe_invalid_ident_message
            .into_iter()
            .chain(others.into_iter().map(|msg| msg.to_string()))
            .map(|msg| format!("  > {msg}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn invalid_identifier_message(
        errors: Vec<FilterErrorKind>,
    ) -> Option<String> {
        let Some(FilterErrorKind::InvalidIdentifier {
            table,
            valid_identifiers,
            ..
        }) = errors.first()
        else {
            return None;
        };
        let all_invalid_idents = errors
            .iter()
            .map(|err| {
                let Self::InvalidIdentifier { identifier, .. } = err else {
                    unreachable!();
                };
                format!("\"{}\"", identifier)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let e = format!(
            "The filter expression refers to identifiers \
            that are not valid for its input table \"{}\". Invalid \
            identifiers: [{}], valid identifiers: [{}]",
            table,
            all_invalid_idents,
            valid_identifiers
                .iter()
                .map(|ident| format!("\"{}\"", ident))
                .collect::<Vec<_>>()
                .join(", ")
        );
        Some(e)
    }
}

impl fmt::Display for FilterErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FilterErrorKind::IncompatibleExpression {
                identifier: field,
                expected_type,
                expression_type,
            } => {
                write!(
                    f,
                    "Filter expression for identifier \"{}\" has type \"{}\" \
                    which is not compatible with the expected type \"{}\"",
                    field, expression_type, expected_type,
                )
            }
            FilterErrorKind::InvalidIdentifier {
                identifier,
                table,
                valid_identifiers,
            } => {
                write!(
                    f,
                    "The filter expression refers to an identifier \
                    that is not valid for its input table \"{}\". \
                    Invalid identifier: \"{}\", valid identifiers: [{}]",
                    identifier,
                    table,
                    valid_identifiers
                        .iter()
                        .map(|ident| format!("\"{}\"", ident))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            FilterErrorKind::IdentifierInvalidForNDTables {
                identifier,
                table,
                n_dims,
            } => {
                write!(
                    f,
                    "The special identifier \"{}\" may only be used \
                    for 1-dimensional tables, but table \"{}\" has {} dimensions",
                    identifier, table, n_dims,
                )
            }
            FilterErrorKind::FilteringNotSupported { identifier } => {
                write!(
                    f,
                    "Filtering on the special identifier \"{}\" is \
                    not yet supported",
                    identifier,
                )
            }
        }
    }
}
