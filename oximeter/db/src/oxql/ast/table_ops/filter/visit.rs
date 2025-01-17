// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Methods for walking a tree of filter nodes and applying a function to them.

// Copyright 2024 Oxide Computer Company

use std::time::Duration;

use super::CompoundFilter;
use super::Filter;
use super::FilterExpr;
use super::SimpleFilter;
use crate::oxql::ast::cmp::Comparison;
use crate::oxql::ast::literal::Literal;
use crate::oxql::ast::logical_op::LogicalOp;
use crate::oxql::schema::TableSchema;
use crate::oxql::Error;
use crate::shells::special_idents;

/// A trait for visiting a tree of filter nodes.
///
/// This is used to walk a tree of filter nodes and produce some output based
/// on them. For example, this can be used to convert the tree into a string
/// representation for use in the database.
pub trait Visit {
    /// The result of applying the visitor to a simple filter node.
    type Output;

    /// Visit one simple filter node, returning the output.
    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error>;

    /// Combine the output of previously-visited filter nodes.
    fn combine(
        &self,
        negated: bool,
        left: Self::Output,
        right: Self::Output,
        op: LogicalOp,
    ) -> Self::Output;
}

/// A visitor that shifts timestamps.
///
/// This is used to correct timestamps when handling alignment operations.
/// Suppose we have a query like:
///
/// ```ignore
/// ... | align mean_within(1m) | filter timestamp > t0
/// ```
///
/// That alignment method takes into account measurements in one minute windows.
/// That means data from t0 - 1m ultimately goes into the measurement that
/// appears at t0 in the output itself, i.e., it includes data in the window
/// `[t0 - 1m, t0]`.
///
/// If we naively push the filter through the alignment method, then we'll
/// truncate that window to only `[t0, t0]`, which changes the data we output.
/// By shifting the timestamp in the filter expression forward as we push them
/// through, we ensure the result set is not changed by that optimization.
pub struct ShiftTimestamps {
    /// The period by which we shift timestamps.
    pub period: Duration,
}

impl Visit for ShiftTimestamps {
    type Output = Filter;

    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error> {
        let SimpleFilter { ident, cmp, value } = filter;
        if ident.as_str() == special_idents::TIMESTAMP {
            let Literal::Timestamp(timestamp) = value else {
                anyhow::bail!("Filter expression is not a timestamp");
            };
            let new_timestamp = match cmp {
                Comparison::Eq
                | Comparison::Ne
                | Comparison::Lt
                | Comparison::Le => *timestamp,
                Comparison::Gt | Comparison::Ge => *timestamp - self.period,
                Comparison::Like => unreachable!(),
            };
            Ok(Some(Filter {
                negated,
                expr: FilterExpr::Simple(SimpleFilter {
                    value: Literal::Timestamp(new_timestamp),
                    ..filter.clone()
                }),
            }))
        } else {
            Ok(Some(Filter {
                negated,
                expr: FilterExpr::Simple(filter.clone()),
            }))
        }
    }

    fn combine(
        &self,
        negated: bool,
        left: Self::Output,
        right: Self::Output,
        op: LogicalOp,
    ) -> Self::Output {
        default_combine_impl(negated, left, right, op)
    }
}

// A helper that implements the default `Visitor::combine()` method.
//
// We'd like to make this the default impl of the method itself, but that would
// require that the type `Visitor::Output` have a default, which isn't yet
// stable.
fn default_combine_impl(
    negated: bool,
    left: Filter,
    right: Filter,
    op: LogicalOp,
) -> Filter {
    Filter {
        negated,
        expr: FilterExpr::Compound(CompoundFilter {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }),
    }
}

/// A visitor that _keeps_ filter expressions that refer to the datum.
pub struct OnlyDatum<'a> {
    pub schema: &'a TableSchema,
}

impl Visit for OnlyDatum<'_> {
    type Output = Filter;

    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error> {
        // It should be a datum. Check and remove it.
        if filter.ident.as_str() == special_idents::DATUM {
            for ty in self.schema.data_types.iter().copied() {
                anyhow::ensure!(
                    filter.value.is_compatible_with_datum(ty),
                    "Expression for datum on table '{}' is not \
                    compatible with its type ({})",
                    self.schema.name,
                    ty,
                );
            }
            return Ok(Some(Filter {
                negated,
                expr: FilterExpr::Simple(filter.clone()),
            }));
        }

        // Anything else is outta here.
        Ok(None)
    }

    fn combine(
        &self,
        negated: bool,
        left: Self::Output,
        right: Self::Output,
        op: LogicalOp,
    ) -> Self::Output {
        default_combine_impl(negated, left, right, op)
    }
}

/// A visitor that removes filter expressions that refer to the datum.
pub struct RemoveDatum<'a> {
    pub schema: &'a TableSchema,
}

impl Visit for RemoveDatum<'_> {
    type Output = Filter;

    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error> {
        let ident = filter.ident.as_str();
        // First check if the ident refers to a field, in which case we return
        // it directly. We don't handle `None`, instead moving on to check if
        // the filter ident refers to the datum, timestamp, etc.
        if let Some(field_type) = self.schema.field_type(ident) {
            if !filter.value_type_is_compatible_with_field(*field_type) {
                return Err(anyhow::anyhow!(
                    "Expression for field '{}' is not compatible with \
                    its type ({})",
                    filter.ident,
                    field_type,
                ));
            }
            return Ok(Some(Filter {
                negated,
                expr: FilterExpr::Simple(filter.clone()),
            }));
        }

        // The relevant columns on which we filter depend on the datum
        // type of the table. All tables support "timestamp".
        if ident == special_idents::TIMESTAMP {
            if matches!(filter.value, Literal::Timestamp(_)) {
                return Ok(Some(Filter {
                    negated,
                    expr: FilterExpr::Simple(filter.clone()),
                }));
            }
            return Err(anyhow::anyhow!(
                "Expression cannot be compared with a timestamp"
            ));
        }

        // Check for comparison against the start time, which only works
        // for cumulative tables.
        if ident == special_idents::START_TIME {
            if !self.schema.metric_types[0].is_cumulative() {
                return Err(anyhow::anyhow!(
                    "Start time can only be compared if the metric \
                    is cumulative, but table '{}' has metric type {}",
                    self.schema.name,
                    &self.schema.metric_types[0],
                ));
            }
            if matches!(filter.value, Literal::Timestamp(_)) {
                return Ok(Some(Filter {
                    negated,
                    expr: FilterExpr::Simple(filter.clone()),
                }));
            }
            return Err(anyhow::anyhow!(
                "Expression cannot be compared with a timestamp"
            ));
        }

        // Remove the filter iff it refers to the datum.
        if ident == special_idents::DATUM {
            return Ok(None);
        }

        // Anything else is a bug.
        unreachable!("Filter identifier '{}' is not valid", filter.ident);
    }

    fn combine(
        &self,
        negated: bool,
        left: Self::Output,
        right: Self::Output,
        op: LogicalOp,
    ) -> Self::Output {
        default_combine_impl(negated, left, right, op)
    }
}

/// A visitor that restricts filter expressions to those that apply to the
/// _fields_ of a table schema.
pub struct RestrictToFields<'a> {
    pub schema: &'a TableSchema,
}

impl Visit for RestrictToFields<'_> {
    type Output = Filter;

    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error> {
        let Some(field_type) = self.schema.field_type(filter.ident.as_str())
        else {
            return Ok(None);
        };
        if !filter.value_type_is_compatible_with_field(*field_type) {
            return Err(anyhow::anyhow!(
                "Expression for field '{}' is not compatible with \
                its type ({})",
                filter.ident,
                field_type,
            ));
        }
        Ok(Some(Filter { negated, expr: FilterExpr::Simple(filter.clone()) }))
    }

    fn combine(
        &self,
        negated: bool,
        left: Self::Output,
        right: Self::Output,
        op: LogicalOp,
    ) -> Self::Output {
        Filter {
            negated,
            expr: FilterExpr::Compound(CompoundFilter {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }),
        }
    }
}

/// A visitor that restricts filter expressions to those that apply to the
/// _measurements_ of a table schema.
pub struct RestrictToMeasurements<'a> {
    pub schema: &'a TableSchema,
}

impl Visit for RestrictToMeasurements<'_> {
    type Output = Filter;

    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error> {
        // The relevant columns on which we filter depend on the datum
        // type of the table. All tables support "timestamp".
        let ident = filter.ident.as_str();
        if ident == special_idents::TIMESTAMP {
            if matches!(filter.value, Literal::Timestamp(_)) {
                return Ok(Some(Filter {
                    negated,
                    expr: FilterExpr::Simple(filter.clone()),
                }));
            }
            return Err(anyhow::anyhow!(
                "Expression cannot be compared with a timestamp"
            ));
        }

        // Check for comparison against the start time, which only works
        // for cumulative tables.
        if ident == special_idents::START_TIME {
            if !self.schema.metric_types[0].is_cumulative() {
                return Err(anyhow::anyhow!(
                    "Start time can only be compared if the metric \
                    is cumulative, but table '{}' has metric type {}",
                    self.schema.name,
                    &self.schema.metric_types[0],
                ));
            }
            if matches!(filter.value, Literal::Timestamp(_)) {
                return Ok(Some(Filter {
                    negated,
                    expr: FilterExpr::Simple(filter.clone()),
                }));
            }
            return Err(anyhow::anyhow!(
                "Expression cannot be compared with a timestamp"
            ));
        }

        // We'll delegate to the actual table op to filter on any of the
        // data columns.
        //
        // TODO-completeness: We should allow filtering here once we
        // push deltas into the database.
        Ok(None)
    }

    fn combine(
        &self,
        negated: bool,
        left: Self::Output,
        right: Self::Output,
        op: LogicalOp,
    ) -> Self::Output {
        default_combine_impl(negated, left, right, op)
    }
}

/// Rewrite the filters so that they apply to the database field tables only.
pub struct RewriteForFieldTables<'a> {
    pub schema: &'a TableSchema,
}

impl Visit for RewriteForFieldTables<'_> {
    type Output = String;

    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error> {
        // If the predicate names a field in this table schema,
        // return that predicate printed as a string. If not, we return
        // None.
        let Some(field_type) = self.schema.field_type(filter.ident.as_str())
        else {
            return Ok(None);
        };
        if !filter.value_type_is_compatible_with_field(*field_type) {
            return Err(anyhow::anyhow!(
                "Expression for field '{}' is not compatible with \
                its type ({})",
                filter.ident,
                field_type,
            ));
        }
        let maybe_not = if negated { "NOT " } else { "" };
        Ok(Some(format!("{}{}", maybe_not, filter.as_db_safe_string())))
    }

    fn combine(
        &self,
        negated: bool,
        left: Self::Output,
        right: Self::Output,
        op: LogicalOp,
    ) -> Self::Output {
        let maybe_not = if negated { "NOT " } else { "" };
        format!(
            "{}{}({}, {})",
            maybe_not,
            op.as_db_function_name(),
            left,
            right
        )
    }
}

/// Rewrite filters so that they apply only to the database measurement table.
pub struct RewriteForMeasurementTable<'a> {
    pub schema: &'a TableSchema,
}

impl Visit for RewriteForMeasurementTable<'_> {
    type Output = String;

    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error> {
        let maybe_not = if negated { "NOT " } else { "" };
        // The relevant columns on which we filter depend on the datum
        // type of the table. All tables support "timestamp".
        let ident = filter.ident.as_str();
        if ident == special_idents::TIMESTAMP {
            if matches!(filter.value, Literal::Timestamp(_)) {
                return Ok(Some(format!(
                    "{}{}",
                    maybe_not,
                    filter.as_db_safe_string()
                )));
            }
            return Err(anyhow::anyhow!(
                "Expression cannot be compared with a timestamp"
            ));
        }

        // Check for comparison against the start time, which only works
        // for cumulative tables.
        if ident == special_idents::START_TIME {
            if !self.schema.metric_types[0].is_cumulative() {
                return Err(anyhow::anyhow!(
                    "Start time can only be compared if the metric \
                    is cumulative, but table '{}' has metric type {}",
                    self.schema.name,
                    &self.schema.metric_types[0],
                ));
            }
            if matches!(filter.value, Literal::Timestamp(_)) {
                return Ok(Some(format!(
                    "{}{}",
                    maybe_not,
                    filter.as_db_safe_string()
                )));
            }
            return Err(anyhow::anyhow!(
                "Expression cannot be compared with a timestamp"
            ));
        }

        // We'll delegate to the actual table op to filter on any of the data
        // columns.
        //
        // TODO-completeness: We should allow pushing the filters here once we
        // push deltas into the database.
        Ok(None)
    }

    fn combine(
        &self,
        negated: bool,
        left: Self::Output,
        right: Self::Output,
        op: LogicalOp,
    ) -> Self::Output {
        let maybe_not = if negated { "NOT " } else { "" };
        format!(
            "{}{}({}, {})",
            maybe_not,
            op.as_db_function_name(),
            left,
            right
        )
    }
}
