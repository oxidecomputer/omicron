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
/// This is used to walk a tree of filter nodes and produce some oputput based
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
pub struct ShiftTimestamps {
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

/// A visitor that _keeps_ filter expressions that refer to the datum.
pub struct OnlyDatum<'a> {
    pub schema: &'a TableSchema,
}

impl<'a> Visit for OnlyDatum<'a> {
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
                    "Expression for datum on table {} is not \
                    compatible with its type {}",
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

/// A visitor that removes filter expressions that refer to the datum.
pub struct RemoveDatum<'a> {
    pub schema: &'a TableSchema,
}

impl<'a> Visit for RemoveDatum<'a> {
    type Output = Filter;

    fn visit_simple(
        &self,
        negated: bool,
        filter: &SimpleFilter,
    ) -> Result<Option<Self::Output>, Error> {
        if let Some(field_type) = self.schema.field_type(filter.ident.as_str())
        {
            if !filter.value_type_is_compatible_with_field(*field_type) {
                return Err(anyhow::anyhow!(
                    "Expression for field {} is not compatible with \
                    its type {}",
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
        let ident = filter.ident.as_str();
        if ident == special_idents::TIMESTAMP {
            if matches!(filter.value, Literal::Timestamp(_)) {
                return Ok(Some(Filter {
                    negated,
                    expr: FilterExpr::Simple(filter.clone()),
                }));
            }
            return Err(anyhow::anyhow!(
                "Literal cannot be compared with a timestamp"
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
                "Literal cannot be compared with a timestamp"
            ));
        }

        // It should be a datum. Check and remove it.
        if ident == special_idents::DATUM {
            return Ok(None);
        }

        // Anything else is a bug.
        unreachable!("Filter identifier '{}' is not valid", filter.ident,);
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
/// _fields_ of a table schema.
pub struct RestrictToFields<'a> {
    pub schema: &'a TableSchema,
}

impl<'a> Visit for RestrictToFields<'a> {
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
                "Expression for field {} is not compatible with \
                its type {}",
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

impl<'a> Visit for RestrictToMeasurements<'a> {
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
                "Literal cannot be compared with a timestamp"
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
                "Literal cannot be compared with a timestamp"
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

/// Rewrite the filters so that they apply to the database field tables only.
pub struct RewriteForFieldTables<'a> {
    pub schema: &'a TableSchema,
}

impl<'a> Visit for RewriteForFieldTables<'a> {
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
                "Expression for field {} is not compatible with \
                its type {}",
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
        format!("{}{}({left}, {right})", maybe_not, op.as_db_function_name())
    }
}

/// Rewrite filters so that they apply only to the database measurement table.
pub struct RewriteForMeasurementTable<'a> {
    pub schema: &'a TableSchema,
}

impl<'a> Visit for RewriteForMeasurementTable<'a> {
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
                "Literal cannot be compared with a timestamp"
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
                "Literal cannot be compared with a timestamp"
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
        format!("{}{}({left}, {right})", maybe_not, op.as_db_function_name())
    }
}

#[cfg(test)]
mod tests {}
