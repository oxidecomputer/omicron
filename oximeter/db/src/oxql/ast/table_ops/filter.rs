// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An AST node describing filtering table operations.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::cmp::Comparison;
use crate::oxql::ast::ident::Ident;
use crate::oxql::ast::literal::Literal;
use crate::oxql::ast::logical_op::LogicalOp;
use crate::oxql::ast::table_ops::limit::Limit;
use crate::oxql::ast::table_ops::limit::LimitKind;
use crate::oxql::Error;
use crate::shells::special_idents;
use chrono::DateTime;
use chrono::Utc;
use oximeter::FieldType;
use oximeter::FieldValue;
use oxql_types::point::DataType;
use oxql_types::point::MetricType;
use oxql_types::point::Points;
use oxql_types::point::ValueArray;
use oxql_types::Table;
use oxql_types::Timeseries;
use regex::Regex;
use std::collections::BTreeSet;
use std::fmt;

/// An AST node for the `filter` table operation.
///
/// This can be a simple operation like `foo == "bar"` or a more complex
/// expression, such as: `filter hostname == "foo" || (hostname == "bar"
/// && id == "baz")`.
#[derive(Clone, Debug, PartialEq)]
pub struct Filter {
    /// True if the whole expression is negated.
    pub negated: bool,
    /// The contained filtering expression, which may contain many expressions
    /// joined by logical operators.
    pub expr: FilterExpr,
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", if self.negated { "!" } else { "" }, self.expr,)
    }
}

impl core::str::FromStr for Filter {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::oxql::ast::grammar::query_parser::filter_expr(s)
            .map_err(|e| anyhow::anyhow!("invalid filter expression: {e}"))
    }
}

// A crude limit on expression complexity, governing how many times we
// iteratively apply a DNF simplification before bailing out.
const EXPR_COMPLEXITY_ITERATIVE_LIMIT: usize = 32;

// A crude limit on expression complexity, governing how many times we
// recursively apply a DNF simplification before bailing out.
const EXPR_COMPLEXITY_RECURSIVE_LIMIT: usize = 32;

impl Filter {
    /// Return the negation of this filter.
    pub fn negate(&self) -> Filter {
        Self { negated: !self.negated, ..self.clone() }
    }

    /// Split the filter at top-level disjunctions.
    ///
    /// This is likely only useful after simplifying to DNF with
    /// `simplify_to_dnf()`.
    pub fn flatten_disjunctions(&self) -> Vec<Self> {
        let mut out = vec![];
        self.flatten_disjunctions_inner(&mut out);
        out
    }

    fn flatten_disjunctions_inner(&self, dis: &mut Vec<Self>) {
        // Recursion is only needed if this is an OR expression. In that case,
        // we split the left and push it, and then recurse on the right.
        //
        // Note that we don't need left-recursion because the parser is strictly
        // non-left-recursive.
        if let FilterExpr::Compound(CompoundFilter {
            left,
            op: LogicalOp::Or,
            right,
        }) = &self.expr
        {
            dis.push(*left.clone());
            right.flatten_disjunctions_inner(dis);
        } else {
            // It's not an OR expression, or it is a simple filter expression.
            // In either case, just push it directly, withouth recursing.
            dis.push(self.clone());
        }
    }

    /// Simplfy a filter expression to disjunctive normal form (DNF).
    ///
    /// Disjunctive normal form is one of a few canonical ways of writing a
    /// boolean expression. It simplifies to a disjunction of conjunctions,
    /// i.e., only has terms like `(a && b) || (c && d) || ...`.
    ///
    /// This method exists for the purposes of creating _independent_ pieces of
    /// a filtering expression, each of which can be used to generate a new SQL
    /// query run against ClickHouse. This is critical to support complicated
    /// OxQL queries. Consider:
    ///
    /// ```ignore
    /// get some_timeseries
    ///     | filter (foo == "bar") || (timestamp > @now() - 1m && foo == "baz")
    /// ```
    ///
    /// This requires fetching part of one timeseries, and all of another. One
    /// cannot run this as a conjunction on the fields and then a query on the
    /// measurements. It must be run in such a way to get the sets of keys
    /// consistent with each term in the disjunction _independently_, so that
    /// one can apply the timestamp filter to only the correct one.
    ///
    /// We use this method to generate the DNF, a form with only disjunctions of
    /// conjunctions. That is, it's not possible to further distribute
    /// conjunctions over disjunctions.
    ///
    /// Each disjunction is then a separate query against the fields table, where
    /// we keep track of the keys in each. Each set of predicates and consistent
    /// keys is then used later to fetch the measurements.
    ///
    /// # Notes
    ///
    /// There is a huge academic literature on this topic, part of the study of
    /// formal languages and other areas theoretical computer science. These
    /// references are mostly pretty dense and formal, though a few are really
    /// useful. This [paper](https://www.researchgate.net/publication/220154187_A_Survey_of_Strategies_in_Program_Transformation_Systems)
    /// is a good and accessible survey to the idea of translation systems --
    /// it's mostly focused on programming languages and compilers, but Figures
    /// 7-9 in particular are about DNF.
    ///
    /// As usual, the Wikipedia page is a reasonable overview as well,
    /// [here](https://en.wikipedia.org/wiki/Disjunctive_normal_form). We're
    /// using the "syntactic" DNF conversion algorithm, essentially. This
    /// involves a recursive application of
    /// [de Morgan's rules](https://en.wikipedia.org/wiki/De_Morgan%27s_laws),
    /// [involution / double-negation](https://en.wikipedia.org/wiki/Involution_(mathematics)),
    /// distributivity of [Boolean operators](https://en.wikipedia.org/wiki/Boolean_algebra#Monotone_laws),
    /// etc.
    pub fn simplify_to_dnf(&self) -> Result<Self, Error> {
        self.simplify_to_dnf_impl(0)
    }

    fn simplify_to_dnf_impl(&self, level: usize) -> Result<Self, Error> {
        anyhow::ensure!(
            level < EXPR_COMPLEXITY_RECURSIVE_LIMIT,
            "Maximum recursion level exceeded trying to simplify \
            logical expression to disjunctive normal form"
        );
        let mut out = self.simplify_to_dnf_inner(level)?;
        if &out == self {
            return Ok(out);
        }
        // Continually apply simplifications as long as able.
        //
        // This makes me really nervous, so I'm adding an escape hatch that we
        // only allow a few iterations. If we've not simplified within that,
        // we'll just declare the expression too complicated to handle.
        for _ in 0..EXPR_COMPLEXITY_ITERATIVE_LIMIT {
            let out_ = out.simplify_to_dnf_inner(level)?;
            if out_ == out {
                return Ok(out_);
            }
            out = out_;
        }
        anyhow::bail!("Logical expression is too complicated to simplify")
    }

    fn simplify_to_dnf_inner(&self, level: usize) -> Result<Self, Error> {
        let new = self.expr.simplify_to_dnf(level)?;

        // This matches the rule:
        //
        // !!x -> x
        if self.negated && new.negated && new.is_simple() {
            return Ok(new.negate());
        }

        // These two blocks match de Morgan's rules, which distribute a negation
        // down and swap the logical operator.
        if self.negated {
            // This matches one of de Morgan's rules:
            //
            // !(x && y) -> !x || !y
            if let FilterExpr::Compound(CompoundFilter {
                left: x,
                op: LogicalOp::And,
                right: y,
            }) = &new.expr
            {
                let expr = FilterExpr::Compound(CompoundFilter {
                    left: Box::new(x.negate()),
                    op: LogicalOp::Or,
                    right: Box::new(y.negate()),
                });
                return Ok(Filter { negated: false, expr });
            }

            // This matches the other of de Morgan's rules:
            //
            // !(x || y) -> !x && !y
            if let FilterExpr::Compound(CompoundFilter {
                left: x,
                op: LogicalOp::And,
                right: y,
            }) = &new.expr
            {
                let expr = FilterExpr::Compound(CompoundFilter {
                    left: Box::new(x.negate()),
                    op: LogicalOp::Or,
                    right: Box::new(y.negate()),
                });
                return Ok(Filter { negated: false, expr });
            }
        }

        // Nothing else to do, just return ourself, though we do need to make
        // sure we copy the negation from self as well.
        Ok(Self { negated: self.negated, ..new })
    }

    // Merge this filter with another one, using the provided operator.
    pub(crate) fn merge(&self, other: &Filter, op: LogicalOp) -> Self {
        Self {
            negated: false,
            expr: FilterExpr::Compound(CompoundFilter {
                left: Box::new(self.clone()),
                op,
                right: Box::new(other.clone()),
            }),
        }
    }

    // Apply the filter to the provided field.
    //
    // This returns `Ok(None)` if the filter doesn't apply. It returns `Ok(x)`
    // if the filter does apply, where `x` is the logical application of the
    // filter to the field. `true` means "keep this field", which is analogous
    // to the `Iterator::filter()` method's signature.
    //
    // If the filter does apply, but is incompatible or incomparable, return an
    // error.
    fn filter_field(
        &self,
        name: &str,
        value: &FieldValue,
    ) -> Result<Option<bool>, Error> {
        let result = match &self.expr {
            FilterExpr::Simple(inner) => inner.filter_field(name, value),
            FilterExpr::Compound(inner) => inner.filter_field(name, value),
        };
        result.map(|maybe_keep| maybe_keep.map(|keep| self.negated ^ keep))
    }

    // Apply the filter to the provided points.
    fn filter_points(&self, points: &Points) -> Result<Points, Error> {
        let to_keep = self.filter_points_inner(points)?;
        points.filter(to_keep)
    }

    // Inner implementation of filtering points.
    //
    // Returns an array of bools, where true indicates the point should be kept.
    fn filter_points_inner(&self, points: &Points) -> Result<Vec<bool>, Error> {
        match &self.expr {
            FilterExpr::Simple(inner) => {
                inner.filter_points(self.negated, points)
            }
            FilterExpr::Compound(inner) => {
                inner.filter_points(self.negated, points)
            }
        }
    }

    // Apply the filtering table operation.
    pub(crate) fn apply(&self, tables: &[Table]) -> Result<Vec<Table>, Error> {
        anyhow::ensure!(
            !tables.is_empty(),
            "Filtering operations require at least one table",
        );
        let mut output_tables = Vec::with_capacity(tables.len());
        // Ensure that all the identifiers in this filter apply to the
        // input timeseries. We can do this once at the beginning, because all
        // the timeseries in a table have the same set of fields.
        let Some(first_timeseries) = tables[0].iter().next() else {
            // You give nothing, you get nothing.
            return Ok(tables.to_vec());
        };

        // There are extra, implied names that depend on the data type of the
        // timeseries itself, check those as well.
        let extras = implicit_field_names(first_timeseries);
        let ident_names = self.ident_names();
        let not_valid = ident_names
            .iter()
            .filter(|&&name| {
                !(first_timeseries.fields.contains_key(name)
                    || extras.contains(name))
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            not_valid.is_empty(),
            "The filter expression contains identifiers that are not \
            valid for its input timeseries. Invalid identifiers: {:?}, \
            timeseries fields: {:?}",
            not_valid,
            first_timeseries
                .fields
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                .union(&extras)
                .collect::<BTreeSet<_>>(),
        );

        // Filter each input table in succession.
        for table in tables.iter() {
            let mut timeseries = Vec::with_capacity(table.len());
            'timeseries: for input in table.iter() {
                // If the filter restricts any of the fields, remove this
                // timeseries altogether.
                for (name, value) in input.fields.iter() {
                    if let Some(false) = self.filter_field(name, value)? {
                        continue 'timeseries;
                    }
                }

                // Apply the filter to the data points as well.
                let points = self.filter_points(&input.points)?;

                if let Some(new_timeseries) = input.copy_with_points(points) {
                    timeseries.push(new_timeseries);
                } else {
                    // None means that the filter removed all data points in
                    // the timeseries. In that case, we remove the timeseries
                    // altogether.
                }
            }
            output_tables.push(Table::from_timeseries(
                table.name(),
                timeseries.into_iter(),
            )?);
        }
        Ok(output_tables)
    }

    // Return the last referenced timestamp by this filter, if any.
    //
    // This is the maximum timestamp, before which any filtered point must lie.
    // This is used to determine the query end time.
    pub(crate) fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        match &self.expr {
            FilterExpr::Simple(inner) => inner.last_timestamp(),
            FilterExpr::Compound(inner) => inner.last_timestamp(),
        }
    }

    // Return the name of all identifiers listed in this filter.
    fn ident_names(&self) -> BTreeSet<&str> {
        match &self.expr {
            FilterExpr::Simple(inner) => {
                let mut out = BTreeSet::new();
                out.insert(inner.ident.as_str());
                out
            }
            FilterExpr::Compound(inner) => {
                let mut all = inner.left.ident_names();
                all.extend(inner.right.ident_names());
                all
            }
        }
    }

    fn is_xor(&self) -> bool {
        self.is_op(LogicalOp::Xor)
    }

    fn is_op(&self, expected_op: LogicalOp) -> bool {
        let FilterExpr::Compound(CompoundFilter { op, .. }) = &self.expr else {
            return false;
        };
        op == &expected_op
    }

    // If this is an XOR, rewrite it to a disjunction of conjunctions.
    //
    // If it is not, return a clone of self.
    fn rewrite_xor_to_disjunction(&self) -> Self {
        let self_ = self.clone();
        if !self.is_xor() {
            return self_;
        }
        let Filter {
            negated,
            expr: FilterExpr::Compound(CompoundFilter { left, right, .. }),
        } = self_
        else {
            unreachable!();
        };
        let left_ = CompoundFilter {
            left: left.clone(),
            op: LogicalOp::And,
            right: Box::new(right.negate()),
        };
        let right_ = CompoundFilter {
            left: Box::new(left.negate()),
            op: LogicalOp::And,
            right,
        };
        let expr = CompoundFilter {
            left: Box::new(left_.to_filter()),
            op: LogicalOp::Or,
            right: Box::new(right_.to_filter()),
        };
        Filter { negated, expr: FilterExpr::Compound(expr) }
    }

    fn is_simple(&self) -> bool {
        matches!(self.expr, FilterExpr::Simple(_))
    }

    /// Return true if this filtering expression can be reordered around a
    /// `limit` table operation.
    ///
    /// We attempt to push filtering expressions down to the database as much as
    /// possible. This involves moving filters "through" an OxQL pipeline, so
    /// that we can run them as early as possible, before other operations like
    /// a `group_by`.
    ///
    /// In some cases, but not all, filters interact with limiting table
    /// operations, which take the first or last k points from a timeseries.
    /// Specifically, we can move a filter around a limit if:
    ///
    /// - The filter does not refer to timestamps at all
    /// - The filter's comparison against timestamps restricts them in the same
    /// "direction" as the limit operation. A timestamp filter which takes later
    /// values, e.g., `timestamp > t0` can be moved around a `last k` operation;
    /// a filter which takes earlier values, e.g., `timestamp < t0` can be moved
    /// around a `first k` operation.
    ///
    /// All other situations return false. Consider a query with `filter
    /// timestamp < t0` and `last k`. Those return different results depending
    /// on which is run first:
    ///
    /// - Running the filter then the limit returns the last values before `t0`,
    /// so the "end" of that chunk of time.
    /// - Running the limit then filter returns the values in the last `k` of
    /// the entire timeseries where the timestamp is before `t0`. That set can
    /// be empty, if all the last `k` samples have a timestamp _after_ `t0`,
    /// whereas the reverse is may well _not_ be empty.
    pub(crate) fn can_reorder_around(&self, limit: &Limit) -> bool {
        match &self.expr {
            FilterExpr::Simple(SimpleFilter { ident, cmp, .. }) => {
                if ident.as_str() != special_idents::TIMESTAMP {
                    return true;
                }
                let is_compatible = match limit.kind {
                    LimitKind::First => {
                        matches!(cmp, Comparison::Lt | Comparison::Le)
                    }
                    LimitKind::Last => {
                        matches!(cmp, Comparison::Gt | Comparison::Ge)
                    }
                };
                self.negated ^ is_compatible
            }
            FilterExpr::Compound(CompoundFilter { left, right, .. }) => {
                let left = left.can_reorder_around(limit);
                let right = right.can_reorder_around(limit);
                self.negated ^ (left && right)
            }
        }
    }
}

/// Return the names of the implicit fields / columns that a filter can apply
/// to, based on the metric types of the contained data points.
fn implicit_field_names(
    first_timeseries: &Timeseries,
) -> BTreeSet<&'static str> {
    let mut out = BTreeSet::new();

    // Everything has a timestamp!
    out.insert(special_idents::TIMESTAMP);
    let type_info = first_timeseries
        .points
        .metric_types()
        .zip(first_timeseries.points.data_types());
    for (metric_type, data_type) in type_info {
        match (metric_type, data_type) {
            // Scalar gauges.
            (
                MetricType::Gauge,
                DataType::Integer
                | DataType::Boolean
                | DataType::Double
                | DataType::String,
            ) => {
                out.insert(special_idents::DATUM);
            }
            // Histogram gauges.
            (
                MetricType::Gauge,
                DataType::IntegerDistribution | DataType::DoubleDistribution,
            ) => {
                special_idents::DISTRIBUTION_IDENTS.iter().for_each(|ident| {
                    out.insert(ident);
                });
            }
            // Scalars, either delta or cumulatives.
            (
                MetricType::Delta | MetricType::Cumulative,
                DataType::Integer | DataType::Double,
            ) => {
                out.insert(special_idents::DATUM);
                out.insert(special_idents::START_TIME);
            }
            // Histograms, either delta or cumulative.
            (
                MetricType::Delta | MetricType::Cumulative,
                DataType::IntegerDistribution | DataType::DoubleDistribution,
            ) => {
                special_idents::DISTRIBUTION_IDENTS.iter().for_each(|ident| {
                    out.insert(ident);
                });
                out.insert(special_idents::START_TIME);
            }
            // Impossible combinations
            (
                MetricType::Delta | MetricType::Cumulative,
                DataType::Boolean | DataType::String,
            ) => unreachable!(),
        }
    }
    out
}

/// A filtering expression, used in the `filter` table operation.
#[derive(Clone, Debug, PartialEq)]
pub enum FilterExpr {
    /// A single logical expression, e.g., `foo == "bar"`.
    Simple(SimpleFilter),
    /// Two logical expressions, e.g., `foo == "bar" || yes == false`
    Compound(CompoundFilter),
}

impl FilterExpr {
    fn to_filter(&self) -> Filter {
        Filter { negated: false, expr: self.clone() }
    }

    fn simplify_to_dnf(&self, level: usize) -> Result<Filter, Error> {
        match self {
            FilterExpr::Simple(_) => Ok(self.to_filter()),
            FilterExpr::Compound(CompoundFilter { left, op, right }) => {
                // Apply recursively first.
                let left = left.simplify_to_dnf_impl(level + 1)?;
                let right = right.simplify_to_dnf_impl(level + 1)?;

                // This matches the rule:
                //
                // (x || y) && z -> (x && z) || (y && z)
                if let (
                    FilterExpr::Compound(CompoundFilter {
                        left: x,
                        op: LogicalOp::Or,
                        right: y,
                    }),
                    LogicalOp::And,
                    FilterExpr::Simple(z),
                ) = (&left.expr, op, &right.expr)
                {
                    let left_ = Filter {
                        negated: false,
                        expr: FilterExpr::Compound(CompoundFilter {
                            left: x.clone(),
                            op: LogicalOp::And,
                            right: Box::new(z.to_filter()),
                        }),
                    };
                    let right_ = Filter {
                        negated: false,
                        expr: FilterExpr::Compound(CompoundFilter {
                            left: y.clone(),
                            op: LogicalOp::And,
                            right: Box::new(z.to_filter()),
                        }),
                    };
                    return Ok(Filter {
                        negated: false,
                        expr: FilterExpr::Compound(CompoundFilter {
                            left: Box::new(left_),
                            op: LogicalOp::Or,
                            right: Box::new(right_),
                        }),
                    });
                }

                // This matches the rule:
                //
                // z && (x || y) -> (z && x) || (z && y)
                if let (
                    FilterExpr::Simple(z),
                    LogicalOp::And,
                    FilterExpr::Compound(CompoundFilter {
                        left: x,
                        op: LogicalOp::Or,
                        right: y,
                    }),
                ) = (&left.expr, op, &right.expr)
                {
                    let left_ = Filter {
                        negated: false,
                        expr: FilterExpr::Compound(CompoundFilter {
                            left: Box::new(z.to_filter()),
                            op: LogicalOp::And,
                            right: x.clone(),
                        }),
                    };
                    let right_ = Filter {
                        negated: false,
                        expr: FilterExpr::Compound(CompoundFilter {
                            left: Box::new(z.to_filter()),
                            op: LogicalOp::And,
                            right: y.clone(),
                        }),
                    };
                    return Ok(Filter {
                        negated: false,
                        expr: FilterExpr::Compound(CompoundFilter {
                            left: Box::new(left_),
                            op: LogicalOp::Or,
                            right: Box::new(right_),
                        }),
                    });
                }

                // Lastly, simplify an XOR to its logical equivalent, which is
                // in DNF.
                let out = Filter {
                    negated: false,
                    expr: FilterExpr::Compound(CompoundFilter {
                        left: Box::new(left),
                        op: *op,
                        right: Box::new(right),
                    }),
                };
                Ok(out.rewrite_xor_to_disjunction())
            }
        }
    }
}

impl fmt::Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterExpr::Simple(inner) => write!(f, "{inner}"),
            FilterExpr::Compound(inner) => write!(f, "{inner}"),
        }
    }
}

/// Two filter expressions joined by a logical operator.
#[derive(Clone, Debug, PartialEq)]
pub struct CompoundFilter {
    /// The left subexpression.
    pub left: Box<Filter>,
    /// The logical operator joining the two expressions.
    pub op: LogicalOp,
    /// The right subexpression.
    pub right: Box<Filter>,
}

impl fmt::Display for CompoundFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right,)
    }
}

impl CompoundFilter {
    fn to_filter(&self) -> Filter {
        Filter { negated: false, expr: FilterExpr::Compound(self.clone()) }
    }

    // Apply the filter to the provided field.
    fn filter_field(
        &self,
        name: &str,
        value: &FieldValue,
    ) -> Result<Option<bool>, Error> {
        let left = self.left.filter_field(name, value)?;
        let right = self.right.filter_field(name, value)?;
        match (left, right) {
            (None, None) => Ok(None),
            (Some(x), None) | (None, Some(x)) => Ok(Some(x)),
            (Some(left), Some(right)) => match self.op {
                LogicalOp::And => Ok(Some(left && right)),
                LogicalOp::Or => Ok(Some(left || right)),
                LogicalOp::Xor => Ok(Some(left ^ right)),
            },
        }
    }

    // Apply the filter to the provided points.
    fn filter_points(
        &self,
        negated: bool,
        points: &Points,
    ) -> Result<Vec<bool>, Error> {
        let mut left = self.left.filter_points_inner(points)?;
        let right = self.right.filter_points_inner(points)?;
        match self.op {
            LogicalOp::And => {
                for i in 0..left.len() {
                    left[i] = negated ^ (left[i] & right[i]);
                }
            }
            LogicalOp::Or => {
                for i in 0..left.len() {
                    left[i] = negated ^ (left[i] | right[i]);
                }
            }
            LogicalOp::Xor => {
                for i in 0..left.len() {
                    left[i] = negated ^ (left[i] ^ right[i]);
                }
            }
        }
        Ok(left)
    }

    fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        let left = self.left.last_timestamp();
        let right = self.right.last_timestamp();
        match (left, right) {
            (None, None) => None,
            (Some(single), None) | (None, Some(single)) => Some(single),
            (Some(left), Some(right)) => Some(left.max(right)),
        }
    }
}

/// A simple filter expression, comparing an identifier to a value.
#[derive(Clone, Debug, PartialEq)]
pub struct SimpleFilter {
    /// The identifier being compared.
    pub ident: Ident,
    /// The comparison operator.
    pub cmp: Comparison,
    /// The value to compare the identifier against.
    pub value: Literal,
}

impl fmt::Display for SimpleFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.ident, self.cmp, self.value,)
    }
}

impl SimpleFilter {
    fn to_filter(&self) -> Filter {
        Filter { negated: false, expr: FilterExpr::Simple(self.clone()) }
    }

    // Apply this filter to the provided field.
    //
    // If the field name does not match the identifier in `self`, return
    // `Ok(None)`, since this filter does not apply to the provided field.
    //
    // If the name matches and the type of `self` is compatible, return `Ok(x)`
    // where `x` is the logical application of the filter to the field.
    //
    // If the field matches the name, but the type is not compatible, return an
    // error.
    fn filter_field(
        &self,
        name: &str,
        value: &FieldValue,
    ) -> Result<Option<bool>, Error> {
        // If the name does not match, this filter does not apply, and so we do not
        // filter the field.
        if self.ident.as_str() != name {
            return Ok(None);
        }
        self.value.compare_field(value, self.cmp)
    }

    pub(crate) fn value_type_is_compatible_with_field(
        &self,
        field_type: FieldType,
    ) -> bool {
        self.value.is_compatible_with_field(field_type)
    }

    /// Return the expression as a string that can be applied safely in the
    /// database.
    pub(crate) fn as_db_safe_string(&self) -> String {
        let expr = self.value.as_db_safe_string();
        let fn_name = self.cmp.as_db_function_name();
        format!("{}({}, {})", fn_name, self.ident, expr)
    }

    // Returns an array of bools, where true indicates the point should be kept.
    fn filter_points(
        &self,
        negated: bool,
        points: &Points,
    ) -> Result<Vec<bool>, Error> {
        let ident = self.ident.as_str();
        if ident == "timestamp" {
            self.filter_points_by_timestamp(negated, points.timestamps())
        } else if ident == "datum" {
            anyhow::ensure!(
                points.dimensionality() == 1,
                "Filtering multidimensional values by datum is not yet supported"
            );
            self.filter_points_by_datum(negated, points.values(0).unwrap())
        } else {
            Ok(vec![!negated; points.len()])
        }
    }

    fn filter_points_by_timestamp(
        &self,
        negated: bool,
        timestamps: &[DateTime<Utc>],
    ) -> Result<Vec<bool>, Error> {
        let Literal::Timestamp(timestamp) = &self.value else {
            anyhow::bail!(
                "Cannot compare non-timestamp filter against a timestamp"
            );
        };
        match self.cmp {
            Comparison::Eq => Ok(timestamps
                .iter()
                .map(|t| negated ^ (t == timestamp))
                .collect()),
            Comparison::Ne => Ok(timestamps
                .iter()
                .map(|t| negated ^ (t != timestamp))
                .collect()),
            Comparison::Gt => Ok(timestamps
                .iter()
                .map(|t| negated ^ (t > timestamp))
                .collect()),
            Comparison::Ge => Ok(timestamps
                .iter()
                .map(|t| negated ^ (t >= timestamp))
                .collect()),
            Comparison::Lt => Ok(timestamps
                .iter()
                .map(|t| negated ^ (t < timestamp))
                .collect()),
            Comparison::Le => Ok(timestamps
                .iter()
                .map(|t| negated ^ (t <= timestamp))
                .collect()),
            Comparison::Like => unreachable!(),
        }
    }

    fn filter_points_by_datum(
        &self,
        negated: bool,
        values: &ValueArray,
    ) -> Result<Vec<bool>, Error> {
        match (&self.value, values) {
            (Literal::Integer(int), ValueArray::Integer(ints)) => {
                match self.cmp {
                    Comparison::Eq => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| negated ^ (i128::from(i) == *int))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ne => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| negated ^ (i128::from(i) != *int))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Gt => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| negated ^ (i128::from(i) > *int))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ge => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| negated ^ (i128::from(i) >= *int))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Lt => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| negated ^ (i128::from(i) < *int))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Le => Ok(ints
                        .iter()
                        .map(|maybe_int| {
                            maybe_int
                                .map(|i| negated ^ (i128::from(i) <= *int))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Like => unreachable!(),
                }
            }
            (Literal::Double(double), ValueArray::Double(doubles)) => {
                match self.cmp {
                    Comparison::Eq => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double
                                .map(|d| negated ^ (d == *double))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ne => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double
                                .map(|d| negated ^ (d != *double))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Gt => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double
                                .map(|d| negated ^ (d > *double))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ge => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double
                                .map(|d| negated ^ (d >= *double))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Lt => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double
                                .map(|d| negated ^ (d < *double))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Le => Ok(doubles
                        .iter()
                        .map(|maybe_double| {
                            maybe_double
                                .map(|d| negated ^ (d <= *double))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Like => unreachable!(),
                }
            }
            (Literal::String(string), ValueArray::String(strings)) => {
                let string = string.as_str();
                match self.cmp {
                    Comparison::Eq => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| negated ^ (s == string))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ne => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| negated ^ (s != string))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Gt => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| negated ^ (s > string))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ge => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| negated ^ (s >= string))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Lt => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| negated ^ (s < string))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Le => Ok(strings
                        .iter()
                        .map(|maybe_string| {
                            maybe_string
                                .as_deref()
                                .map(|s| negated ^ (s <= string))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Like => {
                        let re = Regex::new(string)?;
                        Ok(strings
                            .iter()
                            .map(|maybe_string| {
                                maybe_string
                                    .as_deref()
                                    .map(|s| negated ^ re.is_match(s))
                                    .unwrap_or(false)
                            })
                            .collect())
                    }
                }
            }
            (Literal::Boolean(boolean), ValueArray::Boolean(booleans)) => {
                match self.cmp {
                    Comparison::Eq => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| negated ^ (b == *boolean))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ne => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| negated ^ (b != *boolean))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Gt => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| negated ^ (b & !(*boolean)))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Ge => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| negated ^ (b >= *boolean))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Lt => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| negated ^ (!b & *boolean))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Le => Ok(booleans
                        .iter()
                        .map(|maybe_boolean| {
                            maybe_boolean
                                .map(|b| negated ^ (b <= *boolean))
                                .unwrap_or(false)
                        })
                        .collect()),
                    Comparison::Like => unreachable!(),
                }
            }
            (_, _) => {
                let lit_type = match &self.value {
                    Literal::Uuid(_) => "UUID",
                    Literal::Duration(_) => "duration",
                    Literal::Timestamp(_) => "timestamp",
                    Literal::IpAddr(_) => "IP address",
                    Literal::Integer(_) => "integer",
                    Literal::Double(_) => "double",
                    Literal::String(_) => "string",
                    Literal::Boolean(_) => "boolean",
                };
                anyhow::bail!(
                    "Cannot compare {} literal against values of type {}",
                    lit_type,
                    values.data_type(),
                )
            }
        }
    }

    fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        if self.ident.as_str() == "timestamp"
            && matches!(
                self.cmp,
                Comparison::Lt | Comparison::Le | Comparison::Eq
            )
        {
            let Literal::Timestamp(t) = self.value else {
                return None;
            };
            Some(t)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::oxql::ast::grammar::query_parser;
    use crate::oxql::ast::logical_op::LogicalOp;
    use chrono::Utc;
    use oximeter::FieldValue;
    use oxql_types::point::DataType;
    use oxql_types::point::MetricType;
    use oxql_types::point::Points;
    use oxql_types::point::ValueArray;
    use oxql_types::point::Values;
    use oxql_types::Table;
    use oxql_types::Timeseries;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn test_atom_filter_double_points() {
        let start_times = None;
        let timestamps =
            vec![Utc::now(), Utc::now() + Duration::from_secs(1000)];
        let values = vec![Values {
            values: ValueArray::Double(vec![Some(0.0), Some(2.0)]),
            metric_type: MetricType::Gauge,
        }];
        let points = Points::new(start_times, timestamps, values);

        // This filter should remove the first point based on its timestamp.
        let t = Utc::now() + Duration::from_secs(10);
        let q =
            format!("filter timestamp > @{}", t.format("%Y-%m-%dT%H:%M:%S"));
        let filter = query_parser::filter(q.as_str()).unwrap();
        let out = filter.filter_points(&points).unwrap();
        assert!(out.len() == 1);
        assert_eq!(
            out.values(0).unwrap().as_double().unwrap()[0],
            points.values(0).unwrap().as_double().unwrap()[1],
        );

        // And this one the second point based on the datum
        let filter = query_parser::filter("filter datum < 1.0").unwrap();
        let out = filter.filter_points(&points).unwrap();
        assert!(out.len() == 1);
        assert_eq!(
            out.values(0).unwrap().as_double().unwrap()[0],
            points.values(0).unwrap().as_double().unwrap()[0],
        );
    }

    #[test]
    fn test_atom_filter_points_wrong_type() {
        let start_times = None;
        let timestamps =
            vec![Utc::now(), Utc::now() + Duration::from_secs(1000)];
        let values = vec![Values {
            values: ValueArray::Double(vec![Some(0.0), Some(2.0)]),
            metric_type: MetricType::Gauge,
        }];
        let points = Points::new(start_times, timestamps, values);

        let filter =
            query_parser::filter("filter datum < \"something\"").unwrap();
        assert!(filter.filter_points(&points).is_err());
    }

    #[test]
    fn test_all_ident_names() {
        let f = query_parser::filter("filter timestamp > @now() && datum < 1")
            .unwrap();
        assert_eq!(
            f.ident_names(),
            ["datum", "timestamp"].into_iter().collect()
        );

        let f = query_parser::filter(
            "filter timestamp > @now() - 1m &&  timestamp < @now()",
        )
        .unwrap();
        let idents = f.ident_names();
        assert_eq!(idents.len(), 1);
        assert_eq!(idents.iter().next().unwrap(), &"timestamp");
    }

    #[test]
    #[allow(clippy::impossible_comparisons)]
    fn test_filter_field_logic() {
        for op in [LogicalOp::And, LogicalOp::Or, LogicalOp::Xor] {
            let s = format!("filter (x > 10) {op} (x < 0)");
            let filter = query_parser::filter(&s).unwrap();
            let cases = &[11, 10, 5, 0, -1];
            for &val in cases.iter() {
                let pass = match op {
                    LogicalOp::And => (val > 10) && (val < 0),
                    LogicalOp::Or => (val > 10) || (val < 0),
                    LogicalOp::Xor => (val > 10) ^ (val < 0),
                };
                let result = filter
                    .filter_field("x", &FieldValue::I32(val))
                    .expect("Filter should be considered comparable")
                    .expect("Filter should apply to field of the same name");
                assert_eq!(
                    result,
                    pass,
                    "Filter '{}' should {} the value {}",
                    filter,
                    if pass { "pass" } else { "not pass" },
                    val,
                );
            }

            // This names a different field, so should not apply.
            assert_eq!(
                filter
                    .filter_field("y", &FieldValue::I32(11))
                    .expect("Filter should be considered comparable"),
                None,
                "Filter should not apply, since it names a different field"
            );

            // These values should not be comparable at all, so we'll return an
            // error.
            let incomparable = &[
                FieldValue::String("foo".into()),
                FieldValue::Uuid(Uuid::new_v4()),
                FieldValue::IpAddr("127.0.0.1".parse().unwrap()),
                FieldValue::Bool(false),
            ];
            for na in incomparable.iter() {
                filter
                    .filter_field("x", na)
                    .expect_err("These should not be comparable at all");
            }
        }
    }

    #[test]
    fn test_simplify_to_dnf() {
        let cases = &[
            // Simple cases that should not be changed
            ("a == 0", "a == 0"),
            ("!(a == 0)", "!(a == 0)"),
            ("a == 0 || b == 1", "a == 0 || b == 1"),
            ("a == 0 && b == 1", "a == 0 && b == 1"),

            // Rewrite of XOR
            ("a == 0 ^ b == 1", "(a == 0 && !(b == 1)) || (!(a == 0) && (b == 1))"),

            // Simple applications of distribution rules.
            //
            // Distribute conjunction over disjunction.
            ("a == 0 && (b == 1 || c == 2)", "(a == 0 && b == 1) || (a == 0 && c == 2)"),
            ("a == 0 && (b == 1 || c == 2 || d == 3)", "(a == 0 && b == 1) || (a == 0 && c == 2) || (a == 0 && d == 3)"),
            ("a == 0 && (b == 1 || c == 2 || d == 3 || e == 4)", "(a == 0 && b == 1) || (a == 0 && c == 2) || (a == 0 && d == 3) || (a == 0 && e == 4)"),
        ];
        for (input, expected) in cases.iter() {
            let parsed_input = query_parser::filter_expr(input).unwrap();
            let simplified = parsed_input.simplify_to_dnf().unwrap();
            let parsed_expected = query_parser::filter_expr(expected).unwrap();
            assert_eq!(
                simplified,
                parsed_expected,
                "\ninput expression: {}\nparsed to: {}\nsimplifed to: {}\nexpected: {}\n",
                input,
                parsed_input,
                simplified,
                expected,
            );
        }
    }

    #[test]
    fn test_dnf_conversion_fails_on_extremely_long_expressions() {
        let atom = "a == 0";
        let or_chain = std::iter::repeat(atom)
            .take(super::EXPR_COMPLEXITY_ITERATIVE_LIMIT + 1)
            .collect::<Vec<_>>()
            .join(" || ");
        let expr = format!("{atom} && ({or_chain})");
        let parsed = query_parser::filter_expr(&expr).unwrap();
        assert!(
            parsed.simplify_to_dnf().is_err(),
            "Should fail for extremely long logical expressions"
        );
    }

    #[test]
    fn test_dnf_conversion_fails_on_extremely_deep_expressions() {
        let atom = "a == 0";
        let mut expr = atom.to_string();
        for _ in 0..super::EXPR_COMPLEXITY_RECURSIVE_LIMIT + 1 {
            expr = format!("{atom} && ({expr})");
        }
        let parsed = query_parser::filter_expr(&expr).unwrap();
        assert!(
            parsed.simplify_to_dnf().is_err(),
            "Should fail for extremely deep logical expressions"
        );
    }

    #[test]
    fn test_filter_empty_timeseries() {
        let ts = Timeseries::new(
            std::iter::once((String::from("foo"), FieldValue::U8(0))),
            DataType::Double,
            MetricType::Gauge,
        )
        .unwrap();
        let table = Table::from_timeseries("foo", std::iter::once(ts)).unwrap();
        let filt = query_parser::filter_expr("timestamp > @now()").unwrap();
        assert!(
            filt.apply(&[table]).is_ok(),
            "It's not an error to filter an empty table"
        );
    }

    #[test]
    fn test_error_message_with_invalid_field_names() {
        let ts = Timeseries::new(
            std::iter::once((String::from("foo"), FieldValue::U8(0))),
            DataType::Double,
            MetricType::Gauge,
        )
        .unwrap();
        let table = Table::from_timeseries("foo", std::iter::once(ts)).unwrap();
        let filt = query_parser::filter_expr("bar == 0").unwrap();
        let msg = filt
            .apply(&[table])
            .expect_err(
                "Applying a filter with a name that doesn't appear \
                in the timeseries should be an error",
            )
            .to_string();
        println!("{msg}");
        assert!(
            msg.contains(r#"timeseries fields: {"datum", "foo", "timestamp"}"#)
        );
    }
}
