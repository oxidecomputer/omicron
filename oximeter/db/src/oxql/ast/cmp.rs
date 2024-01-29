// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An AST node describing comparison operators

// Copyright 2024 Oxide Computer Company

use std::fmt;

/// Comparison operators.
// TODO-completeness: Operators for other types, like IP containment ('<<').
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Comparison {
    /// Equality comparison.
    Eq,
    /// Inequality comparison.
    Ne,
    /// Greater-than comparison
    Gt,
    /// Greater-than or equals comparison
    Ge,
    /// Lesser-than comparison
    Lt,
    /// Lesser-than or equals comparison
    Le,
    /// Regular expression pattern matching.
    Like,
}

impl Comparison {
    // Return the _function name_ of the comparison that is safe for use in
    // ClickHouse.
    //
    // Note that we're always using the functional form for these comparisons,
    // even when they have obvious operators. E.g., we return `"equals"` for the
    // `Comparison::Eq` rather than `"=="`.
    //
    // This is to normalize the different comparisons we support, which do not
    // all have operator formats. `Comparison::Like` is the best example, but we
    // may also want to support things like IP address containment. While DBs
    // like PostgreSQL have the `<<` operator for that, ClickHouse supports only
    // the function `isIPAddressInRange()`.
    //
    // One consequence of this is that the caller needs to wrap the argument in
    // parentheses manually.
    pub(crate) fn as_db_function_name(&self) -> &'static str {
        match self {
            Comparison::Eq => "equals",
            Comparison::Ne => "notEquals",
            Comparison::Gt => "greater",
            Comparison::Ge => "greaterOrEquals",
            Comparison::Lt => "less",
            Comparison::Le => "lessOrEquals",
            Comparison::Like => "match",
        }
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Comparison::Eq => "==",
                Comparison::Ne => "!=",
                Comparison::Gt => ">",
                Comparison::Ge => ">=",
                Comparison::Lt => "<",
                Comparison::Le => "<=",
                Comparison::Like => "~=",
            }
        )
    }
}
