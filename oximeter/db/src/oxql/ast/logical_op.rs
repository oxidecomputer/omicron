// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An AST node describing logical operators.

// Copyright 2024 Oxide Computer Company

use std::fmt;

/// Logical operators.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
    Xor,
}

impl LogicalOp {
    pub(crate) fn as_db_function_name(&self) -> &'static str {
        match self {
            LogicalOp::And => "and",
            LogicalOp::Or => "or",
            LogicalOp::Xor => "xor",
        }
    }
}

impl fmt::Display for LogicalOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                LogicalOp::And => "&&",
                LogicalOp::Or => "||",
                LogicalOp::Xor => "^",
            }
        )
    }
}
