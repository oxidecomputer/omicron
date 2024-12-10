// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OxQL query plan node for limiting data.

// Copyright 2024 Oxide Computer Company

use crate::oxql::ast::table_ops::limit;
use crate::oxql::plan::plan::TableOpOutput;

/// A plan node for limiting the number of samples per-timeseries.
#[derive(Clone, Debug, PartialEq)]
pub struct Limit {
    pub output: TableOpOutput,
    pub limit: limit::Limit,
}
