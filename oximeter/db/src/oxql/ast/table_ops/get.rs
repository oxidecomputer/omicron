// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! AST node for the `get` table operation.

// Copyright 2024 Oxide Computer Company

use oximeter::TimeseriesName;

/// An AST node like: `get foo:bar`
#[derive(Clone, Debug, PartialEq)]
pub struct Get {
    pub timeseries_name: TimeseriesName,
}
