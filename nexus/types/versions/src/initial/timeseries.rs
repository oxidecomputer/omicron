// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Timeseries query parameters for the Nexus external API.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A timeseries query string, written in the Oximeter query language.
#[derive(Deserialize, JsonSchema, Serialize)]
pub struct TimeseriesQuery {
    /// A timeseries query string, written in the Oximeter query language.
    pub query: String,
    /// Whether to include query summaries in the response. Note: we omit this
    /// field from the generated docs, since it is not intended for consumption
    /// by customers.
    #[serde(default)]
    #[schemars(skip)]
    pub include_summaries: bool,
}
