// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP header types for the Nexus external API.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Range request headers
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct RangeRequest {
    /// A request to access a portion of the resource, such as `bytes=0-499`
    ///
    /// See: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Range>
    pub range: Option<String>,
}
