// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! System types for version `API_VERSION`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Version information for the API and the system software serving it
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Version {
    /// The API version, e.g. `2026062900.0.0`, matching the `version` field
    /// of the OpenAPI schema. Clients should send it in the `api-version`
    /// header to select this version of the API. The official SDKs do this
    /// automatically.
    pub api_version: semver::Version,

    /// The version of the system software serving this API, e.g., `21.0.0`.
    /// Use this version to find the right docs and release notes. During an
    /// upgrade, some components of the system may be running a newer version.
    pub system_version: semver::Version,
}
