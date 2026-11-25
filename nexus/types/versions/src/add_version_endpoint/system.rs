// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! System types for version `ADD_VERSION_ENDPOINT`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Version information for the API and the system software serving it
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SystemVersion {
    /// The latest API version this system supports, e.g., `2026112100.0.0`,
    /// equal to `info.version` in its OpenAPI document. A request with no
    /// `api-version` header is handled at this version. A request for a
    /// newer one is rejected. Clients should send the version they were built
    /// for, which may be older than this one. The official CLI and SDKs do so
    /// automatically.
    pub api_version: semver::Version,

    /// The release number of the system software serving this API, e.g., `24`
    /// for release 24. This number does not distinguish patch releases such
    /// as 24.1. Use it to find the right docs, release notes, and CLI or SDK
    /// versions.
    pub system_version: String,
}
