// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! System types for version `API_VERSION`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The version of the Oxide API currently being served.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApiVersion {
    /// The current API version, corresponding to a numbered Oxide release
    /// (e.g., `19.0.0` for release 19).
    pub version: semver::Version,
}
