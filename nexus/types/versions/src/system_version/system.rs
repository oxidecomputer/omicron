// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! System types for version `SYSTEM_VERSION`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The version of the Oxide software running on this system.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SystemVersion {
    /// The current system software version
    pub version: semver::Version,
}
