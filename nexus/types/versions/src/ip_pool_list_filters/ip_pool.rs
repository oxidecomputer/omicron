// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::IpVersion;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Filters for listing IP pools.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolListFilter {
    /// Restrict pools to a specific IP version.
    pub ip_version: Option<IpVersion>,

    /// Filter on pools delegated for internal Oxide use.
    ///
    /// Defaults to excluding internal pools when unset.
    pub delegated_for_internal_use: Option<bool>,
}
