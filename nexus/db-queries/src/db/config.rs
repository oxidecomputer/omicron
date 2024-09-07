// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus database configuration

use nexus_config::PostgresConfigWithUrl;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;

/// Nexus database configuration
#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    /// database url
    #[serde_as(as = "DisplayFromStr")]
    pub url: PostgresConfigWithUrl,
}
