// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration parameters to Nexus that are usually only known
//! at runtime.

use super::postgres_config::PostgresConfigWithUrl;
use dropshot::ConfigDropshot;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use uuid::Uuid;

#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RuntimeConfig {
    /// Uuid of the Nexus instance
    pub id: Uuid,
    /// Dropshot configuration for external API server
    pub dropshot_external: ConfigDropshot,
    /// Dropshot configuration for internal API server
    pub dropshot_internal: ConfigDropshot,
    /// Database parameters
    #[serde_as(as = "DisplayFromStr")]
    pub database_url: PostgresConfigWithUrl,
}

