// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Interfaces for working with bootstrap agent configuration
 */

use crate::config::ConfigError;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;
use std::path::Path;
use uuid::Uuid;

/// Configuration for a bootstrap agent
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub id: Uuid,
    pub dropshot: ConfigDropshot,
    pub log: ConfigLogging,

    pub rss_config: Option<SetupServiceConfig>,
}

/// Configuration for the "rack setup service", which is controlled during
/// bootstrap.
///
/// The Rack Setup Service should be responsible for one-time setup actions,
/// such as CockroachDB placement and initialization.  Without operator
/// intervention, however, these actions need a way to be automated in our
/// deployment.
///
/// By injecting this (optional) configuration into the bootstrap agent, it
/// can act as a stand-in initialization service.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SetupServiceConfig {
    #[serde(default, rename = "request")]
    pub requests: Vec<PartitionRequest>,
}

/// A request to initialize a partition.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PartitionRequest {
    /// The Sled Agent address receiving these partitions.
    pub sled_address: SocketAddr,
    /// The request to be sent.
    #[serde(default, rename = "partition")]
    pub partitions: Vec<omicron_common::api::internal::sled_agent::PartitionEnsureBody>,
}

impl SetupServiceConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)?;
        let config = toml::from_str(&contents)?;
        Ok(config)
    }
}
