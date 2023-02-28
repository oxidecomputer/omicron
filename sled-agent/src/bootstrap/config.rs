// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for working with bootstrap agent configuration

use crate::sp::SimSpConfig;
use dropshot::ConfigLogging;
use illumos_utils::dladm::PhysicalLink;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

pub const BOOTSTRAP_AGENT_PORT: u16 = 12346;

/// Configuration for a bootstrap agent
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Config {
    pub id: Uuid,
    pub link: PhysicalLink,
    pub log: ConfigLogging,
    pub rss_config: Option<crate::rack_setup::config::SetupServiceConfig>,
    pub sp_config: Option<SimSpConfig>,
}
