// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Interfaces for working with bootstrap agent configuration
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/**
 * Configuration for a bootstrap agent
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Config {
    pub id: Uuid,
    pub dropshot: ConfigDropshot,
    pub log: ConfigLogging,

    /// Configuration of the rack secret
    /// TODO: Remove this when rack secret initialization becomes dynamic
    pub rack_secret_dir: String,
}
