// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::{Name, UserId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the recovery silo created during rack setup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RecoverySiloConfig {
    pub silo_name: Name,
    pub user_name: UserId,
    pub user_password_hash: omicron_passwords::NewPasswordHash,
}
