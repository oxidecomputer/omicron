// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use crate::component::SpIdentifier;

pub mod v1;
pub mod v2;

pub use v2::*;

#[derive(Deserialize, JsonSchema)]
pub struct PathSpIgnitionCommand {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// Ignition command to perform on the targeted SP.
    pub command: IgnitionCommand,
}

/// Ignition command.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IgnitionCommand {
    PowerOn,
    PowerOff,
    PowerReset,
}

impl From<IgnitionCommand> for gateway_messages::IgnitionCommand {
    fn from(cmd: IgnitionCommand) -> Self {
        match cmd {
            IgnitionCommand::PowerOn => {
                gateway_messages::IgnitionCommand::PowerOn
            }
            IgnitionCommand::PowerOff => {
                gateway_messages::IgnitionCommand::PowerOff
            }
            IgnitionCommand::PowerReset => {
                gateway_messages::IgnitionCommand::PowerReset
            }
        }
    }
}
