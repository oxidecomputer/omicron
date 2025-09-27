// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod v1;
pub mod v2;

pub use v2::*;

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
