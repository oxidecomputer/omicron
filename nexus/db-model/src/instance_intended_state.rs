// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    InstanceIntendedStateEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
    )]
    pub enum InstanceIntendedState;

    // Enum values
    Running => b"running"
    Stopped => b"stopped"
    GuestShutdown => b"guest_shutdown"
    Destroyed => b"destroyed"
);

impl InstanceIntendedState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::GuestShutdown => "guest_shutdown",
            Self::Destroyed => "destroyed",
        }
    }
}

impl fmt::Display for InstanceIntendedState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
