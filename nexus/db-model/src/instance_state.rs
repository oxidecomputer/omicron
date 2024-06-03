// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "instance_state_v2", schema = "public"))]
    pub struct InstanceStateEnum;

    #[derive(Copy, Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = InstanceStateEnum)]
    pub enum InstanceState;

    // Enum values
    Creating => b"creating"
    NoVmm => b"no_vmm"
    Vmm => b"vmm"
    Failed => b"failed"
    Destroyed => b"destroyed"
);

impl InstanceState {
    /* TODO(gjc)
    pub fn new(state: external::InstanceState) -> Self {
        Self(state)
    }
    */

    pub fn state(&self) -> external::InstanceState {
        external::InstanceState::from(*self)
    }

    pub fn label(&self) -> &'static str {
        match self {
            InstanceState::Creating => "creating",
            InstanceState::NoVmm => "no VMM",
            InstanceState::Vmm => "VMM",
            InstanceState::Failed => "failed",
            InstanceState::Destroyed => "destroyed",
        }
    }
}

impl fmt::Display for InstanceState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl From<InstanceState> for omicron_common::api::external::InstanceState {
    fn from(value: InstanceState) -> Self {
        use omicron_common::api::external::InstanceState as Output;
        match value {
            InstanceState::Creating => Output::Creating,
            InstanceState::NoVmm => Output::Stopped,
            InstanceState::Vmm => Output::Running,
            InstanceState::Failed => Output::Failed,
            InstanceState::Destroyed => Output::Destroyed,
        }
    }
}

impl From<InstanceState> for sled_agent_client::types::InstanceState {
    fn from(s: InstanceState) -> Self {
        todo!("gjc");
        /*
        use external::InstanceState::*;
        use sled_agent_client::types::InstanceState as Output;
        match s.0 {
            Creating => Output::Creating,
            Starting => Output::Starting,
            Running => Output::Running,
            Stopping => Output::Stopping,
            Stopped => Output::Stopped,
            Rebooting => Output::Rebooting,
            Migrating => Output::Migrating,
            Repairing => Output::Repairing,
            Failed => Output::Failed,
            Destroyed => Output::Destroyed,
        }
        */
    }
}

// TODO(gjc) do we need this?
/*
impl From<external::InstanceState> for InstanceState {
    fn from(state: external::InstanceState) -> Self {
        Self::new(state)
    }
}
*/
