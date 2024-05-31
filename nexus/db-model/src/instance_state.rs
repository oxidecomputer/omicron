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
    #[diesel(postgres_type(name = "instance_state", schema = "public"))]
    pub struct InstanceStateEnum;

    #[derive(Copy, Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = InstanceStateEnum)]
    pub enum InstanceState;

    // Enum values
    Creating => b"creating"
    Starting => b"starting"
    Running => b"running"
    Stopping => b"stopping"
    Stopped => b"stopped"
    Rebooting => b"rebooting"
    Migrating => b"migrating"
    Repairing => b"repairing"
    Failed => b"failed"
    Destroyed => b"destroyed"
    SagaUnwound => b"saga_unwound"
);

impl InstanceState {
    pub fn new(state: external::InstanceState) -> Self {
        Self::from(state)
    }

    pub fn state(&self) -> external::InstanceState {
        external::InstanceState::from(*self)
    }

    pub fn label(&self) -> &'static str {
        match self {
            InstanceState::Creating => "creating",
            InstanceState::Starting => "starting",
            InstanceState::Running => "running",
            InstanceState::Stopping => "stopping",
            InstanceState::Stopped => "stopped",
            InstanceState::Rebooting => "rebooting",
            InstanceState::Migrating => "migrating",
            InstanceState::Repairing => "repairing",
            InstanceState::Failed => "failed",
            InstanceState::Destroyed => "destroyed",
            InstanceState::SagaUnwound => "saga_unwound",
        }
    }
}

impl fmt::Display for InstanceState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl From<InstanceState> for sled_agent_client::types::InstanceState {
    fn from(s: InstanceState) -> Self {
        use sled_agent_client::types::InstanceState as Output;
        match s {
            InstanceState::Creating => Output::Creating,
            InstanceState::Starting => Output::Starting,
            InstanceState::Running => Output::Running,
            InstanceState::Stopping => Output::Stopping,
            InstanceState::Stopped => Output::Stopped,
            InstanceState::Rebooting => Output::Rebooting,
            InstanceState::Migrating => Output::Migrating,
            InstanceState::Repairing => Output::Repairing,
            InstanceState::Failed => Output::Failed,
            InstanceState::Destroyed | InstanceState::SagaUnwound => {
                Output::Destroyed
            }
        }
    }
}

impl From<external::InstanceState> for InstanceState {
    fn from(state: external::InstanceState) -> Self {
        match state {
            external::InstanceState::Creating => InstanceState::Creating,
            external::InstanceState::Starting => InstanceState::Starting,
            external::InstanceState::Running => InstanceState::Running,
            external::InstanceState::Stopping => InstanceState::Stopping,
            external::InstanceState::Stopped => InstanceState::Stopped,
            external::InstanceState::Rebooting => InstanceState::Rebooting,
            external::InstanceState::Migrating => InstanceState::Migrating,
            external::InstanceState::Repairing => InstanceState::Repairing,
            external::InstanceState::Failed => InstanceState::Failed,
            external::InstanceState::Destroyed => InstanceState::Destroyed,
        }
    }
}

impl From<InstanceState> for external::InstanceState {
    fn from(state: InstanceState) -> Self {
        match state {
            InstanceState::Creating => external::InstanceState::Creating,
            InstanceState::Starting => external::InstanceState::Starting,
            InstanceState::Running => external::InstanceState::Running,
            InstanceState::Stopping => external::InstanceState::Stopping,
            InstanceState::Stopped => external::InstanceState::Stopped,
            InstanceState::Rebooting => external::InstanceState::Rebooting,
            InstanceState::Migrating => external::InstanceState::Migrating,
            InstanceState::Repairing => external::InstanceState::Repairing,
            InstanceState::Failed => external::InstanceState::Failed,
            InstanceState::Destroyed | InstanceState::SagaUnwound => {
                external::InstanceState::Destroyed
            }
        }
    }
}

impl PartialEq<external::InstanceState> for InstanceState {
    fn eq(&self, other: &external::InstanceState) -> bool {
        match (self, other) {
            (InstanceState::Creating, external::InstanceState::Creating)
            | (InstanceState::Starting, external::InstanceState::Starting)
            | (InstanceState::Running, external::InstanceState::Running)
            | (InstanceState::Stopping, external::InstanceState::Stopping)
            | (InstanceState::Stopped, external::InstanceState::Stopped)
            | (InstanceState::Rebooting, external::InstanceState::Rebooting)
            | (InstanceState::Migrating, external::InstanceState::Migrating)
            | (InstanceState::Repairing, external::InstanceState::Repairing)
            | (InstanceState::Failed, external::InstanceState::Failed)
            | (InstanceState::Destroyed, external::InstanceState::Destroyed)
            | (
                InstanceState::SagaUnwound,
                external::InstanceState::Destroyed,
            ) => true,
            _ => false,
        }
    }
}
