// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "vmm_state", schema = "public"))]
    pub struct VmmStateEnum;

    #[derive(Copy, Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = VmmStateEnum)]
    pub enum VmmState;

    Starting => b"starting"
    Running => b"running"
    Stopping => b"stopping"
    Stopped => b"stopped"
    Rebooting => b"rebooting"
    Migrating => b"migrating"
    Failed => b"failed"
    Destroyed => b"destroyed"
    SagaUnwound => b"saga_unwound"
);

impl VmmState {
    pub fn label(&self) -> &'static str {
        match self {
            VmmState::Starting => "starting",
            VmmState::Running => "running",
            VmmState::Stopping => "stopping",
            VmmState::Stopped => "stopped",
            VmmState::Rebooting => "rebooting",
            VmmState::Migrating => "migrating",
            VmmState::Failed => "failed",
            VmmState::Destroyed => "destroyed",
            VmmState::SagaUnwound => "saga_unwound",
        }
    }
}

impl fmt::Display for VmmState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl From<VmmState> for omicron_common::api::internal::nexus::VmmState {
    fn from(value: VmmState) -> Self {
        use omicron_common::api::internal::nexus::VmmState as Output;
        match value {
            VmmState::Starting => Output::Starting,
            VmmState::Running => Output::Running,
            VmmState::Stopping => Output::Stopping,
            VmmState::Stopped => Output::Stopped,
            VmmState::Rebooting => Output::Rebooting,
            VmmState::Migrating => Output::Migrating,
            VmmState::Failed => Output::Failed,
            VmmState::Destroyed | VmmState::SagaUnwound => Output::Destroyed,
        }
    }
}

impl From<VmmState> for sled_agent_client::types::VmmState {
    fn from(value: VmmState) -> Self {
        use sled_agent_client::types::VmmState as Output;
        match value {
            VmmState::Starting => Output::Starting,
            VmmState::Running => Output::Running,
            VmmState::Stopping => Output::Stopping,
            VmmState::Stopped => Output::Stopped,
            VmmState::Rebooting => Output::Rebooting,
            VmmState::Migrating => Output::Migrating,
            VmmState::Failed => Output::Failed,
            VmmState::Destroyed | VmmState::SagaUnwound => Output::Destroyed,
        }
    }
}

impl From<omicron_common::api::internal::nexus::VmmState> for VmmState {
    fn from(value: omicron_common::api::internal::nexus::VmmState) -> Self {
        use omicron_common::api::internal::nexus::VmmState as ApiState;
        use VmmState as Output;
        match value {
            ApiState::Starting => Output::Starting,
            ApiState::Running => Output::Running,
            ApiState::Stopping => Output::Stopping,
            ApiState::Stopped => Output::Stopped,
            ApiState::Rebooting => Output::Rebooting,
            ApiState::Migrating => Output::Migrating,
            ApiState::Failed => Output::Failed,
            ApiState::Destroyed => Output::Destroyed,
        }
    }
}

impl From<VmmState> for omicron_common::api::external::InstanceState {
    fn from(value: VmmState) -> Self {
        use omicron_common::api::external::InstanceState as Output;

        match value {
            VmmState::Starting => Output::Starting,
            VmmState::Running => Output::Running,
            VmmState::Stopping => Output::Stopping,
            // `SagaUnwound` should map to `Stopped` so that an `instance_view`
            // API call that produces an instance with an unwound VMM will appear to
            // be `Stopped`. This is because instances with unwound VMMs can
            // be started by a subsequent instance-start saga, just like
            // instances whose internal state actually is `Stopped`.
            VmmState::Stopped | VmmState::SagaUnwound => Output::Stopped,
            VmmState::Rebooting => Output::Rebooting,
            VmmState::Migrating => Output::Migrating,
            VmmState::Failed => Output::Failed,
            VmmState::Destroyed => Output::Destroyed,
        }
    }
}

impl diesel::query_builder::QueryId for VmmStateEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
