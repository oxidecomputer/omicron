// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use omicron_common::api::internal::nexus::VmmState as ApiState;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone)]
    #[diesel(postgres_type(name = "vmm_state", schema = "public"))]
    pub struct VmmStateEnum;

    #[derive(Copy, Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = VmmStateEnum)]
    pub enum VmmState;

    Creating => b"creating"
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
            VmmState::Creating => "creating",
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

    /// States in which it is safe to deallocate a VMM's sled resources and mark
    /// it as deleted.
    pub const DESTROYABLE_STATES: &'static [Self] =
        &[Self::Destroyed, Self::Failed, Self::SagaUnwound];

    pub const TERMINAL_STATES: &'static [Self] =
        &[Self::Destroyed, Self::Failed];

    /// States in which a VMM record is present in the database but is not
    /// resident on a sled, either because it does not yet exist, was produced
    /// by an unwound update saga and will never exist, or has already been
    /// destroyed.
    pub const NONEXISTENT_STATES: &'static [Self] =
        &[Self::Creating, Self::SagaUnwound, Self::Destroyed];

    pub fn is_terminal(&self) -> bool {
        Self::TERMINAL_STATES.contains(self)
    }
    /// Returns `true` if the instance is in a state in which it exists on a
    /// sled.
    pub fn exists_on_sled(&self) -> bool {
        !Self::NONEXISTENT_STATES.contains(self)
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
            // The `Creating` state is internal to Nexus; the outside world
            // should treat it as equivalent to `Starting`.
            VmmState::Creating | VmmState::Starting => Output::Starting,
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
            // The `Creating` state is internal to Nexus; the outside world
            // should treat it as equivalent to `Starting`.
            VmmState::Creating | VmmState::Starting => Output::Starting,
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

impl From<ApiState> for VmmState {
    fn from(value: ApiState) -> Self {
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
            // An instance with a VMM which is in the `Creating` state maps to
            // `InstanceState::Starting`, rather than `InstanceState::Creating`.
            // If we are still creating the VMM, this is because we are
            // attempting to *start* the instance; instances may be created
            // without creating a VMM to run them, and then started later.
            VmmState::Creating | VmmState::Starting => Output::Starting,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_terminal_api_states_are_terminal_db_states() {
        for &api_state in ApiState::TERMINAL_STATES {
            let db_state = VmmState::from(api_state);
            assert!(
                db_state.is_terminal(),
                "API VmmState::{api_state:?} is considered terminal, but its \
                 corresponding DB state ({db_state:?}) is not!"
            );
        }
    }
}
