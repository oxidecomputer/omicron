// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use omicron_common::api::external::Generation;
use serde::{Deserialize, Serialize};
use sled_agent_types::instance as sled_agent;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SledVmmState {
    pub vmm_state: VmmRuntimeState,
    /// The current state of any inbound migration to this VMM.
    pub migration_in: Option<sled_agent::MigrationRuntimeState>,

    /// The state of any outbound migration from this VMM.
    pub migration_out: Option<sled_agent::MigrationRuntimeState>,
}

impl From<sled_agent::SledVmmState> for SledVmmState {
    fn from(state: sled_agent::SledVmmState) -> Self {
        let sled_agent::SledVmmState { vmm_state, migration_in, migration_out } =
            state;
        Self { vmm_state: vmm_state.into(), migration_in, migration_out }
    }
}

/// The dynamic runtime properties of an individual VMM process.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct VmmRuntimeState {
    /// The last state reported by this VMM.
    pub state: VmmState,
    /// The generation number for this VMM's state.
    #[serde(rename = "gen")]
    pub generation: Generation,
    /// Timestamp for the VMM's state.
    pub time_updated: DateTime<Utc>,
}

impl From<sled_agent::VmmRuntimeState> for VmmRuntimeState {
    fn from(state: sled_agent::VmmRuntimeState) -> Self {
        let sled_agent::VmmRuntimeState { state, generation, time_updated } =
            state;
        Self { state: state.into(), generation, time_updated }
    }
}

impl From<VmmRuntimeState> for sled_agent::VmmRuntimeState {
    fn from(state: VmmRuntimeState) -> Self {
        let VmmRuntimeState { state, generation, time_updated } = state;
        Self { state: state.into(), generation, time_updated }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum VmmState {
    /// The VMM is initializing and has not started running guest CPUs yet.
    Starting,
    /// The VMM has finished initializing and may be running guest CPUs.
    Running,
    /// The VMM is shutting down.
    Stopping,
    /// The VMM's guest has stopped, and the guest will not run again, but the
    /// VMM process may not have released all of its resources yet.
    Stopped,
    /// The VMM is being restarted or its guest OS is rebooting.
    Rebooting,
    /// The VMM is part of a live migration.
    Migrating,
    /// The VMM process reported an internal failure.
    Failed(VmmFailureReason),
    /// The VMM process has been destroyed and its resources have been released.
    Destroyed,
}

impl From<sled_agent::VmmState> for VmmState {
    fn from(state: sled_agent::VmmState) -> Self {
        match state {
            sled_agent::VmmState::Starting => VmmState::Starting,
            sled_agent::VmmState::Running => VmmState::Running,
            sled_agent::VmmState::Stopping => VmmState::Stopping,
            sled_agent::VmmState::Stopped => VmmState::Stopped,
            sled_agent::VmmState::Rebooting => VmmState::Rebooting,
            sled_agent::VmmState::Migrating => VmmState::Migrating,
            sled_agent::VmmState::Failed => {
                // If we are converting a state received from a sled-agent that
                // indicates that the VMM is failed, the failure reason is
                // implicitly "from_sled_agent" for now.
                VmmState::Failed(VmmFailureReason::FromSledAgent)
            }
            sled_agent::VmmState::Destroyed => VmmState::Destroyed,
        }
    }
}

impl From<VmmState> for sled_agent::VmmState {
    fn from(state: VmmState) -> Self {
        match state {
            VmmState::Starting => sled_agent::VmmState::Starting,
            VmmState::Running => sled_agent::VmmState::Running,
            VmmState::Stopping => sled_agent::VmmState::Stopping,
            VmmState::Stopped => sled_agent::VmmState::Stopped,
            VmmState::Rebooting => sled_agent::VmmState::Rebooting,
            VmmState::Migrating => sled_agent::VmmState::Migrating,
            VmmState::Failed(_) => sled_agent::VmmState::Failed,
            VmmState::Destroyed => sled_agent::VmmState::Destroyed,
        }
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum VmmFailureReason {
    /// The reason for this VMM's failure is unknown, because the VMM failed
    /// prior to the recording of failure reasons.
    Prehistoric,
    /// The sled-agent reported that this VMM failed.
    FromSledAgent,
    /// A request to the sled-agent received a response indicating that this
    /// VMM is no longer present on the sled.
    NoSuchVmm,
    /// The sled on which this VMM was running has been expunged.
    SledExpunged,
    /// The sled on which this VMM was running has powered off.
    SledOff,
}

impl VmmFailureReason {
    pub fn description(&self) -> &'static str {
        match self {
            Self::Prehistoric => "<VMM failed prior to recorded history>",
            Self::FromSledAgent => "failed VMM state received from sled-agent",
            Self::NoSuchVmm => "VMM no longer present on sled",
            Self::SledExpunged => {
                "the sled this VMM was running on has been expunged"
            }
            Self::SledOff => "the sled this VMM was running on powered off",
        }
    }
}
