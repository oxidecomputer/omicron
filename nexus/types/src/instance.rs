// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use omicron_common::api::external::Generation;
use serde::{Deserialize, Serialize};
use sled_agent_types::instance as sled_agent;
use std::fmt;

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

impl SledVmmState {
    pub fn migrations(&self) -> Migrations<'_> {
        Migrations {
            migration_in: self.migration_in.as_ref(),
            migration_out: self.migration_out.as_ref(),
        }
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

impl VmmRuntimeState {
    /// Transitions the VMM state from the current state to `new_state`,
    /// returning a `VmmRuntimeState` representing the VMM after transitioning
    /// to the new state.
    ///
    /// If `new_state` is the same as the current state, returns a copy of
    /// `self` without any changes.
    pub fn transition(&self, new_state: VmmState) -> Self {
        if new_state == self.state {
            return self.clone();
        }
        Self {
            state: new_state,
            generation: self.generation.next(),
            time_updated: Utc::now(),
        }
    }
}

impl From<sled_agent::VmmRuntimeState> for VmmRuntimeState {
    fn from(state: sled_agent::VmmRuntimeState) -> Self {
        let sled_agent::VmmRuntimeState { state, generation, time_updated } =
            state;
        Self { state: state.into(), generation, time_updated }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum VmmState {
    /// The VMM exists in the database but has not begun initialization on the sled.
    Creating,
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
    /// The start saga which created this VMM has failed and unwound before the
    /// instance was able to start.
    SagaUnwound,
}

impl VmmState {
    /// Returns a human-readable label for this VMM state.
    pub fn label(&self) -> &'static str {
        match self {
            VmmState::Creating => "creating",
            VmmState::Starting => "starting",
            VmmState::Running => "running",
            VmmState::Stopping => "stopping",
            VmmState::Stopped => "stopped",
            VmmState::Rebooting => "rebooting",
            VmmState::Migrating => "migrating",
            VmmState::Failed(_) => "failed",
            VmmState::Destroyed => "destroyed",
            VmmState::SagaUnwound => "saga_unwound",
        }
    }

    /// Returns `true` if this is a terminal VMM state.
    ///
    /// A VMM in a terminal state will never transition to another state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, VmmState::Destroyed | VmmState::Failed(_))
    }

    /// Returns `true` if the VMM is in a `Failed` state.
    pub fn is_failed(&self) -> bool {
        matches!(self, VmmState::Failed(_))
    }

    /// Returns `true` if the VMM is in a state where it is safe to
    /// deallocate its sled resources and mark it as deleted.
    pub fn is_destroyable(&self) -> bool {
        matches!(
            self,
            VmmState::Destroyed | VmmState::Failed(_) | VmmState::SagaUnwound
        )
    }

    /// Returns `true` if the VMM exists on a sled.
    ///
    /// This is `false` for VMMs that have not yet been created, have been
    /// destroyed, or were produced by an unwound saga and will never exist.
    pub fn exists_on_sled(&self) -> bool {
        !matches!(
            self,
            VmmState::Creating | VmmState::SagaUnwound | VmmState::Destroyed
        )
    }
}

impl fmt::Display for VmmState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.label())
    }
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
    NoSuchInstance,
    /// The sled on which this VMM was running has been expunged.
    SledExpunged,
    /// The sled on which this VMM was running has powered off.
    SledOff,
}

impl VmmFailureReason {
    pub fn description(&self) -> &'static str {
        match self {
            Self::Prehistoric => "VMM failed prior to recorded history",
            Self::FromSledAgent => "failed VMM state received from sled-agent",
            Self::NoSuchInstance => "VMM no longer present on sled",
            Self::SledExpunged => {
                "the sled this VMM was running on has been expunged"
            }
            Self::SledOff => "the sled this VMM was running on powered off",
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Migrations<'state> {
    pub migration_in: Option<&'state sled_agent::MigrationRuntimeState>,
    pub migration_out: Option<&'state sled_agent::MigrationRuntimeState>,
}

impl Migrations<'_> {
    pub fn empty() -> Self {
        Self { migration_in: None, migration_out: None }
    }
}
