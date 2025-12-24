// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled types for version INITIAL.

use super::asset::AssetIdentityMetadata;
use super::hardware::Baseboard;
use daft::Diffable;
use omicron_common::api::external::{ByteCount, InstanceState, Name};
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::EnumIter;
use uuid::Uuid;

// PARAMS

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledSelector {
    /// ID of the sled
    #[schemars(with = "Uuid")]
    pub sled: SledUuid,
}

/// Parameters for `sled_set_provision_policy`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledProvisionPolicyParams {
    /// The provision state.
    pub state: SledProvisionPolicy,
}

/// Response to `sled_set_provision_policy`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledProvisionPolicyResponse {
    /// The old provision state.
    pub old_state: SledProvisionPolicy,

    /// The new provision state.
    pub new_state: SledProvisionPolicy,
}

pub struct SwitchSelector {
    /// ID of the switch
    pub switch: Uuid,
}

// VIEWS

/// The unique ID of a sled.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct SledId {
    #[schemars(with = "Uuid")]
    pub id: SledUuid,
}

/// An operator's view of a Sled.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Sled {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub baseboard: Baseboard,
    /// The rack to which this Sled is currently attached
    pub rack_id: Uuid,
    /// The operator-defined policy of a sled.
    pub policy: SledPolicy,
    /// The current state of the sled.
    pub state: SledState,
    /// The number of hardware threads which can execute on this sled
    pub usable_hardware_threads: u32,
    /// Amount of RAM which may be used by the Sled's OS
    pub usable_physical_ram: ByteCount,
}

/// The operator-defined provision policy of a sled.
///
/// This controls whether new resources are going to be provisioned on this
/// sled.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum SledProvisionPolicy {
    /// New resources will be provisioned on this sled.
    Provisionable,

    /// New resources will not be provisioned on this sled. However, if the
    /// sled is currently in service, existing resources will continue to be on
    /// this sled unless manually migrated off.
    NonProvisionable,
}

/// The operator-defined policy of a sled.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SledPolicy {
    /// The operator has indicated that the sled is in-service.
    InService {
        /// Determines whether new resources can be provisioned onto the sled.
        provision_policy: SledProvisionPolicy,
    },

    /// The operator has indicated that the sled has been permanently removed
    /// from service.
    ///
    /// This is a terminal state: once a particular sled ID is expunged, it
    /// will never return to service. (The actual hardware may be reused, but
    /// it will be treated as a brand-new sled.)
    ///
    /// An expunged sled is always non-provisionable.
    Expunged,
    //
    // NOTE: If you add another variant here, be sure to update `impl
    // IntoEnumIterator for SledPolicy` in `impls/sled.rs`with the new variant.
}

/// The current state of the sled.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    EnumIter,
    Diffable,
)]
#[serde(rename_all = "snake_case")]
pub enum SledState {
    /// The sled is currently active, and has resources allocated on it.
    Active,

    /// The sled has been permanently removed from service.
    ///
    /// This is a terminal state: once a particular sled ID is decommissioned,
    /// it will never return to service. (The actual hardware may be reused,
    /// but it will be treated as a brand-new sled.)
    Decommissioned,
}

/// An operator's view of an instance running on a given sled
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SledInstance {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub active_sled_id: Uuid,
    pub migration_id: Option<Uuid>,
    pub name: Name,
    pub silo_name: Name,
    pub project_name: Name,
    pub state: InstanceState,
    pub ncpus: i64,
    pub memory: i64,
}
