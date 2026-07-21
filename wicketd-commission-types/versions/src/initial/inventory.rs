// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory and discovery types for the commissioning API.
//!
//! These are deliberately minimal projections of wicketd's internal inventory
//! shapes: they carry only the fields the commissioning client needs, so that
//! the large and less stable internal inventory type graph (MGS inventory) does
//! not leak into this stable API.

use std::net::Ipv6Addr;

use iddqd::{IdOrdItem, id_upcast};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Re-exports of pinned gateway types since they are also published by this API.
pub use gateway_types_versions::v1::component::{
    PowerState, SpIdentifier, SpType,
};
pub use gateway_types_versions::v1::rot::RotSlot;

/// A minimal projection of a firmware caboose.
///
/// Only the fields the commissioning client displays are included; the full
/// caboose (git commit, sign, epoch, and so on) is intentionally omitted.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct Caboose {
    /// The firmware version string.
    pub version: String,
    /// The board name the firmware was built for.
    pub board: String,
}

/// The stage0 (or pending stage0next) bootloader caboose for a root of trust.
///
/// The three states are kept distinct because they mean different things to a
/// caller:
///
/// * `unsupported`: this RoT version does not report a stage0 bootloader
///   caboose at all.
/// * `not_read`: the RoT version supports reporting a stage0 caboose, but it
///   has not been read yet.
/// * `read`: the stage0 caboose was read.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Stage0Caboose {
    /// This RoT version does not report a stage0 bootloader caboose.
    Unsupported,
    /// The stage0 bootloader caboose is supported but has not been read yet.
    NotRead,
    /// The stage0 bootloader caboose was read.
    Read {
        /// The caboose that was read.
        caboose: Caboose,
    },
}

/// Root-of-trust information for a single service processor.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct RotInfo {
    /// The currently-active RoT image slot.
    pub active: RotSlot,
    /// The caboose of the image in slot A, if it could be read.
    pub caboose_a: Option<Caboose>,
    /// The caboose of the image in slot B, if it could be read.
    pub caboose_b: Option<Caboose>,
    /// The caboose of the stage0 bootloader.
    pub caboose_stage0: Stage0Caboose,
    /// The caboose of the pending stage0next bootloader.
    pub caboose_stage0next: Stage0Caboose,
}

/// The service processor's state, read together from MGS.
///
/// The serial number and power state are always read together from the same
/// service-processor state, so they are presented as a single unit that is
/// either entirely present or entirely absent.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct SpStateInfo {
    /// The service processor's serial number.
    pub serial_number: String,
    /// The host power state.
    pub power_state: PowerState,
}

/// The faults ignition reports for a present service processor.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct IgnitionFaults {
    /// The A3 power fault.
    pub a3: bool,
    /// The A2 power fault.
    pub a2: bool,
    /// The root-of-trust fault.
    pub rot: bool,
    /// The service-processor fault.
    pub sp: bool,
}

/// The ignition state of a service processor.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SpIgnitionInfo {
    /// Ignition reports the service processor as present.
    Present {
        /// Whether the service processor is powered on.
        power: bool,
        /// The faults ignition reports for the service processor.
        faults: IgnitionFaults,
    },
    /// Ignition reports the service processor as absent.
    Absent,
}

/// A projected view of a single service processor's inventory.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct SpInfo {
    /// Identifies the service processor by type and slot.
    pub id: SpIdentifier,
    /// The service processor's state, if it has been read.
    pub state: Option<SpStateInfo>,
    /// The ignition state of this service processor.
    ///
    /// `None` means ignition state has not been read yet.
    pub ignition: Option<SpIgnitionInfo>,
    /// The caboose of the active service-processor firmware slot, if read.
    pub caboose_active: Option<Caboose>,
    /// The caboose of the inactive service-processor firmware slot, if read.
    pub caboose_inactive: Option<Caboose>,
    /// Root-of-trust information, if it has been read.
    pub rot: Option<RotInfo>,
}

impl IdOrdItem for SpInfo {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

/// The physical location of the sled wicketd is running on.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct LocationInfo {
    /// The slot (0 or 1) of the switch this sled is cabled to.
    pub switch_slot: u16,
    /// The serial number of that switch's service processor, if known.
    pub switch_serial: Option<String>,
    /// The serial number of the sled wicketd is running on, if known.
    pub sled_serial: Option<String>,
}

/// Parameters for the SP inventory endpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpInventoryParams {
    /// Refresh the state of these service processors from MGS before returning,
    /// rather than returning their cached state. Service processors not listed
    /// here are returned from the cache.
    #[serde(default)]
    pub force_refresh: Vec<SpIdentifier>,
}

/// A sled as seen on the bootstrap network.
///
/// A sled is reported here once its service processor's state has been read
/// from MGS; a populated cubby whose state has not yet been polled is absent
/// until it is. A sled's `ip` becomes `Some` once it has been discovered on
/// the bootstrap network; sleds still missing an address report `None`, which
/// lets callers see both ready and not-yet-ready sleds.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct BootstrapSled {
    /// The service processor for this sled (its type and slot).
    pub id: SpIdentifier,
    /// The sled's baseboard serial number.
    pub serial_number: String,
    /// The sled's bootstrap-network address, once it has been discovered.
    pub ip: Option<Ipv6Addr>,
}

impl IdOrdItem for BootstrapSled {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}
