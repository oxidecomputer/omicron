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

// TODO-RAINCLAUDE: SpType, SpIdentifier, PowerState, and RotSlot are unified
// TODO-RAINCLAUDE: with their canonical gateway home so there is a single
// TODO-RAINCLAUDE: definition (RFD 619). Note that gateway's RotSlot is
// TODO-RAINCLAUDE: internally tagged, so it serializes as `{"slot":"a"}`
// TODO-RAINCLAUDE: rather than a bare `"a"`.
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
    /// The caboose of the stage0 bootloader, if present and read.
    pub caboose_stage0: Option<Caboose>,
    /// The caboose of the pending stage0next bootloader, if present and read.
    pub caboose_stage0next: Option<Caboose>,
}

/// A projected view of a single service processor's inventory.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct SpInfo {
    /// Identifies the service processor by type and slot.
    pub id: SpIdentifier,
    /// The service processor's serial number, if its state has been read.
    ///
    /// This is populated together with `power_state` from the service
    /// processor's state: both are `Some`, or both are `None`.
    pub serial_number: Option<String>,
    /// The host power state, if the service processor's state has been read.
    ///
    /// This is populated together with `serial_number` from the service
    /// processor's state: both are `Some`, or both are `None`.
    pub power_state: Option<PowerState>,
    /// Whether ignition reports this service processor as present.
    ///
    /// `None` means ignition state has not been read yet; `Some(false)` means
    /// ignition reports the service processor as absent; `Some(true)` means
    /// ignition reports it as present.
    pub ignition_present: Option<bool>,
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

/// Query parameters for the SP inventory endpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct SpInventoryParams {
    /// If true, re-poll the service processors currently known to the inventory
    /// cache from MGS and return the refreshed state, instead of returning
    /// cached data. If the inventory cache is not yet available, the request
    /// fails with a 503 rather than returning possibly-unrefreshed data.
    #[serde(default)]
    pub force_refresh: bool,
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
    /// The slot (cubby) the sled occupies.
    pub slot: u16,
    /// The sled's baseboard identifier (serial number).
    pub identifier: String,
    /// The sled's bootstrap-network address, once it has been discovered.
    pub ip: Option<Ipv6Addr>,
}

impl IdOrdItem for BootstrapSled {
    type Key<'a> = &'a str;

    fn key(&self) -> Self::Key<'_> {
        &self.identifier
    }

    id_upcast!();
}
