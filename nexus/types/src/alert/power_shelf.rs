// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Power shelf alert types.

use super::*;
use crate::external_api::hardware::Baseboard;
use chrono::DateTime;
use chrono::Utc;
use omicron_uuid_kinds::RackUuid;
use serde::Deserialize;
use uuid::Uuid;

/// An alert indicating that a power supply unit (PSU) has been inserted into a
/// power shelf.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PsuInsertedV0 {
    /// The ID of the rack containing the power shelf.
    #[schemars(with = "Uuid")]
    pub rack_id: RackUuid,
    pub power_shelf: PowerShelf,
    pub psu: Psu,
    pub time: DateTime<Utc>,
}

impl AlertPayload for PsuInsertedV0 {
    const CLASS: AlertClass = AlertClass::PsuInserted;
    const VERSION: u32 = 0;
}

/// An alert indicating that a power supply unit (PSU) has been removed from a
/// power shelf.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PsuRemovedV0 {
    /// The ID of the rack containing the power shelf.
    #[schemars(with = "Uuid")]
    pub rack_id: RackUuid,
    pub power_shelf: PowerShelf,
    pub psu: Psu,
    pub time: DateTime<Utc>,
}

impl AlertPayload for PsuRemovedV0 {
    const CLASS: AlertClass = AlertClass::PsuRemoved;
    const VERSION: u32 = 0;
}

/// Describes the power shelf involved in an alert.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PowerShelf {
    // TODO(eliza): the PSC should probably have a UUID, but AFAICT, they don't
    // currently...
    /// The physical shelf number of the power shelf involved in the alert.
    ///
    /// This will always be either 0 or 1.
    #[schemars(range(min = 0, max = 1))]
    pub shelf: u8,
    /// The baseboard FRU identity of the power shelf controller that was
    /// installed in the power shelf at the time the event occurred.
    pub baseboard: Option<Baseboard>,
}

/// Describes a power supply unit (PSU) involved in an alert.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Psu {
    /// The slot number of the PSU within the power shelf.
    ///
    /// This will always be an integer between 0 and 5 (inclusive), since each
    /// power shelf can contain up to 6 PSUs.
    #[schemars(range(min = 0, max = 5))]
    pub slot: u8,
    /// The FRU identity of the PSU, if it could be determined.
    ///
    /// This value may be NULL if the identity of the PSU could not be read due
    /// to a hardware failure.
    pub identity: Option<PsuIdentity>,
}

/// Describes the FRU identity of a power supply unit (PSU).
///
/// Note that this is *not* in the same format as Oxide FRU identities.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PsuIdentity {
    pub manufacturer: String,
    pub part: String,
    pub firmware_revision: String,
    pub serial: String,
}
