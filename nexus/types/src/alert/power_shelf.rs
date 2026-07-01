// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Power shelf alert types.

use super::*;
use crate::external_api::hardware::Baseboard;
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PsuInsertedV0 {
    pub power_shelf: PowerShelf,
    pub psu: Psu,
    pub time: DateTime<Utc>,
}

impl AlertPayload for PsuInsertedV0 {
    const CLASS: AlertClass = AlertClass::PsuInserted;
    const VERSION: u32 = 0;
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PsuRemovedV0 {
    pub power_shelf: PowerShelf,
    pub psu: Psu,
    pub time: DateTime<Utc>,
}

impl AlertPayload for PsuRemovedV0 {
    const CLASS: AlertClass = AlertClass::PsuRemoved;
    const VERSION: u32 = 0;
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PowerShelf {
    // TODO(eliza): the PSC should probably have a UUID, but AFAICT, they don't
    // currently...
    pub rack_id: Uuid,
    pub shelf: u8,
    pub baseboard: Option<Baseboard>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Psu {
    pub slot: u8,
    pub identity: Option<PsuIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PsuIdentity {
    pub manufacturer: String,
    pub part: String,
    pub firmware_revision: String,
    pub serial: String,
}
