// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Power shelf alerts.

use super::{Alert, VpdIdentity};
use nexus_types::fm::AlertClass;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "version", rename_all = "snake_case")]
pub enum PsuInserted {
    V0 {
        #[serde(flatten)]
        psc_psu: PscPsu,
    },
}

impl Alert for PsuInserted {
    const CLASS: AlertClass = AlertClass::PsuInserted;
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "version", rename_all = "snake_case")]
pub enum PsuRemoved {
    V0 {
        #[serde(flatten)]
        psc_psu: PscPsu,
    },
}

impl Alert for PsuRemoved {
    const CLASS: AlertClass = AlertClass::PsuInserted;
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PscPsu {
    pub psc_id: VpdIdentity,
    pub psc_slot: u16,
    pub psu_id: PsuIdentity,
    pub psu_slot: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PsuIdentity {
    pub manufacturer: Option<String>,
    pub part_number: Option<String>,
    pub firmware_revision: Option<String>,
    pub serial_number: Option<String>,
}
