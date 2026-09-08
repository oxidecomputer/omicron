// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct OxideBarcode {
    pub part_number: String,
    pub revision: u32,
    pub serial_number: String,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct Mpn1Barcode {
    pub manufacturer: String,
    pub part_number: String,
    pub revision: String,
    pub serial_number: String,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub enum Barcode {
    Oxide(OxideBarcode),
    Mpn1(Mpn1Barcode),
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct SledFanTray {
    pub identity: OxideBarcode,
    pub vpd_board_identity: OxideBarcode,
    pub fan0: Barcode,
    pub fan1: Barcode,
    pub fan2: Barcode,
}
