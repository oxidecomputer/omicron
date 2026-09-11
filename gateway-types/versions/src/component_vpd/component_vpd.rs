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
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum ComponentVpd {
    OxideBarcode(OxideBarcode),
    Mpn1Barcode(Mpn1Barcode),
    SledFanTray(SledFanTray),
    Tmp11x(Tmp11x),
    Pmbus(PmbusDevice),
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
#[serde(tag = "type", rename_all = "snake_case")]
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
pub struct Tmp11x {
    pub device_id: u16,
    pub eeprom1: u16,
    pub eeprom2: u16,
    pub eeprom3: u16,
}

/// PMBus vital product data (VPD) read from a PMBus device.
///
/// If the device does not support a particular VPD command, the field in this
/// struct corresponding to that command will be `None`. Otherwise, the value
/// contains the exact bytes returned by the device, including an empty value or
/// any NUL bytes returned by the device.
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
pub struct PmbusDevice {
    /// `MFR_ID` (PMBus command 0x99).
    pub mfr_id: Option<Vec<u8>>,
    /// `MFR_MODEL` (PMBus command 0x9A).
    pub mfr_model: Option<Vec<u8>>,
    /// `MFR_REVISION` (PMBus command 0x9B).
    pub mfr_revision: Option<Vec<u8>>,
    /// `MFR_LOCATION` (PMBus command 0x9C).
    pub mfr_location: Option<Vec<u8>>,
    /// `MFR_DATE` (PMBus command 0x9D).
    pub mfr_date: Option<Vec<u8>>,
    /// `MFR_SERIAL` (PMBus command 0x9E).
    pub mfr_serial: Option<Vec<u8>>,
    /// `IC_DEVICE_ID` (PMBus command 0xAD).
    pub ic_device_id: Option<Vec<u8>>,
    /// `IC_DEVICE_REV` (PMBus command 0xAE).
    pub ic_device_rev: Option<Vec<u8>>,
}
