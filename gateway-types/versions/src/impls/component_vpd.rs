// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::component_vpd::{
    Barcode, Mpn1Barcode, OxideBarcode, SledFanTray,
};
use gateway_messages::vpd as gw;
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
pub enum ParseBarcodeError {
    #[error("barcode received from SP was not UTF-8")]
    // TODO(eliza): t'would be nice to format the invalid bytes in a nice-ish
    // way so you can see what the garbled message was...
    NotUtf8,
    #[error("missing barcode version (expected {expected})")]
    MissingVersion { expected: &'static str },
    #[error("missing part number")]
    MissingPartNumber,
    #[error("missing revision")]
    MissingRevision,
    #[error("missing serial number")]
    MissingSerialNumber,
    #[error("missing MPN1 manufacturer string")]
    MissingManufacturer,
    #[error("unexpected fields")]
    UnexpectedFields,
    #[error("unknown barcode version {version:?} (expected {expected})")]
    UnknownVersion { version: String, expected: &'static str },
    #[error("invalid Oxide barcode revision {revision}")]
    BadRevision {
        revision: String,
        #[source]
        error: ParseIntError,
    },
}

#[derive(thiserror::Error, Debug)]
#[error("invalid {which_barcode} barcode")]
pub struct InvalidAssemblyBarcode {
    which_barcode: &'static str,
    #[source]
    error: ParseBarcodeError,
}

impl InvalidAssemblyBarcode {
    fn mk(which_barcode: &'static str) -> impl Fn(ParseBarcodeError) -> Self {
        move |error| Self { which_barcode, error }
    }
}

impl Barcode {
    const EXPECTED_VERSION: &str =
        "one of '0XV1', 'OXV1', '0XV2', 'OXV2', or 'MPN1'";

    /// Borrows the manufacturer string for this barcode, if this is an `MPN1`
    /// barcode. Otherwise, if this is an Oxide-issued barcode, this returns
    /// `None`.
    pub fn manufacturer(&self) -> Option<&str> {
        match self {
            Self::Mpn1(Mpn1Barcode { manufacturer, .. }) => {
                Some(manufacturer.as_str())
            }
            _ => None,
        }
    }

    /// Borrows the serial number portion of this barcode.
    pub fn serial_number(&self) -> &str {
        match self {
            Self::Mpn1(Mpn1Barcode { serial_number, .. }) => {
                serial_number.as_str()
            }
            Self::Oxide(OxideBarcode { serial_number, .. }) => {
                serial_number.as_str()
            }
        }
    }

    /// Borrows the part number portion of this barcode.
    pub fn part_number(&self) -> &str {
        match self {
            Self::Mpn1(Mpn1Barcode { part_number, .. }) => part_number.as_str(),
            Self::Oxide(OxideBarcode { part_number, .. }) => {
                part_number.as_str()
            }
        }
    }
}

impl TryFrom<gw::Barcode> for Barcode {
    type Error = ParseBarcodeError;
    fn try_from(value: gw::Barcode) -> Result<Self, Self::Error> {
        value.as_str().ok_or(ParseBarcodeError::NotUtf8)?.parse()
    }
}

impl FromStr for Barcode {
    type Err = ParseBarcodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(':');
        let version =
            parts.next().ok_or(ParseBarcodeError::MissingVersion {
                expected: Self::EXPECTED_VERSION,
            })?;
        match version {
            "0XV1" | "OXV1" | "0XV2" | "OXV2" => {
                OxideBarcode::from_parts(version, parts).map(Self::Oxide)
            }
            "MPN1" => Mpn1Barcode::from_parts(parts).map(Self::Mpn1),
            version => Err(ParseBarcodeError::UnknownVersion {
                version: version.to_string(),
                expected: Self::EXPECTED_VERSION,
            }),
        }
    }
}

impl TryFrom<gw::Barcode> for OxideBarcode {
    type Error = ParseBarcodeError;
    fn try_from(value: gw::Barcode) -> Result<Self, Self::Error> {
        value.as_str().ok_or(ParseBarcodeError::NotUtf8)?.parse()
    }
}

impl FromStr for OxideBarcode {
    type Err = ParseBarcodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(':');
        let version =
            parts.next().ok_or(ParseBarcodeError::MissingVersion {
                expected: Self::EXPECTED_VERSION,
            })?;
        Self::from_parts(version, parts)
    }
}

impl OxideBarcode {
    const EXPECTED_VERSION: &str = "one of '0XV1', 'OXV1', '0XV2', or 'OXV2'";

    fn from_parts<'parts>(
        version: &'parts str,
        mut parts: impl Iterator<Item = &'parts str> + 'parts,
    ) -> Result<Self, ParseBarcodeError> {
        let part_number =
            parts.next().ok_or(ParseBarcodeError::MissingPartNumber)?;
        let revision =
            parts.next().ok_or(ParseBarcodeError::MissingRevision)?;
        let serial_number =
            parts.next().ok_or(ParseBarcodeError::MissingSerialNumber)?;
        if parts.next().is_some() {
            return Err(ParseBarcodeError::UnexpectedFields);
        }
        let part_number = match version {
            "0XV1" | "OXV1" => {
                // V1 does not include the hyphen in the part number when stored
                // in an EEPROM, so we need to insert it.
                let pn_chars = part_number.chars();
                let mut part_number = String::with_capacity(11);
                for (i, ch) in pn_chars.enumerate() {
                    if i == 4 && ch != '-' {
                        part_number.push('-');
                    }
                    part_number.push(ch);
                }
                part_number
            }
            "0XV2" | "OXV2" => part_number.to_string(),
            _ => {
                return Err(ParseBarcodeError::UnknownVersion {
                    version: version.to_string(),
                    expected: Self::EXPECTED_VERSION,
                });
            }
        };
        let revision = revision.parse().map_err(|error| {
            ParseBarcodeError::BadRevision {
                revision: revision.to_string(),
                error,
            }
        })?;

        Ok(Self {
            part_number,
            revision,
            serial_number: serial_number.to_owned(),
        })
    }
}

impl TryFrom<gw::Barcode> for Mpn1Barcode {
    type Error = ParseBarcodeError;
    fn try_from(value: gw::Barcode) -> Result<Self, Self::Error> {
        value.as_str().ok_or(ParseBarcodeError::NotUtf8)?.parse()
    }
}

impl FromStr for Mpn1Barcode {
    type Err = ParseBarcodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(':');
        let version =
            parts.next().ok_or(ParseBarcodeError::MissingVersion {
                expected: Self::EXPECTED_VERSION,
            })?;
        if version != Self::EXPECTED_VERSION {
            return Err(ParseBarcodeError::UnknownVersion {
                version: version.to_string(),
                expected: Self::EXPECTED_VERSION,
            });
        }
        Self::from_parts(parts)
    }
}

impl Mpn1Barcode {
    const EXPECTED_VERSION: &str = "MPN1";

    fn from_parts<'parts>(
        mut parts: impl Iterator<Item = &'parts str> + 'parts,
    ) -> Result<Self, ParseBarcodeError> {
        let manufacturer =
            parts.next().ok_or(ParseBarcodeError::MissingManufacturer)?;
        let part_number =
            parts.next().ok_or(ParseBarcodeError::MissingPartNumber)?;
        let revision =
            parts.next().ok_or(ParseBarcodeError::MissingRevision)?;
        let serial_number =
            parts.next().ok_or(ParseBarcodeError::MissingSerialNumber)?;
        if parts.next().is_some() {
            return Err(ParseBarcodeError::UnexpectedFields);
        }

        Ok(Self {
            manufacturer: manufacturer.to_owned(),
            part_number: part_number.to_owned(),
            revision: revision.to_owned(),
            serial_number: serial_number.to_owned(),
        })
    }
}

impl TryFrom<gw::SledFanTrayVpd> for SledFanTray {
    type Error = InvalidAssemblyBarcode;
    fn try_from(value: gw::SledFanTrayVpd) -> Result<Self, Self::Error> {
        let gw::SledFanTrayVpd {
            identity,
            vpd_board_identity,
            fans: [fan0, fan1, fan2],
        } = value;
        Ok(Self {
            identity: identity
                .try_into()
                .map_err(InvalidAssemblyBarcode::mk("fan tray"))?,
            vpd_board_identity: vpd_board_identity
                .try_into()
                .map_err(InvalidAssemblyBarcode::mk("VPD board"))?,
            fan0: fan0
                .try_into()
                .map_err(InvalidAssemblyBarcode::mk("fan 0"))?,
            fan1: fan1
                .try_into()
                .map_err(InvalidAssemblyBarcode::mk("fan 1"))?,
            fan2: fan2
                .try_into()
                .map_err(InvalidAssemblyBarcode::mk("fan 2"))?,
        })
    }
}
