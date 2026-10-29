// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::component_vpd::{
    Barcode, ComponentVpd, Mpn1Barcode, OxideBarcode, PmbusDevice, SledFanTray,
    Tmp11x,
};
use gateway_messages::vpd as gw;
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(thiserror::Error, Debug, Eq, PartialEq)]
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
pub enum InvalidComponentVpd {
    #[error(transparent)]
    Barcode(#[from] ParseBarcodeError),
    #[error(transparent)]
    SledFanTray(#[from] InvalidAssemblyBarcode),
}

#[derive(thiserror::Error, Debug)]
#[error("invalid fan assembly {which_barcode} barcode")]
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
            Mpn1Barcode::MPN1 => Mpn1Barcode::from_parts(parts).map(Self::Mpn1),
            version => Err(ParseBarcodeError::UnknownVersion {
                version: version.to_string(),
                expected: Self::EXPECTED_VERSION,
            }),
        }
    }
}

impl fmt::Display for Barcode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Oxide(oxide) => oxide.fmt(f),
            Self::Mpn1(mpn1) => mpn1.fmt(f),
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
                // V1 does not include the hyphen after the first three digits
                // of the part number, so we need to insert it.
                let pn_chars = part_number.chars();
                let mut part_number = String::with_capacity(11);
                for (i, ch) in pn_chars.enumerate() {
                    if i == 3 && ch != '-' {
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
                expected: Self::MPN1,
            })?;
        if version != Self::MPN1 {
            return Err(ParseBarcodeError::UnknownVersion {
                version: version.to_string(),
                expected: Self::MPN1,
            });
        }
        Self::from_parts(parts)
    }
}

impl fmt::Display for OxideBarcode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { part_number, revision, serial_number } = self;
        write!(f, "0XV2:{part_number}:{revision:03}:{serial_number}")
    }
}

impl Mpn1Barcode {
    const MPN1: &str = "MPN1";

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

impl fmt::Display for Mpn1Barcode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { manufacturer, part_number, revision, serial_number } = self;
        write!(
            f,
            "{}:{manufacturer}:{part_number}:{revision:}:{serial_number}",
            Self::MPN1
        )
    }
}

impl TryFrom<gw::Vpd> for ComponentVpd {
    type Error = InvalidComponentVpd;

    fn try_from(value: gw::Vpd) -> Result<Self, Self::Error> {
        match value {
            gw::Vpd::Pmbus(vpd) => Ok(Self::Pmbus(vpd.into())),
            gw::Vpd::Barcode(barcode) => Ok(match barcode.try_into()? {
                Barcode::Oxide(barcode) => Self::OxideBarcode(barcode),
                Barcode::Mpn1(barcode) => Self::Mpn1Barcode(barcode),
            }),
            gw::Vpd::SledFanTray(vpd) => Ok(Self::SledFanTray(vpd.try_into()?)),
            gw::Vpd::Tmp11x(vpd) => Ok(Self::Tmp11x(vpd.into())),
        }
    }
}

impl From<gw::PmbusVpd> for PmbusDevice {
    fn from(value: gw::PmbusVpd) -> Self {
        let gw::PmbusVpd {
            mfr_id,
            mfr_model,
            mfr_revision,
            mfr_location,
            mfr_date,
            mfr_serial,
            ic_device_id,
            ic_device_rev,
        } = value;
        Self {
            mfr_id: mfr_id.as_bytes().map(<[u8]>::to_vec),
            mfr_model: mfr_model.as_bytes().map(<[u8]>::to_vec),
            mfr_revision: mfr_revision.as_bytes().map(<[u8]>::to_vec),
            mfr_location: mfr_location.as_bytes().map(<[u8]>::to_vec),
            mfr_date: mfr_date.as_bytes().map(<[u8]>::to_vec),
            mfr_serial: mfr_serial.as_bytes().map(<[u8]>::to_vec),
            ic_device_id: ic_device_id.as_bytes().map(<[u8]>::to_vec),
            ic_device_rev: ic_device_rev.as_bytes().map(<[u8]>::to_vec),
        }
    }
}

impl From<gw::Tmp11xVpd> for Tmp11x {
    fn from(value: gw::Tmp11xVpd) -> Self {
        let gw::Tmp11xVpd { id, eeprom1, eeprom2, eeprom3 } = value;
        Self { device_id: id, eeprom1, eeprom2, eeprom3 }
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

#[cfg(test)]
mod tests {
    use super::*;

    // Most of the tests for barcode parsing in this module are lifted from
    // the ones in Hubris' `oxide-barcode` crate.
    // https://github.com/oxidecomputer/hubris/blob/0d1ba0453a5d80470f07ea9949eb82bc3c291e01/lib/oxide-barcode/src/lib.rs#L363-L614

    #[track_caller]
    fn check_parse_oxide(input: &str, expected: OxideBarcode) {
        let parsed = dbg!(input).parse::<OxideBarcode>();
        dbg!(&parsed);

        assert_eq!(
            parsed.as_ref(),
            Ok(dbg!(&expected)),
            "parsing Oxide barcodestring: {input}"
        );

        let formatted = parsed.as_ref().unwrap().to_string();
        assert_eq!(
            parsed,
            dbg!(formatted).parse::<OxideBarcode>(),
            "parsed Oxide barcode for string {input} should round-trip through \
             `fmt::Display`",
        );

        // We accept barcode strings that start with both leading zero and
        // leading capital-O. Permute our input from one of these to the other
        // to make sure both forms parse equivalently.
        let mut copy = input.to_owned();
        match copy.as_bytes()[0] {
            b'0' => copy.replace_range(0..1, "O"),
            b'O' => copy.replace_range(0..1, "0"),
            c => panic!("unexpected leading character: {}", c as char),
        }

        let parsed = dbg!(&copy).parse::<OxideBarcode>();

        assert_eq!(
            dbg!(&parsed).as_ref(),
            Ok(&expected),
            "parsing Oxide barcode string: {copy}"
        );

        let formatted = parsed.as_ref().unwrap().to_string();
        assert_eq!(
            parsed,
            dbg!(formatted).parse::<OxideBarcode>(),
            "parsed Oxide barcode for string {copy} should round-trip through \
             `fmt::Display`",
        );
    }

    #[test]
    fn parse_oxv1() {
        check_parse_oxide(
            "0XV1:1230000456:023:TST01234567",
            OxideBarcode {
                part_number: "123-0000456".to_owned(),
                revision: 23,
                serial_number: "TST01234567".to_owned(),
            },
        );
    }

    #[test]
    fn parse_oxv2() {
        check_parse_oxide(
            "0XV2:123-0000456:023:TST01234567",
            OxideBarcode {
                part_number: "123-0000456".to_owned(),
                revision: 23,
                serial_number: "TST01234567".to_owned(),
            },
        );
    }

    #[test]
    fn parse_oxv2_shorter_serial() {
        check_parse_oxide(
            "0XV2:123-0000456:023:TST0123456",
            OxideBarcode {
                part_number: "123-0000456".to_owned(),
                revision: 23,
                serial_number: "TST0123456".to_owned(),
            },
        );
    }

    #[test]
    fn parse_oxv2_shorter_part() {
        check_parse_oxide(
            "0XV2:123-000045:023:TST01234567",
            OxideBarcode {
                part_number: "123-000045".to_owned(),
                revision: 23,
                serial_number: "TST01234567".to_owned(),
            },
        );
    }

    #[track_caller]
    fn check_parse_mpn1(input: &str, expected: Mpn1Barcode) {
        let parsed = dbg!(input).parse::<Mpn1Barcode>();
        dbg!(&parsed);

        assert_eq!(
            parsed.as_ref(),
            Ok(dbg!(&expected)),
            "parsing MPN1 identity {input}"
        );
        let formatted = parsed.as_ref().unwrap().to_string();
        assert_eq!(
            dbg!(formatted).parse::<Mpn1Barcode>().as_ref(),
            Ok(&expected),
            "parsed MPN1 barcode {input} should round-trip through \
             `fmt::Display`",
        );
    }

    #[test]
    fn parse_mpn1() {
        check_parse_mpn1(
            "MPN1:ABC:ASDF-1000:032:123456789",
            Mpn1Barcode {
                manufacturer: "ABC".to_owned(),
                part_number: "ASDF-1000".to_owned(),
                revision: "032".to_owned(),
                serial_number: "123456789".to_owned(),
            },
        );
    }

    #[test]
    fn parse_mpn1_empty() {
        check_parse_mpn1(
            "MPN1::::",
            Mpn1Barcode {
                manufacturer: String::new(),
                part_number: String::new(),
                revision: String::new(),
                serial_number: String::new(),
            },
        );
    }

    #[test]
    fn parse_mpn1_no_mpn_rev() {
        check_parse_mpn1(
            "MPN1:XYZ:::12345ABCD",
            Mpn1Barcode {
                manufacturer: "XYZ".to_owned(),
                part_number: String::new(),
                revision: String::new(),
                serial_number: "12345ABCD".to_owned(),
            },
        );
    }

    #[test]
    fn parse_mpn1_no_serial() {
        check_parse_mpn1(
            "MPN1:XYZ:1234ABC:420:",
            Mpn1Barcode {
                manufacturer: "XYZ".to_owned(),
                part_number: "1234ABC".to_owned(),
                revision: "420".to_owned(),
                serial_number: String::new(),
            },
        );
    }

    #[test]
    fn parse_mpn1_from_andy() {
        check_parse_mpn1(
            "MPN1:SYD:9CRA0848P8G012:C:WWYY1SSS",
            Mpn1Barcode {
                manufacturer: "SYD".to_owned(),
                part_number: "9CRA0848P8G012".to_owned(),
                revision: "C".to_owned(),
                serial_number: "WWYY1SSS".to_owned(),
            },
        );
    }
}
