// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Names of data links created by tfportd.

use std::fmt;
use std::str::FromStr;

const PREFIX: &str = "tfport";

/// The name of a data link created by tfportd.
///
/// tfportd derives these names from a Dendrite port ID and link ID using the
/// form `tfport{port_id}_{link_id}`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TfportInterfaceName(String);

impl TfportInterfaceName {
    /// Construct the tfport interface for link 0 of a Dendrite port.
    pub fn from_port_name(
        port_name: &str,
    ) -> Result<Self, ParseTfportInterfaceNameError> {
        Self::from_port_and_link(port_name, 0)
    }

    /// Construct a tfport interface from a Dendrite port and link ID.
    pub fn from_port_and_link(
        port_name: &str,
        link_id: u8,
    ) -> Result<Self, ParseTfportInterfaceNameError> {
        if !valid_port_name(port_name) {
            return Err(ParseTfportInterfaceNameError(format!(
                "{PREFIX}{port_name}_{link_id}"
            )));
        }
        Ok(Self::from_valid_parts(port_name, link_id))
    }

    /// Construct the tfport interface for link 0 of a rear port.
    pub fn rear_port(port: u8) -> Self {
        Self::from_valid_parts(&format!("rear{port}"), 0)
    }

    /// Return the Dendrite port name encoded in this interface name.
    pub fn port_name(&self) -> &str {
        let body = self.0.strip_prefix(PREFIX).unwrap();
        body.rsplit_once('_').unwrap().0
    }

    /// Return the Dendrite link ID encoded in this interface name.
    pub fn link_id(&self) -> u8 {
        self.0.rsplit_once('_').unwrap().1.parse().unwrap()
    }

    /// Return the full tfport interface name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_valid_parts(port_name: &str, link_id: u8) -> Self {
        Self(format!("{PREFIX}{port_name}_{link_id}"))
    }
}

impl AsRef<str> for TfportInterfaceName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for TfportInterfaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TfportInterfaceName {
    type Err = ParseTfportInterfaceNameError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let Some(body) = name.strip_prefix(PREFIX) else {
            return Err(ParseTfportInterfaceNameError(name.to_owned()));
        };
        let Some((port_name, link_id)) = body.rsplit_once('_') else {
            return Err(ParseTfportInterfaceNameError(name.to_owned()));
        };
        let Ok(link_id) = link_id.parse::<u8>() else {
            return Err(ParseTfportInterfaceNameError(name.to_owned()));
        };
        if !valid_port_name(port_name) {
            return Err(ParseTfportInterfaceNameError(name.to_owned()));
        }

        let parsed = Self::from_valid_parts(port_name, link_id);
        if parsed.as_str() != name {
            return Err(ParseTfportInterfaceNameError(name.to_owned()));
        }
        Ok(parsed)
    }
}

fn valid_port_name(port_name: &str) -> bool {
    !port_name.is_empty()
        && port_name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// An invalid tfport interface name.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid tfport interface name {0:?}")]
pub struct ParseTfportInterfaceNameError(String);

#[cfg(test)]
mod tests {
    use super::TfportInterfaceName;

    #[test]
    fn constructs_front_and_rear_port_names() {
        let front = TfportInterfaceName::from_port_name("qsfp10").unwrap();
        let rear =
            TfportInterfaceName::from_port_and_link("rear31", 1).unwrap();

        assert_eq!(front.as_str(), "tfportqsfp10_0");
        assert_eq!(rear.as_str(), "tfportrear31_1");
        assert_eq!(rear.link_id(), 1);
    }

    #[test]
    fn parses_interface_name() {
        let name = "tfportqsfp10_1".parse::<TfportInterfaceName>().unwrap();

        assert_eq!(name.port_name(), "qsfp10");
        assert_eq!(name.as_str(), "tfportqsfp10_1");
    }

    #[test]
    fn rejects_invalid_interface_names() {
        for invalid in [
            "qsfp10_0",
            "tfportqsfp10",
            "tfportqsfp10_",
            "tfportqsfp10_256",
            "tfportqsfp10_00",
            "tfportqsfp_10_0",
        ] {
            assert!(
                invalid.parse::<TfportInterfaceName>().is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn rejects_invalid_port_names_during_construction() {
        for invalid in ["", "QSFP0", "qsfp_0", "qsfp0.1"] {
            assert!(TfportInterfaceName::from_port_name(invalid).is_err());
        }
    }
}
