// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version 1 of the external IP types.

use super::SourceNatConfigError;
use crate::address::NUM_SOURCE_NAT_PORTS;
use daft::Diffable;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;

/// An IP address and port range used for source NAT, i.e., making
/// outbound network connections from guests or services.
// Note that `Deserialize` is manually implemented; if you make any changes to
// the fields of this structure, you must make them to that implementation too.
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Diffable,
)]
pub struct SourceNatConfig {
    /// The external address provided to the instance or service.
    pub ip: IpAddr,
    /// The first port used for source NAT, inclusive.
    first_port: u16,
    /// The last port used for source NAT, also inclusive.
    last_port: u16,
}

// We implement `Deserialize` manually to add validity checking on the port
// range.
impl<'de> Deserialize<'de> for SourceNatConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // The fields of `SourceNatConfigShadow` should exactly match the fields
        // of `SourceNatConfig`. We're not really using serde's remote derive,
        // but by adding the attribute we get compile-time checking that all the
        // field names and types match. (It doesn't check the _order_, but that
        // should be fine as long as we're using JSON or similar formats.)
        #[derive(Deserialize)]
        #[serde(remote = "SourceNatConfig")]
        struct SourceNatConfigShadow {
            ip: IpAddr,
            first_port: u16,
            last_port: u16,
        }

        let shadow = SourceNatConfigShadow::deserialize(deserializer)?;
        SourceNatConfig::new(shadow.ip, shadow.first_port, shadow.last_port)
            .map_err(D::Error::custom)
    }
}

impl SourceNatConfig {
    /// Construct a `SourceNatConfig` with the given port range, both inclusive.
    ///
    /// # Errors
    ///
    /// Fails if `(first_port, last_port)` is not aligned to
    /// [`NUM_SOURCE_NAT_PORTS`].
    pub fn new(
        ip: IpAddr,
        first_port: u16,
        last_port: u16,
    ) -> Result<Self, SourceNatConfigError> {
        if first_port % NUM_SOURCE_NAT_PORTS == 0
            && last_port
                .checked_sub(first_port)
                .and_then(|diff| diff.checked_add(1))
                == Some(NUM_SOURCE_NAT_PORTS)
        {
            Ok(Self { ip, first_port, last_port })
        } else {
            Err(SourceNatConfigError::UnalignedPortPair {
                first_port,
                last_port,
            })
        }
    }

    /// Get the port range.
    ///
    /// Guaranteed to be aligned to [`NUM_SOURCE_NAT_PORTS`].
    pub fn port_range(&self) -> std::ops::RangeInclusive<u16> {
        self.first_port..=self.last_port
    }

    /// Get the port range as a raw tuple; both values are inclusive.
    ///
    /// Guaranteed to be aligned to [`NUM_SOURCE_NAT_PORTS`].
    pub fn port_range_raw(&self) -> (u16, u16) {
        self.port_range().into_inner()
    }
}
