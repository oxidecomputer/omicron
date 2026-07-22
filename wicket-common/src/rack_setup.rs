// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use owo_colors::OwoColorize;
use owo_colors::Style;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use sled_hardware_types::Baseboard;
use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use tufaceous_artifact_v2::ArtifactHash;
use wicketd_commission_types::rack_setup::AllowedSourceIps;
use wicketd_commission_types::rack_setup::BgpAuthKeyId;
use wicketd_commission_types::rack_setup::IpRange;
use wicketd_commission_types::rack_setup::UserSpecifiedRackNetworkConfig;

use crate::inventory::SpIdentifier;

/// The subset of `RackInitializeRequest` that the user fills in as clear text
/// (e.g., via an uploaded config file).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct CurrentRssUserConfigInsensitive {
    pub bootstrap_sleds: IdOrdMap<BootstrapSledDescription>,
    pub ntp_servers: Vec<String>,
    pub dns_servers: Vec<IpAddr>,
    pub internal_services_ip_pool_ranges: Vec<IpRange>,
    pub external_dns_ips: Vec<IpAddr>,
    pub external_dns_zone_name: String,
    pub rack_network_config: Option<UserSpecifiedRackNetworkConfig>,
    pub allowed_source_ips: Option<AllowedSourceIps>,
    /// Enable the fleet-wide jumbo-frames opt-in. Operators can also toggle
    /// this at runtime via the Nexus API after handoff.
    #[serde(default)]
    pub external_jumbo_frames_opt_in_enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BootstrapSledDescription {
    pub id: SpIdentifier,
    pub baseboard: Baseboard,
    /// The sled's bootstrap address, if the host is on and we've discovered it
    /// on the bootstrap network.
    pub bootstrap_ip: Option<Ipv6Addr>,
}

impl IdOrdItem for BootstrapSledDescription {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

/// Displayer for a slice of `fmt::Display` structs.
///
/// This is useful when `.join` is unavailable, e.g. if T doesn't implement
/// `Borrow<str>`.
#[derive(Clone, Copy, Debug)]
pub struct DisplaySlice<'a, T>(pub &'a [T]);

impl<T: fmt::Display> fmt::Display for DisplaySlice<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, id) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{id}")?;
        }
        Ok(())
    }
}

/// Describes the actual authentication key to use with a BGP peer.
///
/// Currently, only TCP-MD5 authentication is supported.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BgpAuthKey {
    /// TCP-MD5 authentication.
    TcpMd5 {
        /// The pre-shared key.
        key: String,
    },
}

impl BgpAuthKey {
    /// Returns information about the key that is safe to display in the UI.
    pub fn info(&self) -> BgpAuthKeyInfo {
        match self {
            BgpAuthKey::TcpMd5 { key } => {
                let sha256 =
                    ArtifactHash(Sha256::digest(key.as_bytes()).into());
                BgpAuthKeyInfo::TcpMd5 { sha256 }
            }
        }
    }
}

// Ensure that the key is not displayed in debug output.
impl fmt::Debug for BgpAuthKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BgpAuthKey::TcpMd5 { key: _ } => {
                f.debug_struct("TcpMd5").field("key", &"********").finish()
            }
        }
    }
}

/// Describes insensitive information about a BGP authentication key.
///
/// This information is considered okay to display in the UI.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    JsonSchema,
    PartialOrd,
    Ord,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BgpAuthKeyInfo {
    /// TCP-MD5 authentication.
    TcpMd5 {
        /// A SHA-256 digest of the key.
        // XXX we use ArtifactHash for convenience, this should be its own kind
        // of hash probably.
        sha256: ArtifactHash,
    },
}

impl BgpAuthKeyInfo {
    pub fn to_string_styled(&self, label_style: Style) -> String {
        match self {
            BgpAuthKeyInfo::TcpMd5 { sha256 } => {
                format!(
                    "{} (SHA-256: {})",
                    "TCP-MD5".style(label_style),
                    sha256.style(label_style)
                )
            }
        }
    }
}

/// Returns information about BGP keys for rack network setup.
///
/// This is part of a wicketd response, but is returned here because our
/// tooling turns BTreeMaps into HashMaps. So we use a `replace` directive
/// instead.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    JsonSchema,
    PartialOrd,
    Ord,
)]
pub struct GetBgpAuthKeyInfoResponse {
    /// Information about the requested keys.
    ///
    /// None indicates that the key ID has not been set yet. An error indicates
    /// that the key was not specified in the rack setup config.
    pub data: BTreeMap<BgpAuthKeyId, BgpAuthKeyStatus>,
}

/// The status of a BGP authentication key.
///
/// This is part of a wicketd response, but is returned here because our
/// tooling turns BTreeMaps into HashMaps. So we use a `replace` directive
/// instead.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    JsonSchema,
    PartialOrd,
    Ord,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BgpAuthKeyStatus {
    /// The key was specified but hasn't been set yet.
    Unset,

    /// The key has been set.
    Set {
        /// Information about the key.
        info: BgpAuthKeyInfo,
    },
}

impl BgpAuthKeyStatus {
    /// Returns true if the key is set.
    #[inline]
    pub fn is_set(&self) -> bool {
        matches!(self, BgpAuthKeyStatus::Set { .. })
    }

    /// Returns true if the key is unset.
    #[inline]
    pub fn is_unset(&self) -> bool {
        matches!(self, BgpAuthKeyStatus::Unset)
    }
}
