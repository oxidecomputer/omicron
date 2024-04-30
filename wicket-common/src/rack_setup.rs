// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

pub use gateway_client::types::SpIdentifier as GatewaySpIdentifier;
pub use gateway_client::types::SpType as GatewaySpType;
use ipnetwork::IpNetwork;
use omicron_common::address;
use omicron_common::api::external::Name;
use omicron_common::api::external::SwitchLocation;
use omicron_common::api::internal::shared::BgpConfig;
use omicron_common::api::internal::shared::BgpPeerConfig;
use omicron_common::api::internal::shared::PortFec;
use omicron_common::api::internal::shared::PortSpeed;
use omicron_common::api::internal::shared::RouteConfig;
use omicron_common::update::ArtifactHash;
use owo_colors::OwoColorize;
use owo_colors::Style;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use sled_hardware_types::Baseboard;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::str::FromStr;

/// The subset of `RackInitializeRequest` that the user fills in as clear text
/// (e.g., via an uploaded config file).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct CurrentRssUserConfigInsensitive {
    pub bootstrap_sleds: BTreeSet<BootstrapSledDescription>,
    pub ntp_servers: Vec<String>,
    pub dns_servers: Vec<IpAddr>,
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,
    pub external_dns_ips: Vec<IpAddr>,
    pub external_dns_zone_name: String,
    pub rack_network_config: Option<UserSpecifiedRackNetworkConfig>,
}

/// The portion of `CurrentRssUserConfig` that can be posted in one shot; it is
/// provided by the wicket user uploading a TOML file, currently.
///
/// This is the "write" version of [`CurrentRssUserConfigInsensitive`], with
/// some different fields.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct PutRssUserConfigInsensitive {
    /// List of slot numbers only.
    ///
    /// `wicketd` will map this back to sleds with the correct `SpIdentifier`
    /// based on the `bootstrap_sleds` it provides in
    /// `CurrentRssUserConfigInsensitive`.
    pub bootstrap_sleds: BTreeSet<u32>,
    pub ntp_servers: Vec<String>,
    pub dns_servers: Vec<IpAddr>,
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,
    pub external_dns_ips: Vec<IpAddr>,
    pub external_dns_zone_name: String,
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct BootstrapSledDescription {
    // TODO: We currently use gateway-client's SpIdentifier here, not our own,
    // to avoid wicketd-client getting an "SpIdentifier2". We really do need to
    // unify this type once and forever.
    pub id: GatewaySpIdentifier,
    pub baseboard: Baseboard,
    /// The sled's bootstrap address, if the host is on and we've discovered it
    /// on the bootstrap network.
    pub bootstrap_ip: Option<Ipv6Addr>,
}

/// User-specified parts of
/// [`RackNetworkConfig`](omicron_common::api::internal::shared::RackNetworkConfig).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct UserSpecifiedRackNetworkConfig {
    pub infra_ip_first: Ipv4Addr,
    pub infra_ip_last: Ipv4Addr,
    #[serde(rename = "port")]
    pub ports: BTreeMap<String, UserSpecifiedPortConfig>,
    pub bgp: Vec<BgpConfig>,
}

/// User-specified version of [`PortConfigV1`].
///
/// All of [`PortConfigV1`] is user-specified. But we expect the port name to
/// be a key, rather than a field as in [`PortConfigV1`]. So this has all of
/// the fields other than the port name.
///
/// [`PortConfigV1`]: omicron_common::api::internal::shared::PortConfigV1
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedPortConfig {
    pub routes: Vec<RouteConfig>,
    pub addresses: Vec<IpNetwork>,
    pub switch: SwitchLocation,
    pub uplink_port_speed: PortSpeed,
    pub uplink_port_fec: PortFec,
    pub autoneg: bool,
    #[serde(default)]
    pub bgp_peers: Vec<UserSpecifiedBgpPeerConfig>,
}

/// User-specified version of [`BgpPeerConfig`].
///
/// This is similar to [`BgpPeerConfig`], except it doesn't have the sensitive
/// `md5_auth_key` parameter, instead requiring that the user provide the key
/// separately.
///
/// [`BgpPeerConfig`]: omicron_common::api::internal::shared::BgpPeerConfig
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedBgpPeerConfig {
    /// The autonomous sysetm number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer.
    pub addr: Ipv4Addr,
    /// How long to keep a session alive without a keepalive in seconds.
    /// Defaults to 6 seconds.
    pub hold_time: Option<u64>,
    /// How long to keep a peer in idle after a state machine reset in seconds.
    /// Defaults to 3 seconds.
    pub idle_hold_time: Option<u64>,
    /// How long to delay sending open messages to a peer in seconds. Defaults
    /// to 0.
    pub delay_open: Option<u64>,
    /// The interval in seconds between peer connection retry attempts.
    /// Defaults to 3 seconds.
    pub connect_retry: Option<u64>,
    /// The interval to send keepalive messages at, in seconds. Defaults to 2
    /// seconds.
    pub keepalive: Option<u64>,
    /// Require that a peer has a specified ASN.
    #[serde(default)]
    pub remote_asn: Option<u32>,
    /// Require messages from a peer have a minimum IP time to live field.
    #[serde(default)]
    pub min_ttl: Option<u8>,
    /// The key identifier for authentication to use with the peer.
    #[serde(default)]
    pub auth_key_id: Option<BgpAuthKeyId>,
    /// Apply the provided multi-exit discriminator (MED) updates sent to the peer.
    #[serde(default)]
    pub multi_exit_discriminator: Option<u32>,
    /// Include the provided communities in updates sent to the peer.
    #[serde(default)]
    pub communities: Vec<u32>,
    /// Apply a local preference to routes received from this peer.
    #[serde(default)]
    pub local_pref: Option<u32>,
    /// Enforce that the first AS in paths received from this peer is the peer's AS.
    #[serde(default)]
    pub enforce_first_as: bool,
}

impl UserSpecifiedBgpPeerConfig {
    pub fn hold_time(&self) -> u64 {
        self.hold_time.unwrap_or(BgpPeerConfig::DEFAULT_HOLD_TIME)
    }

    pub fn idle_hold_time(&self) -> u64 {
        self.idle_hold_time.unwrap_or(BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME)
    }

    pub fn delay_open(&self) -> u64 {
        self.delay_open.unwrap_or(BgpPeerConfig::DEFAULT_DELAY_OPEN)
    }

    pub fn connect_retry(&self) -> u64 {
        self.connect_retry.unwrap_or(BgpPeerConfig::DEFAULT_CONNECT_RETRY)
    }

    pub fn keepalive(&self) -> u64 {
        self.keepalive.unwrap_or(BgpPeerConfig::DEFAULT_KEEPALIVE)
    }
}

/// The key identifier for authentication to use with a BGP peer.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct BgpAuthKeyId(Name);

impl BgpAuthKeyId {
    /// Returns the key ID string.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns the key ID as a `Name`.
    pub fn as_name(&self) -> &Name {
        &self.0
    }
}

impl FromStr for BgpAuthKeyId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl fmt::Display for BgpAuthKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
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

/// Describes insensitive information about a BGP authentication key.
///
/// This information is considered okay to display in the UI.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
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
