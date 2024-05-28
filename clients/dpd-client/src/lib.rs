// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

#![allow(clippy::redundant_closure_call)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::match_single_binding)]
#![allow(clippy::clone_on_copy)]
#![allow(clippy::unnecessary_to_owned)]
// The progenitor-generated API for dpd currently incorporates a type from
// oximeter, which includes a docstring that has a doc-test in it.
// That test passes for code that lives in omicron, but fails for code imported
// by omicron.
#![allow(rustdoc::broken_intra_doc_links)]

use slog::info;
use slog::Logger;
use types::LinkCreate;
use types::LinkId;
use types::LinkSettings;
use types::PortSettings;

include!(concat!(env!("OUT_DIR"), "/dpd-client.rs"));

/// State maintained by a [`Client`].
#[derive(Clone, Debug)]
pub struct ClientState {
    /// An arbitrary tag used to identify a client, for controlling things like
    /// per-client settings.
    pub tag: String,
    /// Used for logging requests and responses.
    pub log: Logger,
}

impl Client {
    /// Ensure that a NAT entry exists, overwriting a previous conflicting entry if
    /// applicable.
    ///
    /// nat_ipv[46]_create are not idempotent (see oxidecomputer/dendrite#343),
    /// but this wrapper function is. Call this from sagas instead.
    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_nat_entry(
        &self,
        log: &Logger,
        target_ip: ipnetwork::IpNetwork,
        target_mac: types::MacAddr,
        target_first_port: u16,
        target_last_port: u16,
        target_vni: u32,
        sled_ip_address: &std::net::Ipv6Addr,
    ) -> Result<(), progenitor_client::Error<types::Error>> {
        let existing_nat = match target_ip {
            ipnetwork::IpNetwork::V4(network) => {
                self.nat_ipv4_get(&network.ip(), target_first_port).await
            }
            ipnetwork::IpNetwork::V6(network) => {
                self.nat_ipv6_get(&network.ip(), target_first_port).await
            }
        };

        // If a NAT entry already exists, but has the wrong internal
        // IP address, delete the old entry before continuing (the
        // DPD entry-creation API won't replace an existing entry).
        // If the entry exists and has the right internal IP, there's
        // no more work to do for this external IP.
        match existing_nat {
            Ok(existing) => {
                let existing = existing.into_inner();
                if existing.internal_ip != *sled_ip_address {
                    info!(log, "deleting old nat entry";
                      "target_ip" => ?target_ip);

                    match target_ip {
                        ipnetwork::IpNetwork::V4(network) => {
                            self.nat_ipv4_delete(
                                &network.ip(),
                                target_first_port,
                            )
                            .await
                        }
                        ipnetwork::IpNetwork::V6(network) => {
                            self.nat_ipv6_delete(
                                &network.ip(),
                                target_first_port,
                            )
                            .await
                        }
                    }?;
                } else {
                    info!(log,
                      "nat entry with expected internal ip exists";
                      "target_ip" => ?target_ip,
                      "existing_entry" => ?existing);

                    return Ok(());
                }
            }
            Err(e) => {
                if e.status() == Some(http::StatusCode::NOT_FOUND) {
                    info!(log, "no nat entry found for: {target_ip:#?}");
                } else {
                    return Err(e);
                }
            }
        }

        info!(log, "creating nat entry for: {target_ip:#?}");
        let nat_target = crate::types::NatTarget {
            inner_mac: target_mac,
            internal_ip: *sled_ip_address,
            vni: target_vni.into(),
        };

        match target_ip {
            ipnetwork::IpNetwork::V4(network) => {
                self.nat_ipv4_create(
                    &network.ip(),
                    target_first_port,
                    target_last_port,
                    &nat_target,
                )
                .await
            }
            ipnetwork::IpNetwork::V6(network) => {
                self.nat_ipv6_create(
                    &network.ip(),
                    target_first_port,
                    target_last_port,
                    &nat_target,
                )
                .await
            }
        }?;

        info!(log, "creation of nat entry successful for: {target_ip:#?}");

        Ok(())
    }

    /// Ensure that a NAT entry is deleted.
    ///
    /// nat_ipv[46]_delete are not idempotent (see oxidecomputer/dendrite#343),
    /// but this wrapper function is. Call this from sagas instead.
    pub async fn ensure_nat_entry_deleted(
        &self,
        log: &Logger,
        target_ip: ipnetwork::IpNetwork,
        target_first_port: u16,
    ) -> Result<(), progenitor_client::Error<types::Error>> {
        let result = match target_ip {
            ipnetwork::IpNetwork::V4(network) => {
                self.nat_ipv4_delete(&network.ip(), target_first_port).await
            }
            ipnetwork::IpNetwork::V6(network) => {
                self.nat_ipv6_delete(&network.ip(), target_first_port).await
            }
        };

        match result {
            Ok(_) => {
                info!(log, "deleted old nat entry"; "target_ip" => ?target_ip);
            }

            Err(e) => {
                if e.status() == Some(http::StatusCode::NOT_FOUND) {
                    info!(log, "no nat entry found for: {target_ip:#?}");
                } else {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Ensure that a loopback address is created.
    ///
    /// loopback_ipv[46]_create are not idempotent (see
    /// oxidecomputer/dendrite#343), but this wrapper function is. Call this
    /// from sagas instead.
    pub async fn ensure_loopback_created(
        &self,
        log: &Logger,
        address: IpAddr,
        tag: &str,
    ) -> Result<(), progenitor_client::Error<types::Error>> {
        let result = match &address {
            IpAddr::V4(a) => {
                self.loopback_ipv4_create(&types::Ipv4Entry {
                    addr: *a,
                    tag: tag.into(),
                })
                .await
            }
            IpAddr::V6(a) => {
                self.loopback_ipv6_create(&types::Ipv6Entry {
                    addr: *a,
                    tag: tag.into(),
                })
                .await
            }
        };

        match result {
            Ok(_) => {
                info!(log, "created loopback address"; "address" => ?address);
                Ok(())
            }

            Err(progenitor_client::Error::ErrorResponse(er)) => {
                match er.status() {
                    http::StatusCode::CONFLICT => {
                        info!(log, "loopback address already created"; "address" => ?address);

                        Ok(())
                    }

                    _ => Err(progenitor_client::Error::ErrorResponse(er)),
                }
            }

            Err(e) => Err(e),
        }
    }

    /// Ensure that a loopback address is deleted.
    ///
    /// loopback_ipv[46]_delete are not idempotent (see
    /// oxidecomputer/dendrite#343), but this wrapper function is. Call this
    /// from sagas instead.
    pub async fn ensure_loopback_deleted(
        &self,
        log: &Logger,
        address: IpAddr,
    ) -> Result<(), progenitor_client::Error<types::Error>> {
        let result = match &address {
            IpAddr::V4(a) => self.loopback_ipv4_delete(&a).await,
            IpAddr::V6(a) => self.loopback_ipv6_delete(&a).await,
        };

        match result {
            Ok(_) => {
                info!(log, "deleted loopback address"; "address" => ?address);
                Ok(())
            }

            Err(progenitor_client::Error::ErrorResponse(er)) => {
                match er.status() {
                    http::StatusCode::NOT_FOUND => {
                        info!(log, "loopback address already deleted"; "address" => ?address);
                        Ok(())
                    }

                    _ => Err(progenitor_client::Error::ErrorResponse(er)),
                }
            }

            Err(e) => Err(e),
        }
    }
}

// XXX delete everything below once we use the real dpd-client crate.
// https://github.com/oxidecomputer/omicron/issues/2775

use std::convert::TryFrom;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use rand::prelude::*;

// Given an IPv6 multicast address, generate the associated synthetic mac
// address
pub fn multicast_mac_addr(ip: Ipv6Addr) -> MacAddr {
    let o = ip.octets();
    MacAddr::new(0x33, 0x33, o[12], o[13], o[14], o[15])
}

/// Generate an IPv6 adddress within the provided `cidr`, using the EUI-64
/// transfrom of `mac`.
pub fn generate_ipv6_addr(cidr: Ipv6Cidr, mac: MacAddr) -> Ipv6Addr {
    let prefix: u128 = cidr.prefix.into();
    let mac = u128::from(u64::from_be_bytes(mac.to_eui64()));
    let mask = ((1u128 << cidr.prefix_len) - 1) << (128 - cidr.prefix_len);
    let ipv6 = (prefix & mask) | (mac & !mask);
    ipv6.into()
}

/// Generate a link-local IPv6 address using the EUI-64 transform of `mac`.
pub fn generate_ipv6_link_local(mac: MacAddr) -> Ipv6Addr {
    const LINK_LOCAL_PREFIX: Ipv6Cidr = Ipv6Cidr {
        prefix: Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0),
        prefix_len: 64,
    };

    generate_ipv6_addr(LINK_LOCAL_PREFIX, mac)
}

/// An IP subnet with a network prefix and prefix length.
#[derive(Debug, Eq, PartialEq, Copy, Deserialize, Serialize, Clone)]
#[serde(untagged, rename_all = "snake_case")]
pub enum Cidr {
    V4(Ipv4Cidr),
    V6(Ipv6Cidr),
}

// NOTE: We don't derive JsonSchema. That's intended so that we can use an
// untagged enum for `Cidr`, and use this method to annotate schemars output
// for client-generators (e.g., progenitor) to use in generating a better
// client.
impl JsonSchema for Cidr {
    fn schema_name() -> String {
        "Cidr".to_string()
    }

    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            subschemas: Some(Box::new(schemars::schema::SubschemaValidation {
                one_of: Some(vec![
                    label_schema("v4", gen.subschema_for::<Ipv4Cidr>()),
                    label_schema("v6", gen.subschema_for::<Ipv6Cidr>()),
                ]),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

// Insert another level of schema indirection in order to provide an
// additional title for a subschema. This allows generators to infer a better
// variant name for an "untagged" enum.
fn label_schema(
    label: &str,
    schema: schemars::schema::Schema,
) -> schemars::schema::Schema {
    schemars::schema::SchemaObject {
        metadata: Some(
            schemars::schema::Metadata {
                title: Some(label.to_string()),
                ..Default::default()
            }
            .into(),
        ),
        subschemas: Some(
            schemars::schema::SubschemaValidation {
                all_of: Some(vec![schema]),
                ..Default::default()
            }
            .into(),
        ),
        ..Default::default()
    }
    .into()
}

impl fmt::Display for Cidr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Cidr::V4(c) => write!(f, "{c}"),
            Cidr::V6(c) => write!(f, "{c}"),
        }
    }
}

impl FromStr for Cidr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(cidr) = s.parse() {
            Ok(Cidr::V4(cidr))
        } else if let Ok(cidr) = s.parse() {
            Ok(Cidr::V6(cidr))
        } else {
            Err(format!("Invalid CIDR: '{s}'"))
        }
    }
}

/// An IPv4 subnet with prefix and prefix length.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct Ipv4Cidr {
    pub prefix: Ipv4Addr,
    pub prefix_len: u8,
}

// NOTE
//
// We implement the serde and JsonSchema traits manually. This emitted schema is
// never actually used to generate the client, because we instead ask
// `progenitor` to use the "real" `common::network::Ipv4Cidr` in its place. We
// do however include _some_ schema for this type so that it shows up in the
// document. Rather than provide a regular expression for the format of an IPv4
// or v6 CIDR block, which is complicated, we just provide a human-friendly
// format name of "ipv4cidr" or "ipv6cidr".
impl<'de> serde::Deserialize<'de> for Ipv4Cidr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?.as_str().parse().map_err(
            |e: <Self as FromStr>::Err| {
                <D::Error as serde::de::Error>::custom(e)
            },
        )
    }
}

impl Serialize for Ipv4Cidr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{self}"))
    }
}

impl JsonSchema for Ipv4Cidr {
    fn schema_name() -> String {
        String::from("Ipv4Cidr")
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("An IPv4 subnet".to_string()),
                description: Some(
                    "An IPv4 subnet, including prefix and subnet mask"
                        .to_string(),
                ),
                examples: vec!["192.168.1.0/24".into()],
                ..Default::default()
            })),
            format: Some(String::from("ipv4cidr")),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            ..Default::default()
        }
        .into()
    }
}

impl Ipv4Cidr {
    /// Return `true` if the IP address is within the network.
    pub fn contains(&self, ipv4: Ipv4Addr) -> bool {
        let prefix: u32 = self.prefix.into();
        let mask = ((1u32 << self.prefix_len) - 1) << (32 - self.prefix_len);
        let addr: u32 = ipv4.into();

        (addr & mask) == prefix
    }
}

impl fmt::Display for Ipv4Cidr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.prefix, self.prefix_len)
    }
}

impl From<u64> for Ipv4Cidr {
    fn from(x: u64) -> Self {
        let prefix: u32 = (x >> 32) as u32;
        let prefix_len: u8 = (x & 0xff) as u8;
        Ipv4Cidr { prefix: prefix.into(), prefix_len }
    }
}

impl From<Ipv4Cidr> for u64 {
    fn from(x: Ipv4Cidr) -> Self {
        let prefix: u32 = x.prefix.into();
        ((prefix as u64) << 32) | (x.prefix_len as u64)
    }
}

impl From<&Ipv4Cidr> for u64 {
    fn from(x: &Ipv4Cidr) -> Self {
        (*x).into()
    }
}

impl FromStr for Ipv4Cidr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || Err(format!("Invalid IPv4 CIDR: '{s}'"));
        let Some((maybe_prefix, maybe_prefix_len)) = s.split_once('/') else {
            return err();
        };
        let Ok(prefix) = maybe_prefix.parse() else {
            return err();
        };
        let Ok(prefix_len) = maybe_prefix_len.parse() else {
            return err();
        };
        if prefix_len <= 32 {
            Ok(Ipv4Cidr { prefix, prefix_len })
        } else {
            err()
        }
    }
}

impl From<Ipv4Cidr> for Cidr {
    fn from(cidr: Ipv4Cidr) -> Self {
        Cidr::V4(cidr)
    }
}

impl TryFrom<Cidr> for Ipv4Cidr {
    type Error = &'static str;

    fn try_from(cidr: Cidr) -> Result<Self, Self::Error> {
        match cidr {
            Cidr::V4(c) => Ok(c),
            _ => Err("not a v4 CIDR"),
        }
    }
}

/// An IPv6 subnet with prefix and prefix length.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Ipv6Cidr {
    pub prefix: Ipv6Addr,
    pub prefix_len: u8,
}

// NOTE: See above about why we manually implement serialization and JsonSchema.
impl<'de> serde::Deserialize<'de> for Ipv6Cidr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?.parse().map_err(
            |e: <Self as FromStr>::Err| {
                <D::Error as serde::de::Error>::custom(e)
            },
        )
    }
}

impl Serialize for Ipv6Cidr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{self}"))
    }
}
impl JsonSchema for Ipv6Cidr {
    fn schema_name() -> String {
        String::from("Ipv6Cidr")
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("An IPv6 subnet".to_string()),
                description: Some(
                    "An IPv6 subnet, including prefix and subnet mask"
                        .to_string(),
                ),
                examples: vec!["fe80::/10".into()],
                ..Default::default()
            })),
            format: Some(String::from("ipv6cidr")),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            ..Default::default()
        }
        .into()
    }
}

impl Ipv6Cidr {
    /// Return `true` if the address is within the subnet.
    pub fn contains(&self, ipv6: Ipv6Addr) -> bool {
        let prefix: u128 = self.prefix.into();
        let mask = ((1u128 << self.prefix_len) - 1) << (128 - self.prefix_len);
        let addr: u128 = ipv6.into();

        (addr & mask) == prefix
    }
}

impl Ord for Ipv6Cidr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.prefix.cmp(&other.prefix) {
            std::cmp::Ordering::Equal => self.prefix_len.cmp(&other.prefix_len),
            o => o,
        }
    }
}

impl PartialOrd for Ipv6Cidr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Ipv6Cidr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.prefix, self.prefix_len)
    }
}

impl FromStr for Ipv6Cidr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || Err(format!("Invalid IPv6 CIDR: '{s}'"));
        let Some((maybe_prefix, maybe_prefix_len)) = s.split_once('/') else {
            return err();
        };
        let Ok(prefix) = maybe_prefix.parse() else {
            return err();
        };
        let Ok(prefix_len) = maybe_prefix_len.parse() else {
            return err();
        };
        if prefix_len <= 128 {
            Ok(Ipv6Cidr { prefix, prefix_len })
        } else {
            err()
        }
    }
}

impl TryFrom<Cidr> for Ipv6Cidr {
    type Error = &'static str;

    fn try_from(cidr: Cidr) -> Result<Self, Self::Error> {
        match cidr {
            Cidr::V6(c) => Ok(c),
            _ => Err("not a v6 CIDR"),
        }
    }
}

impl From<Ipv6Cidr> for Cidr {
    fn from(cidr: Ipv6Cidr) -> Self {
        Cidr::V6(cidr)
    }
}

/// An EUI-48 MAC address, used for layer-2 addressing.
#[derive(Copy, Deserialize, Serialize, JsonSchema, Clone, Eq, PartialEq)]
pub struct MacAddr {
    a: [u8; 6],
}

impl MacAddr {
    /// Create a new MAC address from octets in network byte order.
    pub fn new(o0: u8, o1: u8, o2: u8, o3: u8, o4: u8, o5: u8) -> MacAddr {
        MacAddr { a: [o0, o1, o2, o3, o4, o5] }
    }

    /// Create a new MAC address from a slice of bytes in network byte order.
    ///
    /// # Panics
    ///
    /// Panics if the slice is fewer than 6 octets.
    ///
    /// Note that any further octets are ignored.
    pub fn from_slice(s: &[u8]) -> MacAddr {
        MacAddr::new(s[0], s[1], s[2], s[3], s[4], s[5])
    }

    /// Convert `self` to an array of bytes in network byte order.
    pub fn to_vec(self) -> Vec<u8> {
        vec![self.a[0], self.a[1], self.a[2], self.a[3], self.a[4], self.a[5]]
    }

    /// Return `true` if `self` is the null MAC address, all zeros.
    pub fn is_null(self) -> bool {
        const EMPTY: MacAddr = MacAddr { a: [0, 0, 0, 0, 0, 0] };

        self == EMPTY
    }

    /// Generate a random MAC address.
    pub fn random() -> MacAddr {
        let mut rng = rand::thread_rng();
        let mut m = MacAddr { a: [0; 6] };
        for octet in m.a.iter_mut() {
            *octet = rng.gen();
        }
        m
    }

    /// Generate an EUI-64 ID from the mac address, following the process
    /// desribed in RFC 2464, section 4.
    pub fn to_eui64(self) -> [u8; 8] {
        [
            self.a[0] ^ 0x2,
            self.a[1],
            self.a[2],
            0xff,
            0xfe,
            self.a[3],
            self.a[4],
            self.a[5],
        ]
    }
}

impl FromStr for MacAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: Vec<&str> = s.split(':').collect();

        if v.len() != 6 {
            return Err(format!("invalid mac address: {} octets", v.len()));
        }

        let mut m = MacAddr { a: [0u8; 6] };
        for (i, octet) in v.iter().enumerate() {
            match u8::from_str_radix(octet, 16) {
                Ok(b) => m.a[i] = b,
                Err(_) => {
                    return Err(format!(
                        "invalid mac address: bad octet '{octet}'",
                    ))
                }
            }
        }
        Ok(m)
    }
}

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.a[0], self.a[1], self.a[2], self.a[3], self.a[4], self.a[5]
        )
    }
}

impl fmt::Debug for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.a[0], self.a[1], self.a[2], self.a[3], self.a[4], self.a[5]
        )
    }
}

impl From<MacAddr> for u64 {
    fn from(mac: MacAddr) -> u64 {
        ((mac.a[0] as u64) << 40)
            | ((mac.a[1] as u64) << 32)
            | ((mac.a[2] as u64) << 24)
            | ((mac.a[3] as u64) << 16)
            | ((mac.a[4] as u64) << 8)
            | (mac.a[5] as u64)
    }
}

impl From<&MacAddr> for u64 {
    fn from(mac: &MacAddr) -> u64 {
        From::from(*mac)
    }
}

impl From<u64> for MacAddr {
    fn from(x: u64) -> Self {
        MacAddr {
            a: [
                ((x >> 40) & 0xff) as u8,
                ((x >> 32) & 0xff) as u8,
                ((x >> 24) & 0xff) as u8,
                ((x >> 16) & 0xff) as u8,
                ((x >> 8) & 0xff) as u8,
                (x & 0xff) as u8,
            ],
        }
    }
}

impl Eq for PortSettings {}

impl PartialEq for PortSettings {
    fn eq(&self, other: &Self) -> bool {
        self.links == other.links
    }
}

impl Eq for LinkSettings {}

impl PartialEq for LinkSettings {
    fn eq(&self, other: &Self) -> bool {
        self.addrs == other.addrs && self.params == other.params
    }
}

impl Eq for LinkCreate {}

impl PartialEq for LinkCreate {
    fn eq(&self, other: &Self) -> bool {
        self.autoneg == other.autoneg
            && self.fec == other.fec
            && self.kr == other.kr
            && self.lane == other.lane
            && self.speed == other.speed
    }
}

impl Eq for LinkId {}

impl PartialEq for LinkId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
