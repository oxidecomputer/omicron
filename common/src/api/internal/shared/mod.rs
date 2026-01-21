// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types shared between Nexus and Sled Agent.

use super::nexus::HostIdentifier;
use crate::{
    api::external::{self, Vni},
    disk::DatasetName,
    zpool_name::ZpoolName,
};
use daft::Diffable;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ExternalZpoolUuid;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    net::{IpAddr, Ipv6Addr},
    str::FromStr,
};
use strum::EnumCount;
use uuid::Uuid;

pub mod external_ip;
pub mod network_interface;
pub mod rack_init;

// Re-export latest version of all NIC-related types.
pub use network_interface::NetworkInterfaceKind;
pub use network_interface::*;

// Re-export latest version of the external IP types.
pub use external_ip::ExternalIpConfig;
pub use external_ip::ExternalIpConfigBuilder;
pub use external_ip::ExternalIps;
pub use external_ip::ExternalIpv4Config;
pub use external_ip::ExternalIpv6Config;
pub use external_ip::SourceNatConfigError;
pub use external_ip::SourceNatConfigGeneric;
pub use external_ip::SourceNatConfigV4;
pub use external_ip::SourceNatConfigV6;

// Re-export latest version of rack_init types.
pub use rack_init::BfdPeerConfig;
pub use rack_init::BgpConfig;
pub use rack_init::BgpPeerConfig;
pub use rack_init::ExternalPortDiscovery;
pub use rack_init::HostPortConfig;
pub use rack_init::LldpAdminStatus;
pub use rack_init::LldpPortConfig;
pub use rack_init::ParseLldpAdminStatusError;
pub use rack_init::ParseSwitchLocationError;
pub use rack_init::PortConfig;
pub use rack_init::PortFec;
pub use rack_init::PortSpeed;
pub use rack_init::RackNetworkConfig;
pub use rack_init::RouteConfig;
pub use rack_init::SwitchLocation;
pub use rack_init::SwitchPorts;
pub use rack_init::TxEqConfig;
pub use rack_init::UplinkAddressConfig;
pub use rack_init::UplinkAddressConfigError;

/// Description of source IPs allowed to reach rack services.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "allow", content = "ips")]
pub enum AllowedSourceIps {
    /// Allow traffic from any external IP address.
    Any,
    /// Restrict access to a specific set of source IP addresses or subnets.
    ///
    /// All others are prevented from reaching rack services.
    List(IpAllowList),
}

impl TryFrom<Vec<IpNet>> for AllowedSourceIps {
    type Error = &'static str;
    fn try_from(list: Vec<IpNet>) -> Result<Self, Self::Error> {
        IpAllowList::try_from(list).map(Self::List)
    }
}

impl TryFrom<&[ipnetwork::IpNetwork]> for AllowedSourceIps {
    type Error = &'static str;
    fn try_from(list: &[ipnetwork::IpNetwork]) -> Result<Self, Self::Error> {
        IpAllowList::try_from(list).map(Self::List)
    }
}

/// A non-empty allowlist of IP subnets.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(try_from = "Vec<IpNet>", into = "Vec<IpNet>")]
#[schemars(transparent)]
pub struct IpAllowList(Vec<IpNet>);

impl IpAllowList {
    /// Return the entries of the list as a slice.
    pub fn as_slice(&self) -> &[IpNet] {
        &self.0
    }

    /// Return an iterator over the entries of the list.
    pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
        self.0.iter()
    }

    /// Consume the list into an iterator.
    pub fn into_iter(self) -> impl Iterator<Item = IpNet> {
        self.0.into_iter()
    }

    /// Return the number of entries in the allowlist.
    ///
    /// Note that this is always >= 1, though we return a usize for simplicity.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<IpAllowList> for Vec<IpNet> {
    fn from(list: IpAllowList) -> Self {
        list.0
    }
}

impl TryFrom<Vec<IpNet>> for IpAllowList {
    type Error = &'static str;
    fn try_from(list: Vec<IpNet>) -> Result<Self, Self::Error> {
        if list.is_empty() {
            return Err("IP allowlist must not be empty");
        }
        Ok(Self(list))
    }
}

impl TryFrom<&[ipnetwork::IpNetwork]> for IpAllowList {
    type Error = &'static str;

    fn try_from(list: &[ipnetwork::IpNetwork]) -> Result<Self, Self::Error> {
        if list.is_empty() {
            return Err("IP allowlist must not be empty");
        }
        Ok(Self(list.into_iter().map(|net| (*net).into()).collect()))
    }
}

/// A VPC route resolved into a concrete target.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ResolvedVpcRoute {
    pub dest: IpNet,
    pub target: RouterTarget,
}

/// VPC firewall rule after object name resolution has been performed by Nexus
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ResolvedVpcFirewallRule {
    pub status: external::VpcFirewallRuleStatus,
    pub direction: external::VpcFirewallRuleDirection,
    pub targets: Vec<NetworkInterface>,
    pub filter_hosts: Option<HashSet<HostIdentifier>>,
    pub filter_ports: Option<Vec<external::L4PortRange>>,
    pub filter_protocols: Option<Vec<external::VpcFirewallRuleProtocol>>,
    pub action: external::VpcFirewallRuleAction,
    pub priority: external::VpcFirewallRulePriority,
}

/// A mapping from a virtual NIC to a physical host
#[derive(
    Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct VirtualNetworkInterfaceHost {
    pub virtual_ip: IpAddr,
    pub virtual_mac: external::MacAddr,
    pub physical_host_ip: Ipv6Addr,
    pub vni: external::Vni,
}

/// DHCP configuration for a port
///
/// Not present here: Hostname (DHCPv4 option 12; used in DHCPv6 option 39); we
/// use `InstanceRuntimeState::hostname` for this value.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct DhcpConfig {
    /// DNS servers to send to the instance
    ///
    /// (DHCPv4 option 6; DHCPv6 option 23)
    pub dns_servers: Vec<IpAddr>,

    /// DNS zone this instance's hostname belongs to (e.g. the `project.example`
    /// part of `instance1.project.example`)
    ///
    /// (DHCPv4 option 15; used in DHCPv6 option 39)
    pub host_domain: Option<String>,

    /// DNS search domains
    ///
    /// (DHCPv4 option 119; DHCPv6 option 24)
    pub search_domains: Vec<String>,
}

/// The target for a given router entry.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case", content = "value")]
pub enum RouterTarget {
    Drop,
    InternetGateway(InternetGatewayRouterTarget),
    Ip(IpAddr),
    VpcSubnet(IpNet),
}

/// An Internet Gateway router target.
#[derive(
    Copy, Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum InternetGatewayRouterTarget {
    /// Targets the gateway for the system-internal services VPC.
    System,
    /// Targets a gateway for an instance's VPC.
    Instance(Uuid),
}

/// Information on the current parent router (and version) of a route set
/// according to the control plane.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct RouterVersion {
    pub router_id: Uuid,
    pub version: u64,
}

impl RouterVersion {
    /// Return whether a new route set should be applied over the current
    /// values.
    ///
    /// This will occur when seeing a new version and a matching parent,
    /// or a new parent router on the control plane.
    pub fn is_replaced_by(&self, other: &Self) -> bool {
        (self.router_id != other.router_id) || self.version < other.version
    }
}

/// Identifier for a VPC and/or subnet.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct RouterId {
    pub vni: Vni,
    pub kind: RouterKind,
}

/// The scope of a set of VPC router rules.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case", content = "subnet")]
pub enum RouterKind {
    System,
    Custom(IpNet),
}

/// Version information for routes on a given VPC subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ResolvedVpcRouteState {
    pub id: RouterId,
    pub version: Option<RouterVersion>,
}

/// An updated set of routes for a given VPC and/or subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ResolvedVpcRouteSet {
    pub id: RouterId,
    pub version: Option<RouterVersion>,
    pub routes: HashSet<ResolvedVpcRoute>,
}

/// Per-NIC mappings from external IP addresses to the Internet Gateways
/// which can choose them as a source.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ExternalIpGatewayMap {
    pub mappings: HashMap<Uuid, HashMap<IpAddr, HashSet<Uuid>>>,
}

/// Describes the purpose of the dataset.
#[derive(
    Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, EnumCount, Diffable,
)]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum DatasetKind {
    // Durable datasets for zones
    Cockroach,
    Crucible,
    /// Used for single-node clickhouse deployments
    Clickhouse,
    /// Used for replicated clickhouse deployments
    ClickhouseKeeper,
    /// Used for replicated clickhouse deployments
    ClickhouseServer,
    ExternalDns,
    InternalDns,

    // Zone filesystems
    TransientZoneRoot,
    TransientZone {
        #[cfg_attr(any(test, feature = "testing"), strategy("[^/]+"))]
        name: String,
    },

    // Other datasets
    Debug,

    /// Used for local storage disk types, contains volumes delegated to VMMs
    LocalStorage,
}

impl Serialize for DatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(de::Error::custom)
    }
}

impl JsonSchema for DatasetKind {
    fn schema_name() -> String {
        "DatasetKind".to_string()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        // The schema is a bit more complicated than this -- it's either one of
        // the fixed values or a string starting with "zone/" -- but this is
        // good enough for now.
        let mut schema = <String>::json_schema(generator).into_object();
        schema.metadata().description = Some(
            "The kind of dataset. See the `DatasetKind` enum \
             in omicron-common for possible values."
                .to_owned(),
        );
        schema.into()
    }
}

impl DatasetKind {
    pub fn dataset_should_be_encrypted(&self) -> bool {
        match self {
            // We encrypt all datasets except Crucible.
            //
            // Crucible already performs encryption internally, and we
            // avoid double-encryption.
            DatasetKind::Crucible => false,
            _ => true,
        }
    }

    /// Returns true if this dataset is delegated to a non-global zone.
    ///
    /// Note: the `zoned` property of a dataset controls whether or not a
    /// dataset is managed from a non-global zone. This function's intent is
    /// different in the sense that it's asking whether or not a dataset will be
    /// delegated to a non-global zone, not managed by a non-global zone.
    pub fn zoned(&self) -> bool {
        use DatasetKind::*;
        match self {
            Cockroach | Crucible | Clickhouse | ClickhouseKeeper
            | ClickhouseServer | ExternalDns | InternalDns => true,

            TransientZoneRoot | TransientZone { .. } | Debug | LocalStorage => {
                false
            }
        }
    }

    /// Returns the zone name, if this is a dataset for a zone filesystem.
    ///
    /// Otherwise, returns "None".
    pub fn zone_name(&self) -> Option<&str> {
        if let DatasetKind::TransientZone { name } = self {
            Some(name)
        } else {
            None
        }
    }
}

// Be cautious updating this implementation:
//
// - It should align with [DatasetKind::FromStr], below
// - The strings here are used here comprise the dataset name, stored durably
// on-disk
impl fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DatasetKind::*;
        let s = match self {
            Crucible => "crucible",
            Cockroach => "cockroachdb",
            Clickhouse => "clickhouse",
            ClickhouseKeeper => "clickhouse_keeper",
            ClickhouseServer => "clickhouse_server",
            ExternalDns => "external_dns",
            InternalDns => "internal_dns",
            TransientZoneRoot => "zone",
            TransientZone { name } => {
                write!(f, "zone/{}", name)?;
                return Ok(());
            }
            Debug => "debug",
            LocalStorage => "local_storage",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetKindParseError {
    #[error("Dataset unknown: {0}")]
    UnknownDataset(String),
}

impl FromStr for DatasetKind {
    type Err = DatasetKindParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DatasetKind::*;
        let kind = match s {
            "cockroachdb" => Cockroach,
            "crucible" => Crucible,
            "clickhouse" => Clickhouse,
            "clickhouse_keeper" => ClickhouseKeeper,
            "clickhouse_server" => ClickhouseServer,
            "external_dns" => ExternalDns,
            "internal_dns" => InternalDns,
            "zone" => TransientZoneRoot,
            "debug" => Debug,
            "local_storage" => LocalStorage,
            other => {
                if let Some(name) = other.strip_prefix("zone/") {
                    TransientZone { name: name.to_string() }
                } else {
                    return Err(DatasetKindParseError::UnknownDataset(
                        s.to_string(),
                    ));
                }
            }
        };
        Ok(kind)
    }
}

/// Identifiers for a single sled.
///
/// This is intended primarily to be used in timeseries, to identify
/// sled from which metric data originates.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SledIdentifiers {
    /// Control plane ID of the rack this sled is a member of
    pub rack_id: Uuid,
    /// Control plane ID for the sled itself
    pub sled_id: Uuid,
    /// Model name of the sled
    pub model: String,
    /// Revision number of the sled
    pub revision: u32,
    /// Serial number of the sled
    //
    // NOTE: This is only guaranteed to be unique within a model.
    pub serial: String,
}

/// Delegate a ZFS volume to a zone
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DelegatedZvol {
    /// Delegate a slice of the local storage dataset present on this pool into
    /// the zone.
    LocalStorage { zpool_id: ExternalZpoolUuid, dataset_id: DatasetUuid },
}

impl DelegatedZvol {
    /// Return the fully qualified dataset name that the volume is in.
    pub fn parent_dataset_name(&self) -> String {
        match &self {
            DelegatedZvol::LocalStorage { zpool_id, dataset_id } => {
                // The local storage dataset is the parent for an allocation
                let local_storage_parent = DatasetName::new(
                    ZpoolName::External(*zpool_id),
                    DatasetKind::LocalStorage,
                );

                format!("{}/{}", local_storage_parent.full_name(), dataset_id)
            }
        }
    }

    /// Return the mountpoint for the parent dataset in the zone
    pub fn parent_dataset_mountpoint(&self) -> String {
        match &self {
            DelegatedZvol::LocalStorage { dataset_id, .. } => {
                format!("/{}", dataset_id)
            }
        }
    }

    /// Return the fully qualified volume name
    pub fn volume_name(&self) -> String {
        match &self {
            DelegatedZvol::LocalStorage { .. } => {
                // For now, all local storage zvols use the same name
                format!("{}/vol", self.parent_dataset_name())
            }
        }
    }

    /// Return the device that should be delegated into the zone
    pub fn zvol_device(&self) -> String {
        match &self {
            DelegatedZvol::LocalStorage { .. } => {
                // Use the `rdsk` device to avoid interacting with an additional
                // buffer cache that would be used if we used `dsk`.
                format!("/dev/zvol/rdsk/{}", self.volume_name())
            }
        }
    }

    pub fn volblocksize(&self) -> u32 {
        match &self {
            DelegatedZvol::LocalStorage { .. } => {
                // all Local storage zvols use 4096 byte blocks
                4096
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::internal::shared::AllowedSourceIps;
    use oxnet::{IpNet, Ipv4Net, Ipv6Net};
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_deserialize_allowed_source_ips() {
        let parsed: AllowedSourceIps = serde_json::from_str(
            r#"{"allow":"list","ips":["127.0.0.1/32","10.0.0.0/24","fd00::1/64"]}"#,
        )
        .unwrap();
        assert_eq!(
            parsed,
            AllowedSourceIps::try_from(vec![
                Ipv4Net::host_net(Ipv4Addr::LOCALHOST).into(),
                IpNet::V4(
                    Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap()
                ),
                IpNet::V6(
                    Ipv6Net::new(
                        Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1),
                        64
                    )
                    .unwrap()
                ),
            ])
            .unwrap()
        );
    }

    #[test]
    fn test_deserialize_unknown_string() {
        serde_json::from_str::<AllowedSourceIps>(r#"{"allow":"wat"}"#)
            .expect_err(
                "Should not be able to deserialize from unknown variant name",
            );
    }

    #[test]
    fn test_deserialize_any_into_allowed_external_ips() {
        assert_eq!(
            AllowedSourceIps::Any,
            serde_json::from_str(r#"{"allow":"any"}"#).unwrap(),
        );
    }

    #[test]
    fn test_dataset_kind_serialization() {
        let kinds = [
            DatasetKind::Cockroach,
            DatasetKind::Crucible,
            DatasetKind::Clickhouse,
            DatasetKind::ClickhouseKeeper,
            DatasetKind::ClickhouseServer,
            DatasetKind::ExternalDns,
            DatasetKind::InternalDns,
            DatasetKind::TransientZoneRoot,
            DatasetKind::TransientZone { name: String::from("myzone") },
            DatasetKind::Debug,
            DatasetKind::LocalStorage,
        ];

        assert_eq!(kinds.len(), DatasetKind::COUNT);

        for kind in &kinds {
            // To string, from string
            let as_str = kind.to_string();
            let from_str =
                DatasetKind::from_str(&as_str).unwrap_or_else(|_| {
                    panic!("Failed to convert {kind} to and from string")
                });
            assert_eq!(
                *kind, from_str,
                "{kind} failed to convert to/from a string"
            );

            // Serialize, deserialize
            let ser = serde_json::to_string(&kind)
                .unwrap_or_else(|_| panic!("Failed to serialize {kind}"));
            let de: DatasetKind = serde_json::from_str(&ser)
                .unwrap_or_else(|_| panic!("Failed to deserialize {kind}"));
            assert_eq!(*kind, de, "{kind} failed serialization");

            // Test that serialization is equivalent to stringifying.
            assert_eq!(
                format!("\"{as_str}\""),
                ser,
                "{kind} does not match stringification/serialization"
            );
        }
    }

    #[test]
    fn test_delegated_zvol_device_name() {
        let delegated_zvol = DelegatedZvol::LocalStorage {
            zpool_id: "cb832c2e-fa94-4911-89a9-895ac8b1e8f3".parse().unwrap(),
            dataset_id: "2bbf0908-21da-4bc3-882b-1a1e715c54bd".parse().unwrap(),
        };

        assert_eq!(
            delegated_zvol.zvol_device(),
            [
                String::from("/dev/zvol/rdsk"),
                String::from("oxp_cb832c2e-fa94-4911-89a9-895ac8b1e8f3/crypt"),
                String::from("local_storage"),
                String::from("2bbf0908-21da-4bc3-882b-1a1e715c54bd/vol"),
            ]
            .join("/"),
        );
    }
}
