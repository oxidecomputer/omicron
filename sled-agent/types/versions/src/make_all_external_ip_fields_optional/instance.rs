// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version `MAKE_ALL_EXTERNAL_IP_FIELDS_OPTIONAL`.

use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;

use omicron_common::address::ConcreteIp;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::shared::DelegatedZvol;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::VmmRuntimeState;
use crate::v7::instance::InstanceMulticastMembership;
use crate::v10::inventory::NetworkInterface;
use crate::v11;
use crate::v11::inventory::SnatSchema;
use crate::v11::inventory::SourceNatConfig;
use crate::v18::attached_subnet::AttachedSubnet;
use crate::v29::instance::VmmSpec;
use crate::v31;
use crate::v31::instance::ResolvedVpcFirewallRule;

/// The body of a request to ensure that a instance and VMM are known to a sled
/// agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// The virtual hardware configuration this virtual machine should have when
    /// it is started.
    pub vmm_spec: VmmSpec,

    /// Information about the sled-local configuration that needs to be
    /// established to make the VM's virtual hardware fully functional.
    pub local_config: InstanceSledLocalConfig,

    /// The initial VMM runtime state for the VMM being registered.
    pub vmm_runtime: VmmRuntimeState,

    /// The ID of the instance for which this VMM is being created.
    pub instance_id: InstanceUuid,

    /// The ID of the migration in to this VMM, if this VMM is being
    /// ensured is part of a migration in. If this is `None`, the VMM is not
    /// being created due to a migration.
    pub migration_id: Option<Uuid>,

    /// The address at which this VMM should serve a Propolis server API.
    pub propolis_addr: SocketAddr,

    /// Metadata used to track instance statistics.
    pub metadata: InstanceMetadata,
}

/// Describes sled-local configuration that a sled-agent must establish to make
/// the instance's virtual hardware fully functional.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstanceSledLocalConfig {
    pub hostname: Hostname,
    pub nics: Vec<NetworkInterface>,
    pub external_ips: ExternalIpConfig,
    pub attached_subnets: Vec<AttachedSubnet>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
}

impl From<v31::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(old: v31::instance::InstanceEnsureBody) -> Self {
        Self {
            vmm_spec: old.vmm_spec,
            local_config: old.local_config.into(),
            vmm_runtime: old.vmm_runtime,
            instance_id: old.instance_id,
            migration_id: old.migration_id,
            propolis_addr: old.propolis_addr,
            metadata: old.metadata,
        }
    }
}

impl From<v31::instance::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    fn from(old: v31::instance::InstanceSledLocalConfig) -> Self {
        let external_ips = match old.external_ips {
            None => ExternalIpConfig::default(),
            Some(eic) => eic.into(),
        };
        Self {
            hostname: old.hostname,
            nics: old.nics,
            external_ips,
            attached_subnets: old.attached_subnets,
            multicast_groups: old.multicast_groups,
            firewall_rules: old.firewall_rules,
            dhcp_config: old.dhcp_config,
            delegated_zvols: old.delegated_zvols,
        }
    }
}

/// Helper trait specifying the name of the JSON Schema for an
/// `ExternalIpConfig` object.
///
/// This exists so we can use a generic type and have the names of the concrete
/// type aliases be the same as the name of the schema oject.
pub trait ExternalIpSchema {
    fn json_schema_name() -> String;
}

impl ExternalIpSchema for Ipv4Addr {
    fn json_schema_name() -> String {
        String::from("ExternalIpv4Config")
    }
}

impl ExternalIpSchema for Ipv6Addr {
    fn json_schema_name() -> String {
        String::from("ExternalIpv6Config")
    }
}

/// External IP address configuration.
///
/// This encapsulates all the external addresses of a single IP version,
/// including source NAT, Ephemeral, and Floating IPs.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(bound = "T: ConcreteIp + SnatSchema + serde::de::DeserializeOwned")]
pub struct ExternalIps<T>
where
    T: ConcreteIp,
{
    /// Source NAT configuration, for outbound-only connectivity.
    pub source_nat: Option<SourceNatConfig<T>>,
    /// An Ephemeral address for in- and outbound connectivity.
    pub ephemeral_ip: Option<T>,
    /// Additional Floating IPs for in- and outbound connectivity.
    pub floating_ips: BTreeSet<T>,
}

impl<T: ConcreteIp> Default for ExternalIps<T> {
    fn default() -> Self {
        Self {
            source_nat: None,
            ephemeral_ip: None,
            floating_ips: BTreeSet::new(),
        }
    }
}

pub type ExternalIpv4Config = ExternalIps<Ipv4Addr>;
pub type ExternalIpv6Config = ExternalIps<Ipv6Addr>;

// Private type only used for deriving the JSON schema for `ExternalIps`.
#[derive(JsonSchema)]
#[allow(dead_code)]
struct ExternalIpsSchema<T>
where
    T: ConcreteIp + SnatSchema + ExternalIpSchema,
{
    /// Source NAT configuration, for outbound-only connectivity.
    source_nat: Option<SourceNatConfig<T>>,
    /// An Ephemeral address for in- and outbound connectivity.
    ephemeral_ip: Option<T>,
    /// Additional Floating IPs for in- and outbound connectivity.
    floating_ips: BTreeSet<T>,
}

impl JsonSchema for ExternalIpv4Config {
    fn schema_name() -> String {
        String::from("ExternalIpv4Config")
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        ExternalIpsSchema::<Ipv4Addr>::json_schema(generator)
    }
}

impl JsonSchema for ExternalIpv6Config {
    fn schema_name() -> String {
        String::from("ExternalIpv6Config")
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        ExternalIpsSchema::<Ipv6Addr>::json_schema(generator)
    }
}

/// External IP address configuration.
///
/// This encapsulates all external addresses for an instance, with separate
/// optional configurations for IPv4 and IPv6.
#[derive(
    Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize,
)]
pub struct ExternalIpConfig {
    /// IPv4 external IP configuration.
    pub v4: Option<ExternalIpv4Config>,
    /// IPv6 external IP configuration.
    pub v6: Option<ExternalIpv6Config>,
}

impl From<v11::instance::ExternalIpv4Config> for ExternalIpv4Config {
    fn from(old: v11::instance::ExternalIpv4Config) -> Self {
        Self {
            source_nat: old.source_nat,
            ephemeral_ip: old.ephemeral_ip,
            floating_ips: old.floating_ips.into_iter().collect(),
        }
    }
}

impl From<v11::instance::ExternalIpv6Config> for ExternalIpv6Config {
    fn from(old: v11::instance::ExternalIpv6Config) -> Self {
        Self {
            source_nat: old.source_nat,
            ephemeral_ip: old.ephemeral_ip,
            floating_ips: old.floating_ips.into_iter().collect(),
        }
    }
}

impl From<v11::instance::ExternalIpConfig> for ExternalIpConfig {
    fn from(old: v11::instance::ExternalIpConfig) -> Self {
        match old {
            v11::instance::ExternalIpConfig::V4(v4) => {
                Self { v4: Some(v4.into()), v6: None }
            }
            v11::instance::ExternalIpConfig::V6(v6) => {
                Self { v4: None, v6: Some(v6.into()) }
            }
            v11::instance::ExternalIpConfig::DualStack { v4, v6 } => {
                Self { v4: Some(v4.into()), v6: Some(v6.into()) }
            }
        }
    }
}
