// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;

use itertools::Either;
use itertools::Itertools;
use omicron_common::api::external;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::inventory::SourceNatConfig;
use super::inventory::SourceNatConfigV4;
use super::inventory::SourceNatConfigV6;
use crate::impls::inventory::SourceNatConfigError;
use crate::v1;
use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::VmmSpec;
use crate::v7::instance::InstanceMulticastMembership;
use crate::v9::instance::DelegatedZvol;
use crate::v10;
use crate::v10::instance::ResolvedVpcFirewallRule;

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
    pub external_ips: Option<ExternalIpConfig>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
}

impl TryFrom<v10::instance::InstanceEnsureBody> for InstanceEnsureBody {
    type Error = external::Error;

    fn try_from(
        v10: v10::instance::InstanceEnsureBody,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            vmm_spec: v10.vmm_spec,
            local_config: v10.local_config.try_into()?,
            vmm_runtime: v10.vmm_runtime,
            instance_id: v10.instance_id,
            migration_id: v10.migration_id,
            propolis_addr: v10.propolis_addr,
            metadata: v10.metadata,
        })
    }
}

impl TryFrom<v10::instance::InstanceSledLocalConfig>
    for InstanceSledLocalConfig
{
    type Error = external::Error;

    fn try_from(
        v10: v10::instance::InstanceSledLocalConfig,
    ) -> Result<Self, Self::Error> {
        let external_ips = ExternalIpConfig::try_from_generic(
            Some(v10.source_nat),
            v10.ephemeral_ip,
            v10.floating_ips.clone(),
        )
        .map_err(|e| {
            external::Error::invalid_request(format!(
                "invalid external IP config: {e}"
            ))
        })?;

        Ok(Self {
            hostname: v10.hostname,
            nics: v10.nics,
            external_ips: Some(external_ips),
            multicast_groups: v10.multicast_groups,
            firewall_rules: v10.firewall_rules,
            dhcp_config: v10.dhcp_config,
            delegated_zvols: v10.delegated_zvols,
        })
    }
}

/// IPv4 external IP configuration.
#[derive(
    Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize,
)]
pub struct ExternalIpv4Config {
    /// Source NAT configuration, for outbound-only connectivity.
    pub source_nat: Option<SourceNatConfigV4>,
    /// An Ephemeral address for in- and outbound connectivity.
    pub ephemeral_ip: Option<Ipv4Addr>,
    /// Additional Floating IPs for in- and outbound connectivity.
    pub floating_ips: Vec<Ipv4Addr>,
}

/// IPv6 external IP configuration.
#[derive(
    Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize,
)]
pub struct ExternalIpv6Config {
    /// Source NAT configuration, for outbound-only connectivity.
    pub source_nat: Option<SourceNatConfigV6>,
    /// An Ephemeral address for in- and outbound connectivity.
    pub ephemeral_ip: Option<Ipv6Addr>,
    /// Additional Floating IPs for in- and outbound connectivity.
    pub floating_ips: Vec<Ipv6Addr>,
}

/// A single- or dual-stack external IP configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ExternalIpConfig {
    /// Single-stack IPv4 external IP configuration.
    V4(ExternalIpv4Config),
    /// Single-stack IPv6 external IP configuration.
    V6(ExternalIpv6Config),
    /// Both IPv4 and IPv6 external IP configuration.
    DualStack { v4: ExternalIpv4Config, v6: ExternalIpv6Config },
}

impl From<ExternalIpv4Config> for ExternalIpConfig {
    fn from(cfg: ExternalIpv4Config) -> Self {
        Self::V4(cfg)
    }
}

impl From<ExternalIpv6Config> for ExternalIpConfig {
    fn from(cfg: ExternalIpv6Config) -> Self {
        Self::V6(cfg)
    }
}

// This impl is here because it's only used in the `TryFrom<_>` above.
impl ExternalIpConfig {
    /// Attempt to convert from generic IP addressing information.
    ///
    /// This is used to convert older API versions which used version-agnostic
    /// IP address types (`IpAddr`) rather than concrete `Ipv4Addr`/`Ipv6Addr`.
    ///
    /// Returns an error if there are no addresses at all, or if there are
    /// mixed IP versions.
    fn try_from_generic(
        source_nat: Option<v1::inventory::SourceNatConfig>,
        ephemeral_ip: Option<IpAddr>,
        floating_ips: Vec<IpAddr>,
    ) -> Result<Self, ExternalIpsError> {
        let snat_v4;
        let snat_v6;
        match source_nat {
            Some(snat) => match snat.ip {
                IpAddr::V4(ipv4) => {
                    snat_v6 = None;
                    snat_v4 = Some(SourceNatConfig::new(
                        ipv4,
                        snat.first_port,
                        snat.last_port,
                    )?);
                }
                IpAddr::V6(ipv6) => {
                    snat_v6 = Some(SourceNatConfig::new(
                        ipv6,
                        snat.first_port,
                        snat.last_port,
                    )?);
                    snat_v4 = None;
                }
            },
            None => {
                snat_v4 = None;
                snat_v6 = None;
            }
        };

        let (fips_v4, fips_v6): (Vec<_>, Vec<_>) =
            floating_ips.into_iter().partition_map(|fip| match fip {
                IpAddr::V4(v4) => Either::Left(v4),
                IpAddr::V6(v6) => Either::Right(v6),
            });

        match (snat_v4, snat_v6, ephemeral_ip) {
            (None, None, None) => {
                match (fips_v4.is_empty(), fips_v6.is_empty()) {
                    (true, true) => Err(ExternalIpsError::NoIps),
                    (true, false) => Ok(ExternalIpv6Config {
                        source_nat: None,
                        ephemeral_ip: None,
                        floating_ips: fips_v6,
                    }
                    .into()),
                    (false, true) => Ok(ExternalIpv4Config {
                        source_nat: None,
                        ephemeral_ip: None,
                        floating_ips: fips_v4,
                    }
                    .into()),
                    (false, false) => Err(ExternalIpsError::MixedIpVersions),
                }
            }
            (None, None, Some(IpAddr::V4(v4))) => {
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(ExternalIpv4Config {
                    source_nat: None,
                    ephemeral_ip: Some(v4),
                    floating_ips: fips_v4,
                }
                .into())
            }
            (None, None, Some(IpAddr::V6(v6))) => {
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(ExternalIpv6Config {
                    source_nat: None,
                    ephemeral_ip: Some(v6),
                    floating_ips: fips_v6,
                }
                .into())
            }
            (None, Some(snat_v6), None) => {
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(ExternalIpv6Config {
                    source_nat: Some(snat_v6),
                    ephemeral_ip: None,
                    floating_ips: fips_v6,
                }
                .into())
            }
            (None, Some(snat_v6), Some(IpAddr::V6(eip_v6))) => {
                if !fips_v4.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(ExternalIpv6Config {
                    source_nat: Some(snat_v6),
                    ephemeral_ip: Some(eip_v6),
                    floating_ips: fips_v6,
                }
                .into())
            }
            (Some(snat_v4), None, None) => {
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(ExternalIpv4Config {
                    source_nat: Some(snat_v4),
                    ephemeral_ip: None,
                    floating_ips: fips_v4,
                }
                .into())
            }
            (Some(snat_v4), None, Some(IpAddr::V4(eip_v4))) => {
                if !fips_v6.is_empty() {
                    return Err(ExternalIpsError::MixedIpVersions);
                }
                Ok(ExternalIpv4Config {
                    source_nat: Some(snat_v4),
                    ephemeral_ip: Some(eip_v4),
                    floating_ips: fips_v4,
                }
                .into())
            }
            (Some(_), None, Some(IpAddr::V6(_)))
            | (None, Some(_), Some(IpAddr::V4(_))) => {
                Err(ExternalIpsError::MixedIpVersions)
            }
            (Some(_), Some(_), None) | (Some(_), Some(_), Some(_)) => {
                Err(ExternalIpsError::MixedIpVersions)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ExternalIpsError {
    #[error(
        "Must specify at least one SNAT, ephemeral, or floating IP address"
    )]
    NoIps,
    #[error(
        "SNAT, ephemeral, and floating IPs must all be of the same IP version"
    )]
    MixedIpVersions,
    #[error(transparent)]
    SourceNat(#[from] SourceNatConfigError),
}
