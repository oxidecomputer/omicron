// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2026010300 to 2026010500.
//!
//! ## Pool Selection Changes
//!
//! [`FloatingIpCreate`], [`EphemeralIpCreate`], and [`ExternalIpCreate`]
//! use flat structures with `pool` and `ip_version` fields. Newer versions
//! use tagged enums ([`AddressSelector`] and [`PoolSelector`]) that
//! make invalid states unrepresentable.
//!
//! Affected endpoints:
//! - `POST /v1/floating-ips` (floating_ip_create)
//! - `POST /v1/instances/{instance}/external-ips/ephemeral` (instance_ephemeral_ip_attach)
//! - `POST /v1/instances` (instance_create)
//! - `POST /experimental/v1/probes` (probe_create)
//!
//! ## Multicast Changes
//!
//! [`InstanceCreate`] uses `Vec<NameOrId>` for `multicast_groups`. Newer
//! versions use `Vec<MulticastGroupJoinSpec>` for implicit lifecycle support.
//!
//! [`FloatingIpCreate`]: self::FloatingIpCreate
//! [`EphemeralIpCreate`]: self::EphemeralIpCreate
//! [`ExternalIpCreate`]: self::ExternalIpCreate
//! [`InstanceCreate`]: self::InstanceCreate
//! [`AddressSelector`]: crate::v2026011501::AddressSelector
//! [`PoolSelector`]: nexus_types::external_api::params::PoolSelector
//! [`MulticastGroupJoinSpec`]: nexus_types::external_api::params::MulticastGroupJoinSpec

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use nexus_types::external_api::params;
use omicron_common::api::external;

use crate::v2026011501;
use crate::v2026013000;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceCpuPlatform,
    IpVersion, NameOrId,
};

/// Parameters for creating an ephemeral IP address for an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct EphemeralIpCreate {
    /// Name or ID of the IP pool used to allocate an address.
    /// If unspecified, the default IP pool will be used.
    pub pool: Option<NameOrId>,
    /// The IP version preference for address allocation. Only used when
    /// allocating from the default pool (i.e., when `pool` is not specified).
    /// If a silo has multiple default pools of different IP versions, this
    /// field is required to disambiguate.
    pub ip_version: Option<IpVersion>,
}

impl TryFrom<EphemeralIpCreate> for params::EphemeralIpCreate {
    type Error = external::Error;

    fn try_from(
        old: EphemeralIpCreate,
    ) -> Result<params::EphemeralIpCreate, external::Error> {
        let pool_selector = match (old.pool, old.ip_version) {
            // Named pool specified -> ip_version must not be set
            (Some(pool), None) => params::PoolSelector::Explicit { pool },
            // Named pool & ip_version is an invalid combination
            (Some(_), Some(_)) => {
                return Err(external::Error::invalid_request(
                    "cannot specify both `pool` and `ip_version`; \
                     `ip_version` is only used when allocating from the default pool",
                ));
            }
            // Default pool with optional ip_version preference
            (None, ip_version) => params::PoolSelector::Auto { ip_version },
        };
        Ok(params::EphemeralIpCreate { pool_selector })
    }
}

/// The type of IP address to attach to an instance during creation.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpCreate {
    /// An IP address providing both inbound and outbound access.
    /// The address is automatically assigned from the provided IP pool
    /// or the default IP pool if not specified.
    Ephemeral {
        /// Name or ID of the IP pool to use. If unspecified, the
        /// default IP pool will be used.
        pool: Option<NameOrId>,
        /// The IP version preference for address allocation. Only used when
        /// allocating from the default pool (i.e., when `pool` is not
        /// specified). If a silo has multiple default pools of different IP
        /// versions, this field is required to disambiguate.
        ip_version: Option<IpVersion>,
    },
    /// A floating IP address.
    Floating {
        /// The name or ID of the floating IP address to attach.
        floating_ip: NameOrId,
    },
}

impl TryFrom<ExternalIpCreate> for params::ExternalIpCreate {
    type Error = external::Error;

    fn try_from(
        old: ExternalIpCreate,
    ) -> Result<params::ExternalIpCreate, external::Error> {
        match old {
            ExternalIpCreate::Ephemeral { pool, ip_version } => {
                let pool_selector = match (pool, ip_version) {
                    // Named pool specified -> ip_version must not be set
                    (Some(pool), None) => {
                        params::PoolSelector::Explicit { pool }
                    }
                    // Named pool & ip_version is an invalid combination
                    (Some(_), Some(_)) => {
                        return Err(external::Error::invalid_request(
                            "cannot specify both `pool` and `ip_version`; \
                             `ip_version` is only used when allocating from the default pool",
                        ));
                    }
                    // Default pool with optional ip_version preference
                    (None, ip_version) => {
                        params::PoolSelector::Auto { ip_version }
                    }
                };
                Ok(params::ExternalIpCreate::Ephemeral { pool_selector })
            }
            ExternalIpCreate::Floating { floating_ip } => {
                Ok(params::ExternalIpCreate::Floating { floating_ip })
            }
        }
    }
}

/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: omicron_common::api::external::IdentityMetadataCreateParams,
    /// An IP address to reserve for use as a floating IP. This field is
    /// optional: when not set, an address will be automatically chosen from
    /// `pool`. If set, then the IP must be available in the resolved `pool`.
    pub ip: Option<std::net::IpAddr>,
    /// The parent IP pool that a floating IP is pulled from. If unset, the
    /// default pool is selected.
    pub pool: Option<NameOrId>,
    /// The IP version preference for address allocation. Only used when
    /// allocating from the default pool (i.e., when `pool` and `ip` are not
    /// specified). If a silo has multiple default pools of different IP
    /// versions, this field is required to disambiguate.
    pub ip_version: Option<IpVersion>,
}

impl TryFrom<FloatingIpCreate> for v2026011501::FloatingIpCreate {
    type Error = external::Error;

    fn try_from(
        old: FloatingIpCreate,
    ) -> Result<v2026011501::FloatingIpCreate, external::Error> {
        let address_selector = match (old.ip, old.pool, old.ip_version) {
            // Explicit IP address provided -> ip_version must not be set
            (Some(ip), pool, None) => {
                v2026011501::AddressSelector::Explicit { ip, pool }
            }
            // Explicit IP and ip_version is an invalid combination
            (Some(_), _, Some(_)) => {
                return Err(external::Error::invalid_request(
                    "cannot specify both `ip` and `ip_version`; \
                     the IP version is determined by the explicit IP address",
                ));
            }
            // No explicit IP, but named pool specified -> ip_version must not be set
            (None, Some(pool), None) => v2026011501::AddressSelector::Auto {
                pool_selector: params::PoolSelector::Explicit { pool },
            },
            // Named pool and ip_version is an invalid combination
            (None, Some(_), Some(_)) => {
                return Err(external::Error::invalid_request(
                    "cannot specify both `pool` and `ip_version`; \
                     `ip_version` is only used when allocating from the default pool",
                ));
            }
            // Allocate from default pool with optional IP version preference
            (None, None, ip_version) => v2026011501::AddressSelector::Auto {
                pool_selector: params::PoolSelector::Auto { ip_version },
            },
        };
        Ok(v2026011501::FloatingIpCreate {
            identity: old.identity,
            address_selector,
        })
    }
}

// v2026010300 (DUAL_STACK_NICS) uses the current `InstanceNetworkInterfaceAttachment`
// with `DefaultIpv4`, `DefaultIpv6`, `DefaultDualStack` variants.
pub use params::InstanceNetworkInterfaceAttachment;

/// Create-time parameters for an `Instance`
///
/// This version uses `Vec<NameOrId>` for `multicast_groups` instead of
/// `Vec<MulticastGroupJoinSpec>` in newer versions.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The number of vCPUs to be allocated to the instance
    pub ncpus: InstanceCpuCount,
    /// The amount of RAM (in bytes) to be allocated to the instance
    pub memory: ByteCount,
    /// The hostname to be assigned to the instance
    pub hostname: Hostname,
    /// User data for instance initialization systems (such as cloud-init).
    #[serde(default, with = "params::UserData")]
    pub user_data: Vec<u8>,
    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,
    /// The external IP addresses provided to this instance.
    // Uses local ExternalIpCreate (has ip_version field) → params::ExternalIpCreate
    #[serde(default)]
    pub external_ips: Vec<ExternalIpCreate>,
    /// The multicast groups this instance should join.
    ///
    /// The instance will be automatically added as a member of the specified
    /// multicast groups during creation, enabling it to send and receive
    /// multicast traffic for those groups.
    #[serde(default)]
    pub multicast_groups: Vec<NameOrId>,
    /// A list of disks to be attached to the instance.
    #[serde(default)]
    pub disks: Vec<v2026013000::InstanceDiskAttachment>,
    /// The disk the instance is configured to boot from.
    #[serde(default)]
    pub boot_disk: Option<v2026013000::InstanceDiskAttachment>,
    /// An allowlist of SSH public keys to be transferred to the instance.
    pub ssh_public_keys: Option<Vec<NameOrId>>,
    /// Should this instance be started upon creation; true by default.
    #[serde(default = "params::bool_true")]
    pub start: bool,
    /// The auto-restart policy for this instance.
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,
    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,
    /// The CPU platform to be used for this instance.
    #[serde(default)]
    pub cpu_platform: Option<InstanceCpuPlatform>,
}

impl TryFrom<InstanceCreate> for params::InstanceCreate {
    type Error = external::Error;

    fn try_from(
        old: InstanceCreate,
    ) -> Result<params::InstanceCreate, external::Error> {
        let external_ips: Vec<params::ExternalIpCreate> = old
            .external_ips
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(params::InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces,
            external_ips,
            multicast_groups: old
                .multicast_groups
                .into_iter()
                .map(|g| params::MulticastGroupJoinSpec {
                    group: g.into(),
                    source_ips: None,
                    ip_version: None,
                })
                .collect(),
            disks: old.disks.into_iter().map(Into::into).collect(),
            boot_disk: old.boot_disk.map(Into::into),
            ssh_public_keys: old.ssh_public_keys,
            start: old.start,
            auto_restart_policy: old.auto_restart_policy,
            anti_affinity_groups: old.anti_affinity_groups,
            cpu_platform: old.cpu_platform,
        })
    }
}

/// Create time parameters for probes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProbeCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    #[schemars(with = "uuid::Uuid")]
    pub sled: omicron_uuid_kinds::SledUuid,
    pub ip_pool: Option<NameOrId>,
}

impl From<ProbeCreate> for params::ProbeCreate {
    fn from(old: ProbeCreate) -> params::ProbeCreate {
        let pool_selector = match old.ip_pool {
            Some(pool) => params::PoolSelector::Explicit { pool },
            None => params::PoolSelector::Auto { ip_version: None },
        };
        params::ProbeCreate {
            identity: old.identity,
            sled: old.sled,
            pool_selector,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        identity_strategy, ip_version_strategy, name_or_id_strategy,
        optional_ip_strategy, optional_name_or_id_strategy,
    };
    use proptest::prelude::*;
    use std::net::IpAddr;
    use test_strategy::proptest;

    /// Strategy for valid FloatingIpCreate inputs (ones that won't error).
    ///
    /// Valid when `ip_version` is not specified or when both `ip` and `pool`
    /// are `None`.
    fn valid_floating_ip_create_strategy()
    -> impl Strategy<Value = FloatingIpCreate> {
        let ip_or_pool =
            (optional_ip_strategy(), optional_name_or_id_strategy())
                .prop_map(|(ip, pool)| (ip, pool, None));

        let ip_version_only =
            ip_version_strategy().prop_map(|v| (None, None, Some(v)));

        (identity_strategy(), prop_oneof![ip_or_pool, ip_version_only])
            .prop_map(|(identity, (ip, pool, ip_version))| FloatingIpCreate {
                identity,
                ip,
                pool,
                ip_version,
            })
    }

    /// Strategy for invalid FloatingIpCreate inputs (ones that will error).
    ///
    /// Invalid when `ip_version` is specified along with `ip` and/or `pool`.
    fn invalid_floating_ip_create_strategy()
    -> impl Strategy<Value = FloatingIpCreate> {
        let at_least_one_of_ip_or_pool = prop_oneof![
            // only ip
            any::<IpAddr>().prop_map(|ip| (Some(ip), None)),
            // only pool
            name_or_id_strategy().prop_map(|pool| (None, Some(pool))),
            // both
            (any::<IpAddr>(), name_or_id_strategy())
                .prop_map(|(ip, pool)| (Some(ip), Some(pool))),
        ];

        (identity_strategy(), at_least_one_of_ip_or_pool, ip_version_strategy())
            .prop_map(|(identity, (ip, pool), ip_version)| FloatingIpCreate {
                identity,
                ip,
                pool,
                ip_version: Some(ip_version),
            })
    }

    /// Verifies that valid inputs convert to v2026011501::FloatingIpCreate
    /// with the correct AddressSelector variant based on ip/pool/ip_version.
    #[proptest]
    fn floating_ip_create_valid_converts_correctly(
        #[strategy(valid_floating_ip_create_strategy())]
        input: FloatingIpCreate,
    ) {
        use proptest::test_runner::TestCaseError;

        let output: v2026011501::FloatingIpCreate =
            input.clone().try_into().map_err(|e| {
                TestCaseError::fail(format!("unexpected error: {e}"))
            })?;

        prop_assert_eq!(input.identity.name, output.identity.name);
        prop_assert_eq!(
            input.identity.description,
            output.identity.description
        );

        match (input.ip, input.pool, input.ip_version) {
            // Explicit IP address provided -> AddressSelector::Explicit.
            (Some(ip), pool, None) => {
                let v2026011501::AddressSelector::Explicit {
                    ip: out_ip,
                    pool: out_pool,
                } = output.address_selector
                else {
                    return Err(TestCaseError::fail(
                        "expected Explicit variant",
                    ));
                };
                prop_assert_eq!(ip, out_ip);
                prop_assert_eq!(pool, out_pool);
            }
            // Pool specified without IP -> AddressSelector::Auto with
            // PoolSelector::Explicit.
            (None, Some(pool), None) => {
                let v2026011501::AddressSelector::Auto {
                    pool_selector:
                        params::PoolSelector::Explicit { pool: out_pool },
                } = output.address_selector
                else {
                    return Err(TestCaseError::fail(
                        "expected Auto with Explicit pool_selector",
                    ));
                };
                prop_assert_eq!(pool, out_pool);
            }
            // Neither IP nor pool -> AddressSelector::Auto with
            // PoolSelector::Auto.
            (None, None, ip_version) => {
                let v2026011501::AddressSelector::Auto {
                    pool_selector:
                        params::PoolSelector::Auto { ip_version: out_ip_version },
                } = output.address_selector
                else {
                    return Err(TestCaseError::fail(
                        "expected Auto with Auto pool_selector",
                    ));
                };
                prop_assert_eq!(ip_version, out_ip_version);
            }
            // We shouldn't get here with valid_floating_ip_create_strategy.
            _ => unreachable!(),
        }
    }

    /// Verifies that invalid inputs (ip_version with ip or pool) return errors.
    #[proptest]
    fn floating_ip_create_invalid_returns_error(
        #[strategy(invalid_floating_ip_create_strategy())]
        input: FloatingIpCreate,
    ) {
        let result: Result<v2026011501::FloatingIpCreate, _> = input.try_into();
        prop_assert!(result.is_err());
    }
}
