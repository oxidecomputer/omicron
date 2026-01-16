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
//! [`AddressSelector`]: nexus_types::external_api::params::AddressSelector
//! [`PoolSelector`]: nexus_types::external_api::params::PoolSelector
//! [`MulticastGroupJoinSpec`]: nexus_types::external_api::params::MulticastGroupJoinSpec

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use nexus_types::external_api::params;
use omicron_common::api::external;
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

impl TryFrom<FloatingIpCreate> for params::FloatingIpCreate {
    type Error = external::Error;

    fn try_from(
        old: FloatingIpCreate,
    ) -> Result<params::FloatingIpCreate, external::Error> {
        let address_selector = match (old.ip, old.pool, old.ip_version) {
            // Explicit IP address provided -> ip_version must not be set
            (Some(ip), pool, None) => {
                params::AddressSelector::Explicit { ip, pool }
            }
            // Explicit IP and ip_version is an invalid combination
            (Some(_), _, Some(_)) => {
                return Err(external::Error::invalid_request(
                    "cannot specify both `ip` and `ip_version`; \
                     the IP version is determined by the explicit IP address",
                ));
            }
            // No explicit IP, but named pool specified -> ip_version must not be set
            (None, Some(pool), None) => params::AddressSelector::Auto {
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
            (None, None, ip_version) => params::AddressSelector::Auto {
                pool_selector: params::PoolSelector::Auto { ip_version },
            },
        };
        Ok(params::FloatingIpCreate {
            identity: old.identity,
            address_selector,
        })
    }
}

// v2026010300 (DUAL_STACK_NICS) uses v2026011300's network interface types
// (pre-VPC_SUBNET_ATTACHMENT, with `subnet_name: Name`).
use crate::v2026010500;
use crate::v2026011300;

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
    pub network_interfaces: v2026011300::InstanceNetworkInterfaceAttachment,
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
    pub disks: Vec<params::InstanceDiskAttachment>,
    /// The disk the instance is configured to boot from.
    #[serde(default)]
    pub boot_disk: Option<params::InstanceDiskAttachment>,
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

impl TryFrom<InstanceCreate> for v2026010500::InstanceCreate {
    type Error = external::Error;

    fn try_from(
        old: InstanceCreate,
    ) -> Result<v2026010500::InstanceCreate, external::Error> {
        let external_ips: Vec<params::ExternalIpCreate> = old
            .external_ips
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(v2026010500::InstanceCreate {
            identity: old.identity,
            ncpus: old.ncpus,
            memory: old.memory,
            hostname: old.hostname,
            user_data: old.user_data,
            network_interfaces: old.network_interfaces,
            external_ips,
            multicast_groups: old.multicast_groups,
            disks: old.disks,
            boot_disk: old.boot_disk,
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
    use proptest::prelude::*;

    fn make_floating_ip(
        ip: Option<std::net::IpAddr>,
        pool: Option<NameOrId>,
        ip_version: Option<IpVersion>,
    ) -> FloatingIpCreate {
        FloatingIpCreate {
            identity: IdentityMetadataCreateParams {
                name: "fip".parse().unwrap(),
                description: "test".to_string(),
            },
            ip,
            pool,
            ip_version,
        }
    }

    // =========================================================================
    // Error cases: these test invalid input combinations that must be rejected
    // =========================================================================

    #[test]
    fn ephemeral_ip_with_pool_and_ip_version_fails() {
        let old = EphemeralIpCreate {
            pool: Some("my-pool".parse::<external::Name>().unwrap().into()),
            ip_version: Some(IpVersion::V4),
        };
        assert!(TryInto::<params::EphemeralIpCreate>::try_into(old).is_err());
    }

    #[test]
    fn floating_ip_with_ip_and_ip_version_fails() {
        let old = make_floating_ip(
            Some("10.0.0.1".parse().unwrap()),
            None,
            Some(IpVersion::V4),
        );
        assert!(TryInto::<params::FloatingIpCreate>::try_into(old).is_err());
    }

    #[test]
    fn floating_ip_with_pool_and_ip_version_fails() {
        let old = make_floating_ip(
            None,
            Some("my-pool".parse::<external::Name>().unwrap().into()),
            Some(IpVersion::V4),
        );
        assert!(TryInto::<params::FloatingIpCreate>::try_into(old).is_err());
    }

    // =========================================================================
    // Property tests
    // =========================================================================

    proptest! {
        /// EphemeralIpCreate: ip_version is preserved when using default pool
        #[test]
        fn ephemeral_ip_preserves_ip_version(
            ip_version in prop::option::of(prop_oneof![
                Just(IpVersion::V4),
                Just(IpVersion::V6),
            ])
        ) {
            let old = EphemeralIpCreate { pool: None, ip_version };
            let result: params::EphemeralIpCreate = old.try_into().unwrap();
            match result.pool_selector {
                params::PoolSelector::Auto { ip_version: v } => {
                    prop_assert!(v == ip_version);
                }
                _ => panic!("expected Auto variant"),
            }
        }

        /// EphemeralIpCreate: explicit pool produces Explicit selector
        #[test]
        fn ephemeral_ip_with_pool_produces_explicit(
            pool_name in "[a-z][a-z0-9]{0,8}"
        ) {
            let pool: NameOrId = pool_name.parse::<external::Name>().unwrap().into();
            let old = EphemeralIpCreate { pool: Some(pool), ip_version: None };
            let result: params::EphemeralIpCreate = old.try_into().unwrap();
            match result.pool_selector {
                params::PoolSelector::Explicit { .. } => {}
                _ => panic!("expected Explicit variant"),
            }
        }

        /// FloatingIpCreate: explicit IP is preserved in output
        #[test]
        fn floating_ip_preserves_explicit_ip(ip in any::<std::net::IpAddr>()) {
            let old = make_floating_ip(Some(ip), None, None);
            let result: params::FloatingIpCreate = old.try_into().unwrap();
            match result.address_selector {
                params::AddressSelector::Explicit { ip: addr, .. } => {
                    prop_assert!(addr == ip);
                }
                _ => panic!("expected Explicit variant"),
            }
        }

        /// FloatingIpCreate: explicit pool produces correct selector
        #[test]
        fn floating_ip_with_pool_produces_explicit(
            pool_name in "[a-z][a-z0-9]{0,8}"
        ) {
            let pool: NameOrId = pool_name.parse::<external::Name>().unwrap().into();
            let old = make_floating_ip(None, Some(pool), None);
            let result: params::FloatingIpCreate = old.try_into().unwrap();
            match result.address_selector {
                params::AddressSelector::Auto {
                    pool_selector: params::PoolSelector::Explicit { .. }
                } => {}
                _ => panic!("expected Auto/Explicit variant"),
            }
        }

        /// FloatingIpCreate: ip_version is preserved when using default pool
        #[test]
        fn floating_ip_preserves_ip_version(
            ip_version in prop::option::of(prop_oneof![
                Just(IpVersion::V4),
                Just(IpVersion::V6),
            ])
        ) {
            let old = make_floating_ip(None, None, ip_version);
            let result: params::FloatingIpCreate = old.try_into().unwrap();
            match result.address_selector {
                params::AddressSelector::Auto {
                    pool_selector: params::PoolSelector::Auto { ip_version: v }
                } => {
                    prop_assert!(v == ip_version);
                }
                _ => panic!("expected Auto/Auto variant"),
            }
        }
    }
}
