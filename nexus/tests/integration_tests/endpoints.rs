// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authz-related configuration for API endpoints
//!
//! This is used for various authz-related tests.
//! THERE ARE NO TESTS IN THIS FILE.

use crate::integration_tests::unauthorized::HTTP_SERVER;
use chrono::Utc;
use http::method::Method;
use internal_dns_types::names::DNS_ZONE_EXTERNAL_TESTING;
use nexus_db_queries::authn;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::identity::Resource;
use nexus_test_utils::PHYSICAL_DISK_UUID;
use nexus_test_utils::RACK_UUID;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils::SWITCH_UUID;
use nexus_test_utils::resource_helpers::test_params;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::shared::Ipv4Range;
use nexus_types::external_api::views::SledProvisionPolicy;
use omicron_common::api::external::AddressLotKind;
use omicron_common::api::external::AffinityPolicy;
use omicron_common::api::external::AllowedSourceIps;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::FailureDomain;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::UserId;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_test_utils::certificates::CertificateChain;
use semver::Version;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::LazyLock;

type DiskTest<'a> =
    nexus_test_utils::resource_helpers::DiskTest<'a, omicron_nexus::Server>;

pub static HARDWARE_RACK_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/system/hardware/racks/{}", RACK_UUID));
pub const HARDWARE_UNINITIALIZED_SLEDS: &'static str =
    "/v1/system/hardware/sleds-uninitialized";
pub static HARDWARE_SLED_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/system/hardware/sleds/{}", SLED_AGENT_UUID));
pub static HARDWARE_SLED_PROVISION_POLICY_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/system/hardware/sleds/{}/provision-policy",
            SLED_AGENT_UUID
        )
    });
pub static DEMO_SLED_PROVISION_POLICY: LazyLock<
    params::SledProvisionPolicyParams,
> = LazyLock::new(|| params::SledProvisionPolicyParams {
    state: SledProvisionPolicy::NonProvisionable,
});

pub static HARDWARE_SWITCH_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/system/hardware/switches/{}", SWITCH_UUID));
pub const HARDWARE_DISKS_URL: &'static str = "/v1/system/hardware/disks";
pub static HARDWARE_DISK_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/system/hardware/disks/{}", PHYSICAL_DISK_UUID)
});
pub static HARDWARE_SLED_DISK_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/system/hardware/sleds/{}/disks", SLED_AGENT_UUID)
});

pub static SLED_INSTANCES_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/system/hardware/sleds/{}/instances", SLED_AGENT_UUID)
});
pub static DEMO_UNINITIALIZED_SLED: LazyLock<params::UninitializedSledId> =
    LazyLock::new(|| params::UninitializedSledId {
        serial: "demo-serial".to_string(),
        part: "demo-part".to_string(),
    });

pub const SUPPORT_BUNDLES_URL: &'static str =
    "/experimental/v1/system/support-bundles";
pub static SUPPORT_BUNDLE_URL: LazyLock<String> =
    LazyLock::new(|| format!("{SUPPORT_BUNDLES_URL}/{{id}}"));

// Global policy
pub const SYSTEM_POLICY_URL: &'static str = "/v1/system/policy";

// Silo used for testing
pub static DEMO_SILO_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-silo".parse().unwrap());
pub static DEMO_SILO_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/system/silos/{}", *DEMO_SILO_NAME));
pub static DEMO_SILO_IP_POOLS_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/ip-pools", *DEMO_SILO_URL));
pub static DEMO_SILO_POLICY_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/system/silos/{}/policy", *DEMO_SILO_NAME));
pub static DEMO_SILO_QUOTAS_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/system/silos/{}/quotas", *DEMO_SILO_NAME));
pub static DEMO_SILO_CREATE: LazyLock<params::SiloCreate> =
    LazyLock::new(|| params::SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_SILO_NAME.clone(),
            description: String::from(""),
        },
        quotas: params::SiloQuotasCreate::arbitrarily_high_default(),
        discoverable: true,
        identity_mode: shared::SiloIdentityMode::SamlJit,
        admin_group_name: None,
        tls_certificates: vec![],
        mapped_fleet_roles: Default::default(),
    });

pub static DEMO_SILO_UTIL_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/system/utilization/silos/{}", *DEMO_SILO_NAME)
});

// Use the default Silo for testing the local IdP
pub static DEMO_SILO_USERS_CREATE_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/system/identity-providers/local/users?silo={}",
        DEFAULT_SILO.identity().name,
    )
});
pub static DEMO_SILO_USERS_LIST_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/system/users?silo={}", DEFAULT_SILO.identity().name,)
});
pub static DEMO_SILO_USER_ID_GET_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/system/users/{{id}}?silo={}", DEFAULT_SILO.identity().name,)
});
pub static DEMO_SILO_USER_ID_DELETE_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/system/identity-providers/local/users/{{id}}?silo={}",
            DEFAULT_SILO.identity().name,
        )
    });
pub static DEMO_SILO_USER_ID_SET_PASSWORD_URL: LazyLock<String> = LazyLock::new(
    || {
        format!(
            "/v1/system/identity-providers/local/users/{{id}}/set-password?silo={}",
            DEFAULT_SILO.identity().name,
        )
    },
);

// Project used for testing
pub static DEMO_PROJECT_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-project".parse().unwrap());
pub static DEMO_PROJECT_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/projects/{}", *DEMO_PROJECT_NAME));
pub static DEMO_PROJECT_SELECTOR: LazyLock<String> =
    LazyLock::new(|| format!("project={}", *DEMO_PROJECT_NAME));
pub static DEMO_PROJECT_POLICY_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/projects/{}/policy", *DEMO_PROJECT_NAME));
pub static DEMO_PROJECT_URL_IMAGES: LazyLock<String> =
    LazyLock::new(|| format!("/v1/images?project={}", *DEMO_PROJECT_NAME));
pub static DEMO_PROJECT_URL_INSTANCES: LazyLock<String> =
    LazyLock::new(|| format!("/v1/instances?project={}", *DEMO_PROJECT_NAME));
pub static DEMO_PROJECT_URL_AFFINITY_GROUPS: LazyLock<String> =
    LazyLock::new(|| {
        format!("/v1/affinity-groups?project={}", *DEMO_PROJECT_NAME)
    });
pub static DEMO_PROJECT_URL_ANTI_AFFINITY_GROUPS: LazyLock<String> =
    LazyLock::new(|| {
        format!("/v1/anti-affinity-groups?project={}", *DEMO_PROJECT_NAME)
    });
pub static DEMO_PROJECT_URL_SNAPSHOTS: LazyLock<String> =
    LazyLock::new(|| format!("/v1/snapshots?project={}", *DEMO_PROJECT_NAME));
pub static DEMO_PROJECT_URL_VPCS: LazyLock<String> =
    LazyLock::new(|| format!("/v1/vpcs?project={}", *DEMO_PROJECT_NAME));
pub static DEMO_PROJECT_URL_FIPS: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/floating-ips?project={}", *DEMO_PROJECT_NAME)
});
pub static DEMO_PROJECT_CREATE: LazyLock<params::ProjectCreate> =
    LazyLock::new(|| params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_PROJECT_NAME.clone(),
            description: String::from(""),
        },
    });

// VPC used for testing
pub static DEMO_VPC_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-vpc".parse().unwrap());
pub static DEMO_VPC_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/vpcs/{}?{}", *DEMO_VPC_NAME, *DEMO_PROJECT_SELECTOR)
});
pub static DEMO_VPC_SELECTOR: LazyLock<String> = LazyLock::new(|| {
    format!("project={}&vpc={}", *DEMO_PROJECT_NAME, *DEMO_VPC_NAME)
});
pub static DEMO_VPC_URL_FIREWALL_RULES: LazyLock<String> =
    LazyLock::new(|| format!("/v1/vpc-firewall-rules?{}", *DEMO_VPC_SELECTOR));
pub static DEMO_VPC_URL_ROUTERS: LazyLock<String> =
    LazyLock::new(|| format!("/v1/vpc-routers?{}", *DEMO_VPC_SELECTOR));
pub static DEMO_VPC_URL_SUBNETS: LazyLock<String> =
    LazyLock::new(|| format!("/v1/vpc-subnets?{}", *DEMO_VPC_SELECTOR));
pub static DEMO_VPC_CREATE: LazyLock<params::VpcCreate> =
    LazyLock::new(|| params::VpcCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_VPC_NAME.clone(),
            description: String::from(""),
        },
        ipv6_prefix: None,
        dns_name: DEMO_VPC_NAME.clone(),
    });

// VPC Subnet used for testing
pub static DEMO_VPC_SUBNET_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-vpc-subnet".parse().unwrap());
pub static DEMO_VPC_SUBNET_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/vpc-subnets/{}?{}", *DEMO_VPC_SUBNET_NAME, *DEMO_VPC_SELECTOR)
});
pub static DEMO_VPC_SUBNET_INTERFACES_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/vpc-subnets/{}/network-interfaces?{}",
            *DEMO_VPC_SUBNET_NAME, *DEMO_VPC_SELECTOR
        )
    });
pub static DEMO_VPC_SUBNET_CREATE: LazyLock<params::VpcSubnetCreate> =
    LazyLock::new(|| params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_VPC_SUBNET_NAME.clone(),
            description: String::from(""),
        },
        ipv4_block: "10.1.2.3/8".parse().unwrap(),
        ipv6_block: None,
        custom_router: None,
    });

// VPC Router used for testing
pub static DEMO_VPC_ROUTER_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-vpc-router".parse().unwrap());
pub static DEMO_VPC_ROUTER_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/vpc-routers/{}?project={}&vpc={}",
        *DEMO_VPC_ROUTER_NAME, *DEMO_PROJECT_NAME, *DEMO_VPC_NAME
    )
});
pub static DEMO_VPC_ROUTER_URL_ROUTES: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/vpc-router-routes?project={}&vpc={}&router={}",
        *DEMO_PROJECT_NAME, *DEMO_VPC_NAME, *DEMO_VPC_ROUTER_NAME
    )
});
pub static DEMO_VPC_ROUTER_CREATE: LazyLock<params::VpcRouterCreate> =
    LazyLock::new(|| params::VpcRouterCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_VPC_ROUTER_NAME.clone(),
            description: String::from(""),
        },
    });

// Router Route used for testing
pub static DEMO_ROUTER_ROUTE_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-router-route".parse().unwrap());
pub static DEMO_ROUTER_ROUTE_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/vpc-router-routes/{}?project={}&vpc={}&router={}",
        *DEMO_ROUTER_ROUTE_NAME,
        *DEMO_PROJECT_NAME,
        *DEMO_VPC_NAME,
        *DEMO_VPC_ROUTER_NAME
    )
});
pub static DEMO_ROUTER_ROUTE_CREATE: LazyLock<params::RouterRouteCreate> =
    LazyLock::new(|| params::RouterRouteCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_ROUTER_ROUTE_NAME.clone(),
            description: String::from(""),
        },
        target: RouteTarget::Ip(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1))),
        destination: RouteDestination::Subnet("loopback".parse().unwrap()),
    });

// Internet Gateway used for testing
pub static DEMO_INTERNET_GATEWAY_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-internet-gateway".parse().unwrap());
pub static DEMO_INTERNET_GATEWAYS_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/internet-gateways?project={}&vpc={}",
        *DEMO_PROJECT_NAME, *DEMO_VPC_NAME
    )
});
pub static DEMO_INTERNET_GATEWAY_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/internet-gateways/{}?project={}&vpc={}",
        *DEMO_INTERNET_GATEWAY_NAME, *DEMO_PROJECT_NAME, *DEMO_VPC_NAME
    )
});
pub static DEMO_INTERNET_GATEWAY_CREATE: LazyLock<
    params::InternetGatewayCreate,
> = LazyLock::new(|| params::InternetGatewayCreate {
    identity: IdentityMetadataCreateParams {
        name: DEMO_INTERNET_GATEWAY_NAME.clone(),
        description: String::from(""),
    },
});
pub static DEMO_INTERNET_GATEWAY_IP_POOL_CREATE: LazyLock<
    params::InternetGatewayIpPoolCreate,
> = LazyLock::new(|| params::InternetGatewayIpPoolCreate {
    identity: IdentityMetadataCreateParams {
        name: DEMO_INTERNET_GATEWAY_NAME.clone(),
        description: String::from(""),
    },
    ip_pool: NameOrId::Id(uuid::Uuid::new_v4()),
});
pub static DEMO_INTERNET_GATEWAY_IP_ADDRESS_CREATE: LazyLock<
    params::InternetGatewayIpAddressCreate,
> = LazyLock::new(|| params::InternetGatewayIpAddressCreate {
    identity: IdentityMetadataCreateParams {
        name: DEMO_INTERNET_GATEWAY_NAME.clone(),
        description: String::from(""),
    },
    address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
});
pub static DEMO_INTERNET_GATEWAY_IP_POOLS_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/internet-gateway-ip-pools?project={}&vpc={}&gateway={}",
            *DEMO_PROJECT_NAME, *DEMO_VPC_NAME, *DEMO_INTERNET_GATEWAY_NAME,
        )
    });
pub static DEMO_INTERNET_GATEWAY_IP_ADDRS_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/internet-gateway-ip-addresses?project={}&vpc={}&gateway={}",
            *DEMO_PROJECT_NAME, *DEMO_VPC_NAME, *DEMO_INTERNET_GATEWAY_NAME,
        )
    });
pub static DEMO_INTERNET_GATEWAY_IP_POOL_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-igw-pool".parse().unwrap());
pub static DEMO_INTERNET_GATEWAY_IP_ADDRESS_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-igw-address".parse().unwrap());
pub static DEMO_INTERNET_GATEWAY_IP_POOL_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/internet-gateway-ip-pools/{}?project={}&vpc={}&gateway={}",
            *DEMO_INTERNET_GATEWAY_IP_POOL_NAME,
            *DEMO_PROJECT_NAME,
            *DEMO_VPC_NAME,
            *DEMO_INTERNET_GATEWAY_NAME,
        )
    });
pub static DEMO_INTERNET_GATEWAY_IP_ADDR_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/internet-gateway-ip-addresses/{}?project={}&vpc={}&gateway={}",
            *DEMO_INTERNET_GATEWAY_IP_ADDRESS_NAME,
            *DEMO_PROJECT_NAME,
            *DEMO_VPC_NAME,
            *DEMO_INTERNET_GATEWAY_NAME,
        )
    });

// Disk used for testing
pub static DEMO_DISK_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-disk".parse().unwrap());

// TODO: Once we can test a URL multiple times we should also a case to exercise
// authz for disks filtered by instances
pub static DEMO_DISKS_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/disks?{}", *DEMO_PROJECT_SELECTOR));
pub static DEMO_DISK_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/disks/{}?{}", *DEMO_DISK_NAME, *DEMO_PROJECT_SELECTOR)
});
pub static DEMO_DISK_CREATE: LazyLock<params::DiskCreate> =
    LazyLock::new(|| {
        params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_DISK_NAME.clone(),
                description: "".parse().unwrap(),
            },
            disk_source: params::DiskSource::Blank {
                block_size: params::BlockSize::try_from(4096).unwrap(),
            },
            size: ByteCount::from_gibibytes_u32(
                // divide by at least two to leave space for snapshot blocks
                DiskTest::DEFAULT_ZPOOL_SIZE_GIB / 5,
            ),
        }
    });
pub static DEMO_DISK_METRICS_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/disks/{}/metrics/activated?start_time={:?}&end_time={:?}&{}",
        *DEMO_DISK_NAME,
        Utc::now(),
        Utc::now(),
        *DEMO_PROJECT_SELECTOR,
    )
});

// Related to importing blocks from an external source
pub static DEMO_IMPORT_DISK_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-import-disk".parse().unwrap());
pub static DEMO_IMPORT_DISK_CREATE: LazyLock<params::DiskCreate> =
    LazyLock::new(|| {
        params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_IMPORT_DISK_NAME.clone(),
                description: "".parse().unwrap(),
            },
            disk_source: params::DiskSource::ImportingBlocks {
                block_size: params::BlockSize::try_from(4096).unwrap(),
            },
            size: ByteCount::from_gibibytes_u32(
                // divide by at least two to leave space for snapshot blocks
                DiskTest::DEFAULT_ZPOOL_SIZE_GIB / 5,
            ),
        }
    });
pub static DEMO_IMPORT_DISK_BULK_WRITE_START_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/disks/{}/bulk-write-start?{}",
            *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_IMPORT_DISK_BULK_WRITE_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/disks/{}/bulk-write?{}",
            *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_IMPORT_DISK_BULK_WRITE_STOP_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/disks/{}/bulk-write-stop?{}",
            *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_IMPORT_DISK_FINALIZE_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/disks/{}/finalize?{}",
            *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR
        )
    });

// Affinity/Anti- group used for testing

pub static DEMO_AFFINITY_GROUP_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-affinity-group".parse().unwrap());
pub static DEMO_AFFINITY_GROUP_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/affinity-groups/{}?{}",
        *DEMO_AFFINITY_GROUP_NAME, *DEMO_PROJECT_SELECTOR
    )
});
pub static DEMO_AFFINITY_GROUP_MEMBERS_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/affinity-groups/{}/members?{}",
            *DEMO_AFFINITY_GROUP_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_AFFINITY_GROUP_INSTANCE_MEMBER_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/affinity-groups/{}/members/instance/{}?{}",
            *DEMO_AFFINITY_GROUP_NAME,
            *DEMO_STOPPED_INSTANCE_NAME,
            *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_AFFINITY_GROUP_CREATE: LazyLock<params::AffinityGroupCreate> =
    LazyLock::new(|| params::AffinityGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_AFFINITY_GROUP_NAME.clone(),
            description: String::from(""),
        },
        policy: AffinityPolicy::Allow,
        failure_domain: FailureDomain::Sled,
    });
pub static DEMO_AFFINITY_GROUP_UPDATE: LazyLock<params::AffinityGroupUpdate> =
    LazyLock::new(|| params::AffinityGroupUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some(String::from("an updated description")),
        },
    });

pub static DEMO_ANTI_AFFINITY_GROUP_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-anti-affinity-group".parse().unwrap());
pub static DEMO_ANTI_AFFINITY_GROUPS_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!("/v1/anti-affinity-groups?{}", *DEMO_PROJECT_SELECTOR)
    });
pub static DEMO_ANTI_AFFINITY_GROUP_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/anti-affinity-groups/{}?{}",
            *DEMO_ANTI_AFFINITY_GROUP_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_ANTI_AFFINITY_GROUP_MEMBERS_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/anti-affinity-groups/{}/members?{}",
            *DEMO_ANTI_AFFINITY_GROUP_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_ANTI_AFFINITY_GROUP_INSTANCE_MEMBER_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/anti-affinity-groups/{}/members/instance/{}?{}",
            *DEMO_ANTI_AFFINITY_GROUP_NAME,
            *DEMO_STOPPED_INSTANCE_NAME,
            *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_ANTI_AFFINITY_GROUP_CREATE: LazyLock<
    params::AntiAffinityGroupCreate,
> = LazyLock::new(|| params::AntiAffinityGroupCreate {
    identity: IdentityMetadataCreateParams {
        name: DEMO_ANTI_AFFINITY_GROUP_NAME.clone(),
        description: String::from(""),
    },
    policy: AffinityPolicy::Allow,
    failure_domain: FailureDomain::Sled,
});
pub static DEMO_ANTI_AFFINITY_GROUP_UPDATE: LazyLock<
    params::AntiAffinityGroupUpdate,
> = LazyLock::new(|| params::AntiAffinityGroupUpdate {
    identity: IdentityMetadataUpdateParams {
        name: None,
        description: Some(String::from("an updated description")),
    },
});

// Instance used for testing
pub static DEMO_INSTANCE_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-instance".parse().unwrap());
pub static DEMO_STOPPED_INSTANCE_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-stopped-instance".parse().unwrap());
pub static DEMO_INSTANCE_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/instances/{}?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR)
});
pub static DEMO_INSTANCE_START_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/instances/{}/start?{}",
        *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
    )
});
pub static DEMO_INSTANCE_STOP_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/instances/{}/stop?{}",
        *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
    )
});
pub static DEMO_INSTANCE_REBOOT_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/instances/{}/reboot?{}",
        *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
    )
});
pub static DEMO_INSTANCE_SERIAL_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/instances/{}/serial-console?{}",
        *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
    )
});
pub static DEMO_INSTANCE_SERIAL_STREAM_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/instances/{}/serial-console/stream?{}",
            *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_INSTANCE_DISKS_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/instances/{}/disks?{}",
        *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
    )
});
pub static DEMO_INSTANCE_DISKS_ATTACH_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/instances/{}/disks/attach?{}",
            *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_INSTANCE_DISKS_DETACH_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/instances/{}/disks/detach?{}",
            *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_INSTANCE_AFFINITY_GROUPS_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/instances/{}/affinity-groups?{}",
            *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_INSTANCE_ANTI_AFFINITY_GROUPS_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/instances/{}/anti-affinity-groups?{}",
            *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_INSTANCE_EPHEMERAL_IP_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/instances/{}/external-ips/ephemeral?{}",
            *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_INSTANCE_SSH_KEYS_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/instances/{}/ssh-public-keys?{}",
        *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
    )
});
pub static DEMO_INSTANCE_NICS_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/network-interfaces?project={}&instance={}",
        *DEMO_PROJECT_NAME, *DEMO_INSTANCE_NAME
    )
});
pub static DEMO_INSTANCE_EXTERNAL_IPS_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/instances/{}/external-ips?{}",
            *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_INSTANCE_CREATE: LazyLock<params::InstanceCreate> =
    LazyLock::new(|| params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_INSTANCE_NAME.clone(),
            description: String::from(""),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from_gibibytes_u32(16),
        hostname: "demo-instance".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: Some(Vec::new()),
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool: Some(DEMO_IP_POOL_NAME.clone().into()),
        }],
        disks: vec![],
        boot_disk: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    });
pub static DEMO_STOPPED_INSTANCE_CREATE: LazyLock<params::InstanceCreate> =
    LazyLock::new(|| params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_STOPPED_INSTANCE_NAME.clone(),
            description: String::from(""),
        },
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from_gibibytes_u32(16),
        hostname: "demo-instance".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: Some(Vec::new()),
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![params::ExternalIpCreate::Ephemeral {
            pool: Some(DEMO_IP_POOL_NAME.clone().into()),
        }],
        disks: vec![],
        boot_disk: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    });
pub static DEMO_INSTANCE_UPDATE: LazyLock<params::InstanceUpdate> =
    LazyLock::new(|| params::InstanceUpdate {
        boot_disk: None,
        auto_restart_policy: None,
        ncpus: InstanceCpuCount(1),
        memory: ByteCount::from_gibibytes_u32(16),
    });

// The instance needs a network interface, too.
pub static DEMO_INSTANCE_NIC_NAME: LazyLock<Name> =
    LazyLock::new(|| nexus_defaults::DEFAULT_PRIMARY_NIC_NAME.parse().unwrap());
pub static DEMO_INSTANCE_NIC_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/network-interfaces/{}?project={}&instance={}",
        *DEMO_INSTANCE_NIC_NAME, *DEMO_PROJECT_NAME, *DEMO_INSTANCE_NAME
    )
});
pub static DEMO_INSTANCE_NIC_CREATE: LazyLock<
    params::InstanceNetworkInterfaceCreate,
> = LazyLock::new(|| params::InstanceNetworkInterfaceCreate {
    identity: IdentityMetadataCreateParams {
        name: DEMO_INSTANCE_NIC_NAME.clone(),
        description: String::from(""),
    },
    vpc_name: DEMO_VPC_NAME.clone(),
    subnet_name: DEMO_VPC_SUBNET_NAME.clone(),
    ip: None,
});
pub static DEMO_INSTANCE_NIC_PUT: LazyLock<
    params::InstanceNetworkInterfaceUpdate,
> = LazyLock::new(|| params::InstanceNetworkInterfaceUpdate {
    identity: IdentityMetadataUpdateParams {
        name: None,
        description: Some(String::from("an updated description")),
    },
    primary: false,
    transit_ips: vec![],
});

pub static DEMO_CERTIFICATE_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-certificate".parse().unwrap());
pub const DEMO_CERTIFICATES_URL: &'static str = "/v1/certificates";
pub const DEMO_CERTIFICATE_URL: &'static str =
    "/v1/certificates/demo-certificate";
pub static DEMO_CERTIFICATE: LazyLock<CertificateChain> = LazyLock::new(|| {
    CertificateChain::new(format!("*.sys.{DNS_ZONE_EXTERNAL_TESTING}"))
});
pub static DEMO_CERTIFICATE_CREATE: LazyLock<params::CertificateCreate> =
    LazyLock::new(|| params::CertificateCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_CERTIFICATE_NAME.clone(),
            description: String::from(""),
        },
        cert: DEMO_CERTIFICATE.cert_chain_as_pem(),
        key: DEMO_CERTIFICATE.end_cert_private_key_as_pem(),
        service: shared::ServiceUsingCertificate::ExternalApi,
    });

pub const DEMO_SWITCH_PORT_URL: &'static str =
    "/v1/system/hardware/switch-port";
pub static DEMO_SWITCH_PORT_SETTINGS_APPLY_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/system/hardware/switch-port/qsfp7/settings?rack_id={}&switch_location={}",
            uuid::Uuid::new_v4(),
            "switch0",
        )
    });
pub static DEMO_SWITCH_PORT_SETTINGS: LazyLock<
    params::SwitchPortApplySettings,
> = LazyLock::new(|| params::SwitchPortApplySettings {
    port_settings: NameOrId::Name("portofino".parse().unwrap()),
});
/* TODO requires dpd access
pub static DEMO_SWITCH_PORT_STATUS_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/system/hardware/switch-port/qsfp7/status?rack_id={}&switch_location={}",
        uuid::Uuid::new_v4(),
        "switch0",
    )
});
*/

pub static DEMO_LOOPBACK_CREATE_URL: LazyLock<String> =
    LazyLock::new(|| "/v1/system/networking/loopback-address".into());
pub static DEMO_LOOPBACK_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/system/networking/loopback-address/{}/{}/{}",
        uuid::Uuid::new_v4(),
        "switch0",
        "203.0.113.99/24",
    )
});
pub static DEMO_LOOPBACK_CREATE: LazyLock<params::LoopbackAddressCreate> =
    LazyLock::new(|| params::LoopbackAddressCreate {
        address_lot: NameOrId::Name("parkinglot".parse().unwrap()),
        rack_id: uuid::Uuid::new_v4(),
        switch_location: "switch0".parse().unwrap(),
        address: "203.0.113.99".parse().unwrap(),
        mask: 24,
        anycast: false,
    });

pub const DEMO_SWITCH_PORT_SETTINGS_URL: &'static str =
    "/v1/system/networking/switch-port-settings?port_settings=portofino";
pub const DEMO_SWITCH_PORT_SETTINGS_INFO_URL: &'static str =
    "/v1/system/networking/switch-port-settings/protofino";
pub static DEMO_SWITCH_PORT_SETTINGS_CREATE: LazyLock<
    params::SwitchPortSettingsCreate,
> = LazyLock::new(|| {
    params::SwitchPortSettingsCreate::new(IdentityMetadataCreateParams {
        name: "portofino".parse().unwrap(),
        description: "just a port".into(),
    })
});

pub const DEMO_ADDRESS_LOTS_URL: &'static str =
    "/v1/system/networking/address-lot";
pub const DEMO_ADDRESS_LOT_URL: &'static str =
    "/v1/system/networking/address-lot/parkinglot";
pub const DEMO_ADDRESS_LOT_BLOCKS_URL: &'static str =
    "/v1/system/networking/address-lot/parkinglot/blocks";
pub static DEMO_ADDRESS_LOT_CREATE: LazyLock<params::AddressLotCreate> =
    LazyLock::new(|| params::AddressLotCreate {
        identity: IdentityMetadataCreateParams {
            name: "parkinglot".parse().unwrap(),
            description: "an address parking lot".into(),
        },
        kind: AddressLotKind::Infra,
        blocks: vec![params::AddressLotBlockCreate {
            first_address: "203.0.113.10".parse().unwrap(),
            last_address: "203.0.113.20".parse().unwrap(),
        }],
    });

pub const DEMO_BGP_CONFIG_CREATE_URL: &'static str =
    "/v1/system/networking/bgp?name_or_id=as47";
pub static DEMO_BGP_CONFIG: LazyLock<params::BgpConfigCreate> =
    LazyLock::new(|| params::BgpConfigCreate {
        identity: IdentityMetadataCreateParams {
            name: "as47".parse().unwrap(),
            description: "BGP config for AS47".into(),
        },
        bgp_announce_set_id: NameOrId::Name("instances".parse().unwrap()),
        asn: 47,
        vrf: None,
        checker: None,
        shaper: None,
    });
pub const DEMO_BGP_ANNOUNCE_SET_URL: &'static str =
    "/v1/system/networking/bgp-announce-set";
pub static DEMO_BGP_ANNOUNCE: LazyLock<params::BgpAnnounceSetCreate> =
    LazyLock::new(|| params::BgpAnnounceSetCreate {
        identity: IdentityMetadataCreateParams {
            name: "a-bag-of-addrs".parse().unwrap(),
            description: "a bag of addrs".into(),
        },
        announcement: vec![params::BgpAnnouncementCreate {
            address_lot_block: NameOrId::Name("some-block".parse().unwrap()),
            network: "10.0.0.0/16".parse().unwrap(),
        }],
    });
pub const DEMO_BGP_ANNOUNCE_SET_DELETE_URL: &'static str =
    "/v1/system/networking/bgp-announce-set/a-bag-of-addrs";
pub const DEMO_BGP_ANNOUNCEMENT_URL: &'static str =
    "/v1/system/networking/bgp-announce-set/a-bag-of-addrs/announcement";
pub const DEMO_BGP_STATUS_URL: &'static str =
    "/v1/system/networking/bgp-status";
pub const DEMO_BGP_EXPORTED_URL: &'static str =
    "/v1/system/networking/bgp-exported";
pub const DEMO_BGP_ROUTES_IPV4_URL: &'static str =
    "/v1/system/networking/bgp-routes-ipv4?asn=47";
pub const DEMO_BGP_MESSAGE_HISTORY_URL: &'static str =
    "/v1/system/networking/bgp-message-history?asn=47";

pub const DEMO_BFD_STATUS_URL: &'static str =
    "/v1/system/networking/bfd-status";

pub const DEMO_BFD_ENABLE_URL: &'static str =
    "/v1/system/networking/bfd-enable";

pub const DEMO_BFD_DISABLE_URL: &'static str =
    "/v1/system/networking/bfd-disable";

pub static DEMO_BFD_ENABLE: LazyLock<params::BfdSessionEnable> =
    LazyLock::new(|| params::BfdSessionEnable {
        local: None,
        remote: "10.0.0.1".parse().unwrap(),
        detection_threshold: 3,
        required_rx: 1000000,
        switch: "switch0".parse().unwrap(),
        mode: omicron_common::api::external::BfdMode::MultiHop,
    });

pub static DEMO_BFD_DISABLE: LazyLock<params::BfdSessionDisable> =
    LazyLock::new(|| params::BfdSessionDisable {
        remote: "10.0.0.1".parse().unwrap(),
        switch: "switch0".parse().unwrap(),
    });

// Project Images
pub static DEMO_IMAGE_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-image".parse().unwrap());
pub static DEMO_PROJECT_IMAGES_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/images?project={}", *DEMO_PROJECT_NAME));
pub static DEMO_PROJECT_IMAGE_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/images/{}?project={}", *DEMO_IMAGE_NAME, *DEMO_PROJECT_NAME)
});
pub static DEMO_PROJECT_PROMOTE_IMAGE_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/images/{}/promote?project={}",
            *DEMO_IMAGE_NAME, *DEMO_PROJECT_NAME
        )
    });

pub static DEMO_SILO_DEMOTE_IMAGE_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/images/{}/demote?project={}",
        *DEMO_IMAGE_NAME, *DEMO_PROJECT_NAME
    )
});

pub static DEMO_IMAGE_CREATE: LazyLock<params::ImageCreate> =
    LazyLock::new(|| params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_IMAGE_NAME.clone(),
            description: String::from(""),
        },
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
        os: "fake-os".to_string(),
        version: "1.0".to_string(),
    });

// IP Pools
pub static DEMO_IP_POOLS_PROJ_URL: LazyLock<String> =
    LazyLock::new(|| "/v1/ip-pools".to_string());
pub const DEMO_IP_POOLS_URL: &'static str = "/v1/system/ip-pools";
pub static DEMO_IP_POOL_NAME: LazyLock<Name> =
    LazyLock::new(|| "default".parse().unwrap());
pub static DEMO_IP_POOL_CREATE: LazyLock<params::IpPoolCreate> =
    LazyLock::new(|| params::IpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_IP_POOL_NAME.clone(),
            description: String::from("an IP pool"),
        },
    });
pub static DEMO_IP_POOL_PROJ_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/ip-pools/{}?project={}",
        *DEMO_IP_POOL_NAME, *DEMO_PROJECT_NAME
    )
});
pub static DEMO_IP_POOL_URL: LazyLock<String> =
    LazyLock::new(|| format!("/v1/system/ip-pools/{}", *DEMO_IP_POOL_NAME));
pub static DEMO_IP_POOL_UTILIZATION_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/utilization", *DEMO_IP_POOL_URL));
pub static DEMO_IP_POOL_UPDATE: LazyLock<params::IpPoolUpdate> =
    LazyLock::new(|| params::IpPoolUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some(String::from("a new IP pool")),
        },
    });
pub static DEMO_IP_POOL_SILOS_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/silos", *DEMO_IP_POOL_URL));
pub static DEMO_IP_POOL_SILOS_BODY: LazyLock<params::IpPoolLinkSilo> =
    LazyLock::new(|| params::IpPoolLinkSilo {
        silo: NameOrId::Id(DEFAULT_SILO.identity().id),
        is_default: true, // necessary for demo instance create to go through
    });

pub static DEMO_IP_POOL_SILO_URL: LazyLock<String> = LazyLock::new(|| {
    format!("{}/silos/{}", *DEMO_IP_POOL_URL, *DEMO_SILO_NAME)
});
pub static DEMO_IP_POOL_SILO_UPDATE_BODY: LazyLock<params::IpPoolSiloUpdate> =
    LazyLock::new(|| params::IpPoolSiloUpdate { is_default: false });

pub static DEMO_IP_POOL_RANGE: LazyLock<IpRange> = LazyLock::new(|| {
    IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(10, 0, 0, 0),
            std::net::Ipv4Addr::new(10, 0, 0, 255),
        )
        .unwrap(),
    )
});
pub static DEMO_IP_POOL_RANGES_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/ranges", *DEMO_IP_POOL_URL));
pub static DEMO_IP_POOL_RANGES_ADD_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/add", *DEMO_IP_POOL_RANGES_URL));
pub static DEMO_IP_POOL_RANGES_DEL_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/remove", *DEMO_IP_POOL_RANGES_URL));

// IP Pools (Services)
pub const DEMO_IP_POOL_SERVICE_URL: &'static str =
    "/v1/system/ip-pools-service";
pub static DEMO_IP_POOL_SERVICE_RANGES_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/ranges", DEMO_IP_POOL_SERVICE_URL));
pub static DEMO_IP_POOL_SERVICE_RANGES_ADD_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/add", *DEMO_IP_POOL_SERVICE_RANGES_URL));
pub static DEMO_IP_POOL_SERVICE_RANGES_DEL_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/remove", *DEMO_IP_POOL_SERVICE_RANGES_URL));

// Snapshots
pub static DEMO_SNAPSHOT_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-snapshot".parse().unwrap());
pub static DEMO_SNAPSHOT_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/snapshots/{}?project={}",
        *DEMO_SNAPSHOT_NAME, *DEMO_PROJECT_NAME
    )
});
pub static DEMO_SNAPSHOT_CREATE: LazyLock<params::SnapshotCreate> =
    LazyLock::new(|| params::SnapshotCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_SNAPSHOT_NAME.clone(),
            description: String::from(""),
        },
        disk: DEMO_DISK_NAME.clone().into(),
    });

// SSH keys
pub const DEMO_SSHKEYS_URL: &'static str = "/v1/me/ssh-keys";
pub static DEMO_SSHKEY_NAME: LazyLock<Name> =
    LazyLock::new(|| "aaaaa-ssh-key".parse().unwrap());

pub static DEMO_SSHKEY_CREATE: LazyLock<params::SshKeyCreate> =
    LazyLock::new(|| params::SshKeyCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_SSHKEY_NAME.clone(),
            description: "a demo key".to_string(),
        },

        public_key: "AAAAAAAAAAAAAAA".to_string(),
    });

pub static DEMO_SPECIFIC_SSHKEY_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/{}", DEMO_SSHKEYS_URL, *DEMO_SSHKEY_NAME));

// Project Floating IPs
pub static DEMO_FLOAT_IP_NAME: LazyLock<Name> =
    LazyLock::new(|| "float-ip".parse().unwrap());

pub static DEMO_FLOAT_IP_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/floating-ips/{}?project={}",
        *DEMO_FLOAT_IP_NAME, *DEMO_PROJECT_NAME
    )
});

pub static DEMO_FLOATING_IP_ATTACH_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/floating-ips/{}/attach?{}",
            *DEMO_FLOAT_IP_NAME, *DEMO_PROJECT_SELECTOR
        )
    });
pub static DEMO_FLOATING_IP_DETACH_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/floating-ips/{}/detach?{}",
            *DEMO_FLOAT_IP_NAME, *DEMO_PROJECT_SELECTOR
        )
    });

pub static DEMO_FLOAT_IP_CREATE: LazyLock<params::FloatingIpCreate> =
    LazyLock::new(|| params::FloatingIpCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_FLOAT_IP_NAME.clone(),
            description: String::from("a new IP pool"),
        },
        ip: Some(std::net::Ipv4Addr::new(10, 0, 0, 141).into()),
        pool: None,
    });

pub static DEMO_FLOAT_IP_UPDATE: LazyLock<params::FloatingIpUpdate> =
    LazyLock::new(|| params::FloatingIpUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some(String::from("an updated Floating IP")),
        },
    });

pub static DEMO_FLOAT_IP_ATTACH: LazyLock<params::FloatingIpAttach> =
    LazyLock::new(|| params::FloatingIpAttach {
        kind: params::FloatingIpParentKind::Instance,
        parent: DEMO_FLOAT_IP_NAME.clone().into(),
    });
pub static DEMO_EPHEMERAL_IP_ATTACH: LazyLock<params::EphemeralIpCreate> =
    LazyLock::new(|| params::EphemeralIpCreate { pool: None });
// Identity providers
pub const IDENTITY_PROVIDERS_URL: &'static str =
    "/v1/system/identity-providers?silo=demo-silo";
pub const SAML_IDENTITY_PROVIDERS_URL: &'static str =
    "/v1/system/identity-providers/saml?silo=demo-silo";
pub static DEMO_SAML_IDENTITY_PROVIDER_NAME: LazyLock<Name> =
    LazyLock::new(|| "demo-saml-provider".parse().unwrap());

pub static SPECIFIC_SAML_IDENTITY_PROVIDER_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/system/identity-providers/saml/{}?silo=demo-silo",
            *DEMO_SAML_IDENTITY_PROVIDER_NAME
        )
    });

pub static SAML_IDENTITY_PROVIDER: LazyLock<
    params::SamlIdentityProviderCreate,
> = LazyLock::new(|| params::SamlIdentityProviderCreate {
    identity: IdentityMetadataCreateParams {
        name: DEMO_SAML_IDENTITY_PROVIDER_NAME.clone(),
        description: "a demo provider".to_string(),
    },

    idp_metadata_source: params::IdpMetadataSource::Url {
        url: HTTP_SERVER.url("/descriptor").to_string(),
    },

    idp_entity_id: "entity_id".to_string(),
    sp_client_id: "client_id".to_string(),
    acs_url: "http://acs".to_string(),
    slo_url: "http://slo".to_string(),
    technical_contact_email: "technical@fake".to_string(),

    signing_keypair: None,

    group_attribute_name: None,
});

pub static DEMO_SYSTEM_METRICS_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/system/metrics/virtual_disk_space_provisioned?start_time={:?}&end_time={:?}",
        Utc::now(),
        Utc::now(),
    )
});

pub static DEMO_SILO_METRICS_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/v1/metrics/virtual_disk_space_provisioned?start_time={:?}&end_time={:?}",
        Utc::now(),
        Utc::now(),
    )
});

pub static TIMESERIES_QUERY_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/timeseries/query?project={}", *DEMO_PROJECT_NAME)
});

pub static SYSTEM_TIMESERIES_LIST_URL: LazyLock<String> =
    LazyLock::new(|| String::from("/v1/system/timeseries/schemas"));

pub static SYSTEM_TIMESERIES_QUERY_URL: LazyLock<String> =
    LazyLock::new(|| String::from("/v1/system/timeseries/query"));

pub static DEMO_TIMESERIES_QUERY: LazyLock<params::TimeseriesQuery> =
    LazyLock::new(|| params::TimeseriesQuery {
        query: String::from("get http_service:request_latency_histogram"),
    });

// Users
pub static DEMO_USER_CREATE: LazyLock<test_params::UserCreate> =
    LazyLock::new(|| test_params::UserCreate {
        external_id: UserId::from_str("dummy-user").unwrap(),
        password: test_params::UserPassword::LoginDisallowed,
    });

// Allowlist for user-facing services.
pub static ALLOW_LIST_URL: LazyLock<String> =
    LazyLock::new(|| String::from("/v1/system/networking/allow-list"));
pub static ALLOW_LIST_UPDATE: LazyLock<params::AllowListUpdate> =
    LazyLock::new(|| params::AllowListUpdate {
        allowed_ips: AllowedSourceIps::Any,
    });

// Updates
pub static DEMO_TARGET_RELEASE: LazyLock<params::SetTargetReleaseParams> =
    LazyLock::new(|| params::SetTargetReleaseParams {
        system_version: Version::new(0, 0, 0),
    });

// Alerts
pub static ALERT_CLASSES_URL: &'static str = "/v1/alert-classes";
pub static ALERT_RECEIVERS_URL: &'static str = "/v1/alert-receivers";
pub static WEBHOOK_RECEIVERS_URL: &'static str = "/v1/webhook-receivers";

pub static DEMO_WEBHOOK_RECEIVER_NAME: LazyLock<Name> =
    LazyLock::new(|| "my-great-webhook".parse().unwrap());
pub static DEMO_WEBHOOK_RECEIVER_CREATE: LazyLock<params::WebhookCreate> =
    LazyLock::new(|| params::WebhookCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_WEBHOOK_RECEIVER_NAME.clone(),
            description: "webhook, line, and sinker".to_string(),
        },
        endpoint: "https://example.com/my-great-webhook".parse().unwrap(),
        secrets: vec!["my cool secret".to_string()],
        subscriptions: vec![
            "test.foo.bar".parse().unwrap(),
            "test.*".parse().unwrap(),
        ],
    });

pub static DEMO_WEBHOOK_RECEIVER_UPDATE: LazyLock<
    params::WebhookReceiverUpdate,
> = LazyLock::new(|| params::WebhookReceiverUpdate {
    identity: IdentityMetadataUpdateParams {
        name: None,
        description: Some("webhooked on phonics".to_string()),
    },
    endpoint: Some("https://example.com/my-cool-webhook".parse().unwrap()),
});

pub static DEMO_ALERT_RECEIVER_URL: LazyLock<String> = LazyLock::new(|| {
    format!("{ALERT_RECEIVERS_URL}/{}", *DEMO_WEBHOOK_RECEIVER_NAME)
});
pub static DEMO_WEBHOOK_RECEIVER_URL: LazyLock<String> = LazyLock::new(|| {
    format!("{WEBHOOK_RECEIVERS_URL}/{}", *DEMO_WEBHOOK_RECEIVER_NAME)
});

pub static DEMO_ALERT_RECEIVER_PROBE_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/probe", *DEMO_ALERT_RECEIVER_URL));

pub static DEMO_ALERT_DELIVERIES_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/deliveries", *DEMO_ALERT_RECEIVER_URL));

pub static DEMO_ALERT_SUBSCRIPTIONS_URL: LazyLock<String> =
    LazyLock::new(|| format!("{}/subscriptions", *DEMO_ALERT_RECEIVER_URL));

pub static DEMO_ALERT_SUBSCRIPTION: LazyLock<shared::AlertSubscription> =
    LazyLock::new(|| "test.foo.**".parse().unwrap());

pub static DEMO_ALERT_SUBSCRIPTION_CREATE: LazyLock<
    params::AlertSubscriptionCreate,
> = LazyLock::new(|| params::AlertSubscriptionCreate {
    subscription: DEMO_ALERT_SUBSCRIPTION.clone(),
});

pub static DEMO_ALERT_SUBSCRIPTION_DELETE_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "{}/subscriptions/{}",
            *DEMO_ALERT_RECEIVER_URL, *DEMO_ALERT_SUBSCRIPTION,
        )
    });

pub static DEMO_WEBHOOK_SECRETS_URL: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/webhook-secrets?receiver={}", *DEMO_WEBHOOK_RECEIVER_NAME)
});

pub static DEMO_WEBHOOK_SECRET_DELETE_URL: LazyLock<String> =
    LazyLock::new(|| {
        format!(
            "/v1/webhook-secrets/{{id}}?receiver={}",
            *DEMO_WEBHOOK_RECEIVER_NAME
        )
    });

pub static DEMO_WEBHOOK_SECRET_CREATE: LazyLock<params::WebhookSecretCreate> =
    LazyLock::new(|| params::WebhookSecretCreate {
        secret: "TRUSTNO1".to_string(),
    });

/// Describes an API endpoint to be verified by the "unauthorized" test
///
/// These structs are also used to check whether we're covering all endpoints in
/// the public OpenAPI spec.
#[derive(Debug)]
pub struct VerifyEndpoint {
    /// URL path for the HTTP resource to test
    ///
    /// Note that we might talk about the "GET organization" endpoint, and might
    /// write that "GET /organizations/{organization_name}".  But the URL here
    /// is for a specific HTTP resource, so it would look like
    /// "/organizations/demo-org" rather than
    /// "/organizations/{organization_name}".
    pub url: &'static str,

    /// Specifies whether an HTTP resource handled by this endpoint is visible
    /// to unauthenticated or unauthorized users
    ///
    /// If it's [`Visibility::Public`] (like "/v1/organizations"), unauthorized
    /// users can expect to get back a 401 or 403 when they attempt to access
    /// it.  If it's [`Visibility::Protected`] (like a specific Organization),
    /// unauthorized users will get a 404.
    pub visibility: Visibility,

    /// Specify level of unprivileged access an authenticated user has
    pub unprivileged_access: UnprivilegedAccess,

    /// Specifies what HTTP methods are supported for this HTTP resource
    ///
    /// The test runner tests a variety of HTTP methods.  For each method, if
    /// it's not in this list, we expect a 405 "Method Not Allowed" response.
    /// For `PUT` and `POST`, the item in `allowed_methods` also contains the
    /// contents of the body to send with the `PUT` or `POST` request.  This
    /// should be valid input for the endpoint.  Otherwise, Nexus could choose
    /// to fail with a 400-level validation error, which would obscure the
    /// authn/authz error we're looking for.
    pub allowed_methods: Vec<AllowedMethod>,
}

/// Describe what access authenticated unprivileged users have.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum UnprivilegedAccess {
    /// Users have full CRUD access to the endpoint
    Full,

    /// Users only have read access to the endpoint
    ReadOnly,

    /// Users have no access at all to the endpoint
    None,
}

/// Describes the visibility of an HTTP resource
#[derive(Debug)]
pub enum Visibility {
    /// All users can see the resource (including unauthenticated or
    /// unauthorized users)
    ///
    /// "/v1/organizations" is Public, for example.
    Public,

    /// Only users with certain privileges can see this endpoint
    ///
    /// "/organizations/demo-org" is not public, for example.
    Protected,
}

/// Describes an HTTP method supported by a particular API endpoint
#[derive(Debug)]
pub enum AllowedMethod {
    /// HTTP "DELETE" method
    Delete,
    /// HTTP "GET" method
    Get,
    /// HTTP "GET" method, but where we cannot statically define a URL that will
    /// work (so the test runner should not expect to get a 200).  This should
    /// be uncommon.  In most cases, resources are identified either by names
    /// that we define here or uuids that we control in the test suite (e.g.,
    /// the rack and sled uuids).
    ///
    /// This is not necessary for methods other than `GET`.  We only need this
    /// to configure the test's expectation for *privileged* requests.  For the
    /// other HTTP methods, we only make unprivileged requests, and they should
    /// always fail in the correct way.
    GetNonexistent,
    /// HTTP "GET" method that is not yet implemented
    ///
    /// This should be a transient state, used only for stub APIs.
    ///
    /// This is not necessary for methods other than `GET`.  We only need this
    /// to configure the test's expectation for *privileged* requests.  For the
    /// other HTTP methods, we only make unprivileged requests, and they should
    /// always fail in the correct way.
    #[allow(dead_code)]
    GetUnimplemented,
    /// HTTP "GET" method, but where the response data may change for reasons
    /// other than successful user interaction.  This should be uncommon; in
    /// most cases resources do not change merely due to the passage of time,
    /// although one common case is when the response data is updated by a
    /// background task.
    GetVolatile,
    /// HTTP "GET" method with websocket handshake headers.
    GetWebsocket,
    /// HTTP "POST" method, with sample input (which should be valid input for
    /// this endpoint)
    Post(serde_json::Value),
    /// HTTP "PUT" method, with sample input (which should be valid input for
    /// this endpoint)
    Put(serde_json::Value),
}

impl AllowedMethod {
    /// Returns the [`http::Method`] used to make a request for this HTTP method
    pub fn http_method(&self) -> &'static http::Method {
        match self {
            AllowedMethod::Delete => &Method::DELETE,
            AllowedMethod::Get
            | AllowedMethod::GetNonexistent
            | AllowedMethod::GetUnimplemented
            | AllowedMethod::GetVolatile
            | AllowedMethod::GetWebsocket => &Method::GET,
            AllowedMethod::Post(_) => &Method::POST,
            AllowedMethod::Put(_) => &Method::PUT,
        }
    }

    /// Returns a JSON value suitable for use as the request body when making a
    /// request to a specific endpoint using this HTTP method
    ///
    /// If this returns `None`, the request body should be empty.
    pub fn body(&self) -> Option<&serde_json::Value> {
        match self {
            AllowedMethod::Delete
            | AllowedMethod::Get
            | AllowedMethod::GetNonexistent
            | AllowedMethod::GetUnimplemented
            | AllowedMethod::GetVolatile
            | AllowedMethod::GetWebsocket => None,
            AllowedMethod::Post(body) => Some(&body),
            AllowedMethod::Put(body) => Some(&body),
        }
    }
}

pub static URL_USERS_DB_INIT: LazyLock<String> = LazyLock::new(|| {
    format!("/v1/system/users-builtin/{}", authn::USER_DB_INIT.name)
});

/// List of endpoints to be verified
pub static VERIFY_ENDPOINTS: LazyLock<Vec<VerifyEndpoint>> =
    LazyLock::new(|| {
        vec![
            // Global IAM policy
            VerifyEndpoint {
                url: &SYSTEM_POLICY_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&shared::Policy::<
                            shared::FleetRole,
                        > {
                            role_assignments: vec![],
                        })
                        .unwrap(),
                    ),
                ],
            },
            // IP Pools top-level endpoint
            VerifyEndpoint {
                url: &DEMO_IP_POOLS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_IP_POOL_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_IP_POOLS_PROJ_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            // Single IP Pool endpoint
            VerifyEndpoint {
                url: &DEMO_IP_POOL_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_IP_POOL_UPDATE).unwrap(),
                    ),
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_IP_POOL_PROJ_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            // IP pool silos endpoint
            VerifyEndpoint {
                url: &DEMO_IP_POOL_SILOS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_IP_POOL_SILOS_BODY)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_IP_POOL_SILO_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Delete,
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_IP_POOL_SILO_UPDATE_BODY)
                            .unwrap(),
                    ),
                ],
            },
            // IP Pool ranges endpoint
            VerifyEndpoint {
                url: &DEMO_IP_POOL_RANGES_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            // IP Pool ranges/add endpoint
            VerifyEndpoint {
                url: &DEMO_IP_POOL_RANGES_ADD_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap(),
                )],
            },
            // IP Pool ranges/delete endpoint
            VerifyEndpoint {
                url: &DEMO_IP_POOL_RANGES_DEL_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap(),
                )],
            },
            // IP pool utilization
            VerifyEndpoint {
                url: &DEMO_IP_POOL_UTILIZATION_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            // IP Pool endpoint (Oxide services)
            VerifyEndpoint {
                url: &DEMO_IP_POOL_SERVICE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            // IP Pool ranges endpoint (Oxide services)
            VerifyEndpoint {
                url: &DEMO_IP_POOL_SERVICE_RANGES_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            // IP Pool ranges/add endpoint (Oxide services)
            VerifyEndpoint {
                url: &DEMO_IP_POOL_SERVICE_RANGES_ADD_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap(),
                )],
            },
            // IP Pool ranges/delete endpoint (Oxide services)
            VerifyEndpoint {
                url: &DEMO_IP_POOL_SERVICE_RANGES_DEL_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap(),
                )],
            },
            /* Silos */
            VerifyEndpoint {
                url: "/v1/system/silos",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_SILO_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_IP_POOLS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_POLICY_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&shared::Policy::<
                            shared::SiloRole,
                        > {
                            role_assignments: vec![],
                        })
                        .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_QUOTAS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(params::SiloQuotasCreate::empty())
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: "/v1/system/silo-quotas",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: "/v1/system/utilization/silos",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_UTIL_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: "/v1/utilization",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: "/v1/policy",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&shared::Policy::<
                            shared::SiloRole,
                        > {
                            role_assignments: vec![],
                        })
                        .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: "/v1/users",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: "/v1/groups",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                // non-existent UUID that will 404
                url: "/v1/groups/8d90b9a5-1cea-4a2b-9af4-71467dd33a04",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_USERS_LIST_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_USERS_CREATE_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_USER_CREATE).unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_USER_ID_GET_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_USER_ID_DELETE_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Delete],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_USER_ID_SET_PASSWORD_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(
                        test_params::UserPassword::LoginDisallowed,
                    )
                    .unwrap(),
                )],
            },
            /* Projects */
            // TODO-security TODO-correctness One thing that's a little strange
            // here: we currently return a 404 if you attempt to create a Project
            // inside an Organization and you're not authorized to do that.  In an
            // ideal world, we'd return a 403 if you can _see_ the Organization and
            // a 404 if not.  But we don't really know if you should be able to see
            // the Organization.  Right now, the only real way to tell that is if
            // you have permissions on anything _inside_ the Organization, which is
            // incredibly expensive to determine in general.
            // TODO: reevaluate the above comment and the change to unprivileged_access below
            VerifyEndpoint {
                url: "/v1/projects",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_PROJECT_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_PROJECT_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                    AllowedMethod::Put(
                        serde_json::to_value(params::ProjectUpdate {
                            identity: IdentityMetadataUpdateParams {
                                name: None,
                                description: Some("different".to_string()),
                            },
                        })
                        .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_PROJECT_POLICY_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&shared::Policy::<
                            shared::ProjectRole,
                        > {
                            role_assignments: vec![],
                        })
                        .unwrap(),
                    ),
                ],
            },
            /* VPCs */
            VerifyEndpoint {
                url: &DEMO_PROJECT_URL_VPCS,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_VPC_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_VPC_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&params::VpcUpdate {
                            identity: IdentityMetadataUpdateParams {
                                name: None,
                                description: Some("different".to_string()),
                            },
                            dns_name: None,
                        })
                        .unwrap(),
                    ),
                    AllowedMethod::Delete,
                ],
            },
            /* Firewall rules */
            VerifyEndpoint {
                url: &DEMO_VPC_URL_FIREWALL_RULES,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(VpcFirewallRuleUpdateParams {
                            rules: vec![],
                        })
                        .unwrap(),
                    ),
                ],
            },
            /* VPC Subnets */
            VerifyEndpoint {
                url: &DEMO_VPC_URL_SUBNETS,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_VPC_SUBNET_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_VPC_SUBNET_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&params::VpcSubnetUpdate {
                            identity: IdentityMetadataUpdateParams {
                                name: None,
                                description: Some("different".to_string()),
                            },
                            custom_router: None,
                        })
                        .unwrap(),
                    ),
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_VPC_SUBNET_INTERFACES_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            /* VPC Routers */
            VerifyEndpoint {
                url: &DEMO_VPC_URL_ROUTERS,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_VPC_ROUTER_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_VPC_ROUTER_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&params::VpcRouterUpdate {
                            identity: IdentityMetadataUpdateParams {
                                name: None,
                                description: Some("different".to_string()),
                            },
                        })
                        .unwrap(),
                    ),
                    AllowedMethod::Delete,
                ],
            },
            /* Router Routes */
            VerifyEndpoint {
                url: &DEMO_VPC_ROUTER_URL_ROUTES,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_ROUTER_ROUTE_CREATE)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_ROUTER_ROUTE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&params::RouterRouteUpdate {
                            identity: IdentityMetadataUpdateParams {
                                name: None,
                                description: Some("different".to_string()),
                            },
                            target: RouteTarget::Ip(IpAddr::from(
                                Ipv4Addr::new(127, 0, 0, 1),
                            )),
                            destination: RouteDestination::Subnet(
                                "loopback".parse().unwrap(),
                            ),
                        })
                        .unwrap(),
                    ),
                    AllowedMethod::Delete,
                ],
            },
            /* Internet Gateways */
            VerifyEndpoint {
                url: &DEMO_INTERNET_GATEWAYS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::GetNonexistent,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_INTERNET_GATEWAY_CREATE)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INTERNET_GATEWAY_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::GetNonexistent,
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INTERNET_GATEWAY_IP_POOLS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::GetNonexistent,
                    AllowedMethod::Post(
                        serde_json::to_value(
                            &*DEMO_INTERNET_GATEWAY_IP_POOL_CREATE,
                        )
                        .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INTERNET_GATEWAY_IP_POOL_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Delete],
            },
            VerifyEndpoint {
                url: &DEMO_INTERNET_GATEWAY_IP_ADDRS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::GetNonexistent,
                    AllowedMethod::Post(
                        serde_json::to_value(
                            &*DEMO_INTERNET_GATEWAY_IP_ADDRESS_CREATE,
                        )
                        .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INTERNET_GATEWAY_IP_ADDR_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Delete],
            },
            /* Disks */
            VerifyEndpoint {
                url: &DEMO_DISKS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_DISK_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_DISK_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_DISK_METRICS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_DISKS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_DISKS_ATTACH_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(params::DiskPath {
                        disk: DEMO_DISK_NAME.clone().into(),
                    })
                    .unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_DISKS_DETACH_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(params::DiskPath {
                        disk: DEMO_DISK_NAME.clone().into(),
                    })
                    .unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_AFFINITY_GROUPS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_ANTI_AFFINITY_GROUPS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            /* Affinity Groups */
            VerifyEndpoint {
                url: &DEMO_PROJECT_URL_AFFINITY_GROUPS,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,

                allowed_methods: vec![
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_AFFINITY_GROUP_CREATE)
                            .unwrap(),
                    ),
                    AllowedMethod::Get,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_AFFINITY_GROUP_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,

                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_AFFINITY_GROUP_UPDATE)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_AFFINITY_GROUP_MEMBERS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,

                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_AFFINITY_GROUP_INSTANCE_MEMBER_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,

                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                    AllowedMethod::Post(serde_json::Value::Null),
                ],
            },
            /* Anti-Affinity Groups */
            VerifyEndpoint {
                url: &DEMO_ANTI_AFFINITY_GROUPS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,

                allowed_methods: vec![
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_ANTI_AFFINITY_GROUP_CREATE)
                            .unwrap(),
                    ),
                    AllowedMethod::Get,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_ANTI_AFFINITY_GROUP_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,

                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_ANTI_AFFINITY_GROUP_UPDATE)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_ANTI_AFFINITY_GROUP_MEMBERS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,

                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_ANTI_AFFINITY_GROUP_INSTANCE_MEMBER_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,

                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                    AllowedMethod::Post(serde_json::Value::Null),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_IMPORT_DISK_BULK_WRITE_START_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::value::Value::Null,
                )],
            },
            VerifyEndpoint {
                url: &DEMO_IMPORT_DISK_BULK_WRITE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: {
                    use base64::prelude::*;
                    vec![AllowedMethod::Post(
                        serde_json::to_value(params::ImportBlocksBulkWrite {
                            offset: 0,
                            base64_encoded_data: BASE64_STANDARD
                                .encode([0; 4096]),
                        })
                        .unwrap(),
                    )]
                },
            },
            VerifyEndpoint {
                url: &DEMO_IMPORT_DISK_BULK_WRITE_STOP_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::value::Value::Null,
                )],
            },
            VerifyEndpoint {
                url: &DEMO_IMPORT_DISK_FINALIZE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::from_str("{}").unwrap(),
                )],
            },
            /* Project images */
            VerifyEndpoint {
                url: &DEMO_PROJECT_URL_IMAGES,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_IMAGE_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_PROJECT_IMAGE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_PROJECT_PROMOTE_IMAGE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::value::Value::Null,
                )],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_DEMOTE_IMAGE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::value::Value::Null,
                )],
            },
            /* Snapshots */
            VerifyEndpoint {
                url: &DEMO_PROJECT_URL_SNAPSHOTS,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(DEMO_SNAPSHOT_CREATE.clone())
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_SNAPSHOT_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            /* Instances */
            VerifyEndpoint {
                url: &DEMO_PROJECT_URL_INSTANCES,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_INSTANCE_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_INSTANCE_UPDATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_START_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::Value::Null,
                )],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_STOP_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::Value::Null,
                )],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_REBOOT_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::Value::Null,
                )],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_SERIAL_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::GetNonexistent, // has required query parameters
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_SERIAL_STREAM_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetWebsocket],
            },
            /* Instance NICs */
            VerifyEndpoint {
                url: &DEMO_INSTANCE_NICS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_INSTANCE_NIC_CREATE)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_NIC_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_INSTANCE_NIC_PUT).unwrap(),
                    ),
                ],
            },
            /* Instance external IP addresses */
            VerifyEndpoint {
                url: &DEMO_INSTANCE_EXTERNAL_IPS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_EPHEMERAL_IP_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_EPHEMERAL_IP_ATTACH)
                            .unwrap(),
                    ),
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_INSTANCE_SSH_KEYS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            /* IAM */
            VerifyEndpoint {
                url: "/v1/system/roles",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: "/v1/system/roles/fleet.admin",
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: "/v1/system/users-builtin",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &URL_USERS_DB_INIT,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            /* Hardware */
            VerifyEndpoint {
                url: "/v1/system/hardware/racks",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &HARDWARE_RACK_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &HARDWARE_UNINITIALIZED_SLEDS,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: "/v1/system/hardware/sleds",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_UNINITIALIZED_SLED)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &SLED_INSTANCES_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &HARDWARE_SLED_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &HARDWARE_SLED_PROVISION_POLICY_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Put(
                    serde_json::to_value(&*DEMO_SLED_PROVISION_POLICY).unwrap(),
                )],
            },
            VerifyEndpoint {
                url: "/v1/system/hardware/switches",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            // TODO: Switches should be configured alongside sled agents during test setup
            VerifyEndpoint {
                url: &HARDWARE_SWITCH_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &HARDWARE_DISKS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &HARDWARE_DISK_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &HARDWARE_SLED_DISK_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            /* Support Bundles */
            VerifyEndpoint {
                url: &SUPPORT_BUNDLES_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(serde_json::to_value(()).unwrap()),
                ],
            },
            VerifyEndpoint {
                url: &SUPPORT_BUNDLE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            /* Updates */
            VerifyEndpoint {
                url: "/v1/system/update/repository?file_name=demo-repo.zip",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Put(
                    // In reality this is the contents of a zip file.
                    serde_json::Value::Null,
                )],
            },
            VerifyEndpoint {
                url: "/v1/system/update/repository/1.0.0",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                // The update system is disabled, which causes a 500 error even for
                // privileged users. That is captured by GetUnimplemented.
                allowed_methods: vec![AllowedMethod::GetUnimplemented],
            },
            VerifyEndpoint {
                url: "/v1/system/update/target-release",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_TARGET_RELEASE).unwrap(),
                    ),
                ],
            },
            /* Metrics */
            VerifyEndpoint {
                url: &DEMO_SYSTEM_METRICS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_SILO_METRICS_URL,
                visibility: Visibility::Public,
                // unprivileged user has silo read, otherwise they wouldn't be able
                // to do anything
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &TIMESERIES_QUERY_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_TIMESERIES_QUERY).unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &SYSTEM_TIMESERIES_LIST_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetVolatile],
            },
            VerifyEndpoint {
                url: &SYSTEM_TIMESERIES_QUERY_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_TIMESERIES_QUERY).unwrap(),
                )],
            },
            /* Silo identity providers */
            VerifyEndpoint {
                url: &IDENTITY_PROVIDERS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &SAML_IDENTITY_PROVIDERS_URL,
                // The visibility here deserves some explanation.  In order to
                // create a real SAML identity provider for doing tests, we have to
                // do it in a non-default Silo (because the default one does not
                // support creating a SAML identity provider).  But unprivileged
                // users won't be able to see that Silo.  So from their perspective,
                // it's like an object in a container they can't see (which is what
                // Visibility::Protected means).
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*SAML_IDENTITY_PROVIDER).unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &SPECIFIC_SAML_IDENTITY_PROVIDER_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            /* Misc */
            VerifyEndpoint {
                url: "/v1/me",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: "/v1/me/groups",
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::ReadOnly,
                allowed_methods: vec![AllowedMethod::Get],
            },
            /* SSH keys */
            VerifyEndpoint {
                url: &DEMO_SSHKEYS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::Full,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_SSHKEY_CREATE).unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_SPECIFIC_SSHKEY_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::Full,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            /* Certificates */
            VerifyEndpoint {
                url: &DEMO_CERTIFICATES_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_CERTIFICATE_CREATE)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_CERTIFICATE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            /* External Networking */
            VerifyEndpoint {
                url: &DEMO_SWITCH_PORT_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            /* TODO requires dpd access
            VerifyEndpoint {
                url: &DEMO_SWITCH_PORT_STATUS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                ],
            },
            */
            VerifyEndpoint {
                url: &DEMO_SWITCH_PORT_SETTINGS_APPLY_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Delete,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_SWITCH_PORT_SETTINGS)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_ADDRESS_LOTS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_ADDRESS_LOT_CREATE)
                            .unwrap(),
                    ),
                    AllowedMethod::Get,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_ADDRESS_LOT_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Delete],
            },
            VerifyEndpoint {
                url: &DEMO_ADDRESS_LOT_BLOCKS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_LOOPBACK_CREATE_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_LOOPBACK_CREATE).unwrap(),
                    ),
                    AllowedMethod::Get,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_LOOPBACK_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Delete],
            },
            VerifyEndpoint {
                url: &DEMO_SWITCH_PORT_SETTINGS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Post(
                        serde_json::to_value(
                            &*DEMO_SWITCH_PORT_SETTINGS_CREATE,
                        )
                        .unwrap(),
                    ),
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_SWITCH_PORT_SETTINGS_INFO_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_BGP_CONFIG_CREATE_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_BGP_CONFIG).unwrap(),
                    ),
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_BGP_ANNOUNCE_SET_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_BGP_ANNOUNCE).unwrap(),
                    ),
                    AllowedMethod::Get,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_BGP_ANNOUNCE_SET_DELETE_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Delete],
            },
            VerifyEndpoint {
                url: &DEMO_BGP_ANNOUNCEMENT_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_BGP_STATUS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_BGP_EXPORTED_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_BGP_ROUTES_IPV4_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_BGP_MESSAGE_HISTORY_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_BFD_STATUS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::GetNonexistent],
            },
            VerifyEndpoint {
                url: &DEMO_BFD_ENABLE_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_BFD_ENABLE).unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &DEMO_BFD_DISABLE_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_BFD_DISABLE).unwrap(),
                )],
            },
            // Floating IPs
            VerifyEndpoint {
                url: &DEMO_PROJECT_URL_FIPS,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_FLOAT_IP_CREATE).unwrap(),
                    ),
                    AllowedMethod::Get,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_FLOAT_IP_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&*DEMO_FLOAT_IP_UPDATE).unwrap(),
                    ),
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_FLOATING_IP_ATTACH_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_FLOAT_IP_ATTACH).unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &DEMO_FLOATING_IP_DETACH_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&()).unwrap(),
                )],
            },
            // User-facing services IP allowlist
            VerifyEndpoint {
                url: &ALLOW_LIST_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Put(
                        serde_json::to_value(&*ALLOW_LIST_UPDATE).unwrap(),
                    ),
                ],
            },
            // Alerts
            VerifyEndpoint {
                url: &WEBHOOK_RECEIVERS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_WEBHOOK_RECEIVER_CREATE)
                        .unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &ALERT_RECEIVERS_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &DEMO_ALERT_RECEIVER_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Delete,
                ],
            },
            VerifyEndpoint {
                url: &DEMO_WEBHOOK_RECEIVER_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Put(
                    serde_json::to_value(&*DEMO_WEBHOOK_RECEIVER_UPDATE)
                        .unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &DEMO_ALERT_RECEIVER_PROBE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(()).unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &DEMO_WEBHOOK_SECRETS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![
                    AllowedMethod::Get,
                    AllowedMethod::Post(
                        serde_json::to_value(&*DEMO_WEBHOOK_SECRET_CREATE)
                            .unwrap(),
                    ),
                ],
            },
            VerifyEndpoint {
                url: &DEMO_ALERT_SUBSCRIPTIONS_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_ALERT_SUBSCRIPTION_CREATE)
                        .unwrap(),
                )],
            },
            VerifyEndpoint {
                url: &DEMO_ALERT_SUBSCRIPTION_DELETE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Delete],
            },
            VerifyEndpoint {
                url: &DEMO_WEBHOOK_SECRET_DELETE_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Delete],
            },
            VerifyEndpoint {
                url: &DEMO_ALERT_DELIVERIES_URL,
                visibility: Visibility::Protected,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
            VerifyEndpoint {
                url: &ALERT_CLASSES_URL,
                visibility: Visibility::Public,
                unprivileged_access: UnprivilegedAccess::None,
                allowed_methods: vec![AllowedMethod::Get],
            },
        ]
    });
