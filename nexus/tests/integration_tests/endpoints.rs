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
use internal_dns::names::DNS_ZONE_EXTERNAL_TESTING;
use lazy_static::lazy_static;
use nexus_db_queries::authn;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::identity::Resource;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::RACK_UUID;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils::SWITCH_UUID;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::Baseboard;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::shared::Ipv4Range;
use nexus_types::external_api::shared::UninitializedSled;
use omicron_common::api::external::AddressLotKind;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::SemverVersion;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_test_utils::certificates::CertificateChain;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::str::FromStr;
use uuid::Uuid;

lazy_static! {
    pub static ref HARDWARE_RACK_URL: String =
        format!("/v1/system/hardware/racks/{}", RACK_UUID);
    pub static ref HARDWARE_UNINITIALIZED_SLEDS: String =
        format!("/v1/system/hardware/sleds-uninitialized");
    pub static ref HARDWARE_SLED_URL: String =
        format!("/v1/system/hardware/sleds/{}", SLED_AGENT_UUID);
    pub static ref HARDWARE_SLED_PROVISION_STATE_URL: String =
        format!("/v1/system/hardware/sleds/{}/provision-state", SLED_AGENT_UUID);
    pub static ref DEMO_SLED_PROVISION_STATE: params::SledProvisionStateParams =
        params::SledProvisionStateParams {
            state: nexus_types::external_api::views::SledProvisionState::NonProvisionable,
        };
    pub static ref HARDWARE_SWITCH_URL: String =
        format!("/v1/system/hardware/switches/{}", SWITCH_UUID);
    pub static ref HARDWARE_DISK_URL: String =
        format!("/v1/system/hardware/disks");
    pub static ref HARDWARE_SLED_DISK_URL: String =
        format!("/v1/system/hardware/sleds/{}/disks", SLED_AGENT_UUID);

    pub static ref SLED_INSTANCES_URL: String =
        format!("/v1/system/hardware/sleds/{}/instances", SLED_AGENT_UUID);

    pub static ref DEMO_UNINITIALIZED_SLED: UninitializedSled = UninitializedSled {
        baseboard: Baseboard {
            serial: "demo-serial".to_string(),
            part: "demo-part".to_string(),
            revision: 6
        },
        rack_id: Uuid::new_v4(),
        cubby: 1
    };

    // Global policy
    pub static ref SYSTEM_POLICY_URL: &'static str = "/v1/system/policy";

    // Silo used for testing
    pub static ref DEMO_SILO_NAME: Name = "demo-silo".parse().unwrap();
    pub static ref DEMO_SILO_URL: String =
        format!("/v1/system/silos/{}", *DEMO_SILO_NAME);
    pub static ref DEMO_SILO_POLICY_URL: String =
        format!("/v1/system/silos/{}/policy", *DEMO_SILO_NAME);
    pub static ref DEMO_SILO_QUOTAS_URL: String =
        format!("/v1/system/silos/{}/quotas", *DEMO_SILO_NAME);
    pub static ref DEMO_SILO_CREATE: params::SiloCreate =
        params::SiloCreate {
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
        };

    pub static ref DEMO_SILO_UTIL_URL: String = format!("/v1/system/utilization/silos/{}", *DEMO_SILO_NAME);

    // Use the default Silo for testing the local IdP
    pub static ref DEMO_SILO_USERS_CREATE_URL: String = format!(
        "/v1/system/identity-providers/local/users?silo={}",
        DEFAULT_SILO.identity().name,
    );
    pub static ref DEMO_SILO_USERS_LIST_URL: String = format!(
        "/v1/system/users?silo={}",
        DEFAULT_SILO.identity().name,
    );
    pub static ref DEMO_SILO_USER_ID_GET_URL: String = format!(
        "/v1/system/users/{{id}}?silo={}",
        DEFAULT_SILO.identity().name,
    );
    pub static ref DEMO_SILO_USER_ID_DELETE_URL: String = format!(
        "/v1/system/identity-providers/local/users/{{id}}?silo={}",
        DEFAULT_SILO.identity().name,
    );
    pub static ref DEMO_SILO_USER_ID_SET_PASSWORD_URL: String = format!(
        "/v1/system/identity-providers/local/users/{{id}}/set-password?silo={}",
        DEFAULT_SILO.identity().name,
    );
}

lazy_static! {

    // Project used for testing
    pub static ref DEMO_PROJECT_NAME: Name = "demo-project".parse().unwrap();
    pub static ref DEMO_PROJECT_URL: String =
        format!("/v1/projects/{}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_SELECTOR: String =
        format!("project={}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_POLICY_URL: String =
        format!("/v1/projects/{}/policy", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_URL_DISKS: String =
        format!("/v1/disks?project={}",  *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_URL_IMAGES: String =
        format!("/v1/images?project={}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_URL_INSTANCES: String = format!("/v1/instances?project={}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_URL_SNAPSHOTS: String = format!("/v1/snapshots?project={}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_URL_VPCS: String = format!("/v1/vpcs?project={}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_URL_FIPS: String = format!("/v1/floating-ips?project={}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_CREATE: params::ProjectCreate =
        params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_PROJECT_NAME.clone(),
                description: String::from(""),
            },
        };

    // VPC used for testing
    pub static ref DEMO_VPC_NAME: Name = "demo-vpc".parse().unwrap();
    pub static ref DEMO_VPC_URL: String =
        format!("/v1/vpcs/{}?{}", *DEMO_VPC_NAME, *DEMO_PROJECT_SELECTOR);

    pub static ref DEMO_VPC_SELECTOR: String =
        format!("project={}&vpc={}", *DEMO_PROJECT_NAME, *DEMO_VPC_NAME);
    pub static ref DEMO_VPC_URL_FIREWALL_RULES: String =
        format!("/v1/vpc-firewall-rules?{}", *DEMO_VPC_SELECTOR);
    pub static ref DEMO_VPC_URL_ROUTERS: String =
        format!("/v1/vpc-routers?{}", *DEMO_VPC_SELECTOR);
    pub static ref DEMO_VPC_URL_SUBNETS: String =
        format!("/v1/vpc-subnets?{}", *DEMO_VPC_SELECTOR);
    pub static ref DEMO_VPC_CREATE: params::VpcCreate =
        params::VpcCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_VPC_NAME.clone(),
                description: String::from(""),
            },
            ipv6_prefix: None,
            dns_name: DEMO_VPC_NAME.clone(),
        };

    // VPC Subnet used for testing
    pub static ref DEMO_VPC_SUBNET_NAME: Name =
        "demo-vpc-subnet".parse().unwrap();
    pub static ref DEMO_VPC_SUBNET_URL: String =
        format!("/v1/vpc-subnets/{}?{}", *DEMO_VPC_SUBNET_NAME, *DEMO_VPC_SELECTOR);
    pub static ref DEMO_VPC_SUBNET_INTERFACES_URL: String =
        format!("/v1/vpc-subnets/{}/network-interfaces?{}", *DEMO_VPC_SUBNET_NAME, *DEMO_VPC_SELECTOR);
    pub static ref DEMO_VPC_SUBNET_CREATE: params::VpcSubnetCreate =
        params::VpcSubnetCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_VPC_SUBNET_NAME.clone(),
                description: String::from(""),
            },
            ipv4_block: Ipv4Net("10.1.2.3/8".parse().unwrap()),
            ipv6_block: None,
        };

    // VPC Router used for testing
    pub static ref DEMO_VPC_ROUTER_NAME: Name =
        "demo-vpc-router".parse().unwrap();
    pub static ref DEMO_VPC_ROUTER_URL: String =
        format!("/v1/vpc-routers/{}?project={}&vpc={}", *DEMO_VPC_ROUTER_NAME, *DEMO_PROJECT_NAME, *DEMO_VPC_NAME);
    pub static ref DEMO_VPC_ROUTER_URL_ROUTES: String =
        format!("/v1/vpc-router-routes?project={}&vpc={}&router={}", *DEMO_PROJECT_NAME, *DEMO_VPC_NAME, *DEMO_VPC_ROUTER_NAME);
    pub static ref DEMO_VPC_ROUTER_CREATE: params::VpcRouterCreate =
        params::VpcRouterCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_VPC_ROUTER_NAME.clone(),
                description: String::from(""),
            },
        };

    // Router Route used for testing
    pub static ref DEMO_ROUTER_ROUTE_NAME: Name =
        "demo-router-route".parse().unwrap();
    pub static ref DEMO_ROUTER_ROUTE_URL: String =
        format!("/v1/vpc-router-routes/{}?project={}&vpc={}&router={}", *DEMO_ROUTER_ROUTE_NAME, *DEMO_PROJECT_NAME, *DEMO_VPC_NAME, *DEMO_VPC_ROUTER_NAME);
    pub static ref DEMO_ROUTER_ROUTE_CREATE: params::RouterRouteCreate =
        params::RouterRouteCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_ROUTER_ROUTE_NAME.clone(),
                description: String::from(""),
            },
            target: RouteTarget::Ip(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1))),
            destination: RouteDestination::Subnet("loopback".parse().unwrap()),
        };

    // Disk used for testing
    pub static ref DEMO_DISK_NAME: Name = "demo-disk".parse().unwrap();
    // TODO: Once we can test a URL multiple times we should also a case to exercise authz for disks filtered by instances
    pub static ref DEMO_DISKS_URL: String =
        format!("/v1/disks?{}", *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_DISK_URL: String =
        format!("/v1/disks/{}?{}", *DEMO_DISK_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_DISK_CREATE: params::DiskCreate =
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
                DiskTest::DEFAULT_ZPOOL_SIZE_GIB / 5
            ),
        };
    pub static ref DEMO_DISK_METRICS_URL: String =
        format!(
            "/v1/disks/{}/metrics/activated?start_time={:?}&end_time={:?}&{}",
            *DEMO_DISK_NAME,
            Utc::now(),
            Utc::now(),
            *DEMO_PROJECT_SELECTOR,
        );

    // Related to importing blocks from an external source
    pub static ref DEMO_IMPORT_DISK_NAME: Name = "demo-import-disk".parse().unwrap();
    pub static ref DEMO_IMPORT_DISK_URL: String =
        format!("/v1/disks/{}?{}", *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_IMPORT_DISK_CREATE: params::DiskCreate =
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
                DiskTest::DEFAULT_ZPOOL_SIZE_GIB / 5
            ),
        };

    pub static ref DEMO_IMPORT_DISK_BULK_WRITE_START_URL: String =
        format!("/v1/disks/{}/bulk-write-start?{}", *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_IMPORT_DISK_BULK_WRITE_URL: String =
        format!("/v1/disks/{}/bulk-write?{}", *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_IMPORT_DISK_BULK_WRITE_STOP_URL: String =
        format!("/v1/disks/{}/bulk-write-stop?{}", *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_IMPORT_DISK_FINALIZE_URL: String =
        format!("/v1/disks/{}/finalize?{}", *DEMO_IMPORT_DISK_NAME, *DEMO_PROJECT_SELECTOR);
}

// Separate lazy_static! blocks to avoid hitting some recursion limit when
// compiling
lazy_static! {
    // Instance used for testing
    pub static ref DEMO_INSTANCE_NAME: Name = "demo-instance".parse().unwrap();
    pub static ref DEMO_INSTANCE_SELECTOR: String = format!("{}&instance={}", *DEMO_PROJECT_SELECTOR, *DEMO_INSTANCE_NAME);
    pub static ref DEMO_INSTANCE_URL: String =
        format!("/v1/instances/{}?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_START_URL: String =
        format!("/v1/instances/{}/start?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_STOP_URL: String =
        format!("/v1/instances/{}/stop?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_REBOOT_URL: String =
        format!("/v1/instances/{}/reboot?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_MIGRATE_URL: String =
        format!("/v1/instances/{}/migrate?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_SERIAL_URL: String =
        format!("/v1/instances/{}/serial-console?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_SERIAL_STREAM_URL: String =
        format!("/v1/instances/{}/serial-console/stream?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);

    pub static ref DEMO_INSTANCE_DISKS_URL: String =
        format!("/v1/instances/{}/disks?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_DISKS_ATTACH_URL: String =
        format!("/v1/instances/{}/disks/attach?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_DISKS_DETACH_URL: String =
        format!("/v1/instances/{}/disks/detach?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);

    pub static ref DEMO_INSTANCE_NICS_URL: String =
        format!("/v1/network-interfaces?project={}&instance={}", *DEMO_PROJECT_NAME, *DEMO_INSTANCE_NAME);
    pub static ref DEMO_INSTANCE_EXTERNAL_IPS_URL: String =
        format!("/v1/instances/{}/external-ips?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_EXTERNAL_IP_ATTACH_URL: String =
        format!("/v1/instances/{}/external-ips/attach?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_EXTERNAL_IP_DETACH_URL: String =
        format!("/v1/instances/{}/external-ips/detach?{}", *DEMO_INSTANCE_NAME, *DEMO_PROJECT_SELECTOR);
    pub static ref DEMO_INSTANCE_CREATE: params::InstanceCreate =
        params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_INSTANCE_NAME.clone(),
                description: String::from(""),
            },
            ncpus: InstanceCpuCount(1),
            memory: ByteCount::from_gibibytes_u32(16),
            hostname: String::from("demo-instance"),
            user_data: vec![],
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![
                params::ExternalIpCreate::Ephemeral { pool_name: Some(DEMO_IP_POOL_NAME.clone()) }
            ],
            disks: vec![],
            start: true,
        };

    // The instance needs a network interface, too.
    pub static ref DEMO_INSTANCE_NIC_NAME: Name =
        nexus_defaults::DEFAULT_PRIMARY_NIC_NAME.parse().unwrap();
    pub static ref DEMO_INSTANCE_NIC_URL: String =
        format!("/v1/network-interfaces/{}?project={}&instance={}", *DEMO_INSTANCE_NIC_NAME, *DEMO_PROJECT_NAME, *DEMO_INSTANCE_NAME);
    pub static ref DEMO_INSTANCE_NIC_CREATE: params::InstanceNetworkInterfaceCreate =
        params::InstanceNetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_INSTANCE_NIC_NAME.clone(),
                description: String::from(""),
            },
            vpc_name: DEMO_VPC_NAME.clone(),
            subnet_name: DEMO_VPC_SUBNET_NAME.clone(),
            ip: None,
        };
    pub static ref DEMO_INSTANCE_NIC_PUT: params::InstanceNetworkInterfaceUpdate = {
        params::InstanceNetworkInterfaceUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: Some(String::from("an updated description")),
            },
            primary: false,
        }
    };
}

lazy_static! {
    pub static ref DEMO_CERTIFICATE_NAME: Name =
        "demo-certificate".parse().unwrap();
    pub static ref DEMO_CERTIFICATES_URL: String = format!("/v1/certificates");
    pub static ref DEMO_CERTIFICATE_URL: String =
        format!("/v1/certificates/demo-certificate");
    pub static ref DEMO_CERTIFICATE: CertificateChain =
        CertificateChain::new(format!("*.sys.{DNS_ZONE_EXTERNAL_TESTING}"));
    pub static ref DEMO_CERTIFICATE_CREATE: params::CertificateCreate =
        params::CertificateCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_CERTIFICATE_NAME.clone(),
                description: String::from(""),
            },
            cert: DEMO_CERTIFICATE.cert_chain_as_pem(),
            key: DEMO_CERTIFICATE.end_cert_private_key_as_pem(),
            service: shared::ServiceUsingCertificate::ExternalApi,
        };
}

lazy_static! {
    pub static ref DEMO_SWITCH_PORT_URL: String =
        format!("/v1/system/hardware/switch-port");

    pub static ref DEMO_SWITCH_PORT_SETTINGS_APPLY_URL: String =
        format!(
            "/v1/system/hardware/switch-port/qsfp7/settings?rack_id={}&switch_location={}",
            uuid::Uuid::new_v4(),
            "switch0",
        );

    pub static ref DEMO_SWITCH_PORT_SETTINGS: params::SwitchPortApplySettings =
        params::SwitchPortApplySettings {
            port_settings: NameOrId::Name("portofino".parse().unwrap()),
        };
}

lazy_static! {
    pub static ref DEMO_LOOPBACK_CREATE_URL: String =
        "/v1/system/networking/loopback-address".into();
    pub static ref DEMO_LOOPBACK_URL: String = format!(
        "/v1/system/networking/loopback-address/{}/{}/{}",
        uuid::Uuid::new_v4(),
        "switch0",
        "203.0.113.99/24",
    );
    pub static ref DEMO_LOOPBACK_CREATE: params::LoopbackAddressCreate =
        params::LoopbackAddressCreate {
            address_lot: NameOrId::Name("parkinglot".parse().unwrap()),
            rack_id: uuid::Uuid::new_v4(),
            switch_location: "switch0".parse().unwrap(),
            address: "203.0.113.99".parse().unwrap(),
            mask: 24,
            anycast: false,
        };
}

lazy_static! {
    pub static ref DEMO_SWITCH_PORT_SETTINGS_URL: String = format!(
        "/v1/system/networking/switch-port-settings?port_settings=portofino"
    );
    pub static ref DEMO_SWITCH_PORT_SETTINGS_INFO_URL: String =
        format!("/v1/system/networking/switch-port-settings/protofino");
    pub static ref DEMO_SWITCH_PORT_SETTINGS_CREATE: params::SwitchPortSettingsCreate =
        params::SwitchPortSettingsCreate::new(IdentityMetadataCreateParams {
            name: "portofino".parse().unwrap(),
            description: "just a port".into(),
        });
}

lazy_static! {
    pub static ref DEMO_ADDRESS_LOTS_URL: String =
        format!("/v1/system/networking/address-lot");
    pub static ref DEMO_ADDRESS_LOT_URL: String =
        format!("/v1/system/networking/address-lot/parkinglot");
    pub static ref DEMO_ADDRESS_LOT_BLOCKS_URL: String =
        format!("/v1/system/networking/address-lot/parkinglot/blocks");
    pub static ref DEMO_ADDRESS_LOT_CREATE: params::AddressLotCreate =
        params::AddressLotCreate {
            identity: IdentityMetadataCreateParams {
                name: "parkinglot".parse().unwrap(),
                description: "an address parking lot".into(),
            },
            kind: AddressLotKind::Infra,
            blocks: vec![params::AddressLotBlockCreate {
                first_address: "203.0.113.10".parse().unwrap(),
                last_address: "203.0.113.20".parse().unwrap(),
            }],
        };
}

lazy_static! {
    pub static ref DEMO_BGP_CONFIG_CREATE_URL: String =
        format!("/v1/system/networking/bgp?name_or_id=as47");
    pub static ref DEMO_BGP_CONFIG: params::BgpConfigCreate =
        params::BgpConfigCreate {
            identity: IdentityMetadataCreateParams {
                name: "as47".parse().unwrap(),
                description: "BGP config for AS47".into(),
            },
            bgp_announce_set_id: NameOrId::Name("instances".parse().unwrap()),
            asn: 47,
            vrf: None,
        };
    pub static ref DEMO_BGP_ANNOUNCE_SET_URL: String =
        format!("/v1/system/networking/bgp-announce?name_or_id=a-bag-of-addrs");
    pub static ref DEMO_BGP_ANNOUNCE: params::BgpAnnounceSetCreate =
        params::BgpAnnounceSetCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-bag-of-addrs".parse().unwrap(),
                description: "a bag of addrs".into(),
            },
            announcement: vec![params::BgpAnnouncementCreate {
                address_lot_block: NameOrId::Name(
                    "some-block".parse().unwrap(),
                ),
                network: "10.0.0.0/16".parse().unwrap(),
            }],
        };
    pub static ref DEMO_BGP_STATUS_URL: String =
        format!("/v1/system/networking/bgp-status");
    pub static ref DEMO_BGP_ROUTES_IPV4_URL: String =
        format!("/v1/system/networking/bgp-routes-ipv4?asn=47");
}

lazy_static! {
    // Project Images
    pub static ref DEMO_IMAGE_NAME: Name = "demo-image".parse().unwrap();
    pub static ref DEMO_PROJECT_IMAGES_URL: String =
        format!("/v1/images?project={}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_IMAGE_URL: String =
        format!("/v1/images/{}?project={}", *DEMO_IMAGE_NAME, *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_PROMOTE_IMAGE_URL: String =
        format!("/v1/images/{}/promote?project={}", *DEMO_IMAGE_NAME, *DEMO_PROJECT_NAME);
    pub static ref DEMO_SILO_DEMOTE_IMAGE_URL: String =
        format!("/v1/images/{}/demote?project={}", *DEMO_IMAGE_NAME, *DEMO_PROJECT_NAME);
    pub static ref DEMO_IMAGE_CREATE: params::ImageCreate =
        params::ImageCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_IMAGE_NAME.clone(),
                description: String::from(""),
            },
            source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
            os: "fake-os".to_string(),
            version: "1.0".to_string()
        };

    // IP Pools
    pub static ref DEMO_IP_POOLS_PROJ_URL: String =
        format!("/v1/ip-pools?project={}", *DEMO_PROJECT_NAME);
    pub static ref DEMO_IP_POOLS_URL: &'static str = "/v1/system/ip-pools";
    pub static ref DEMO_IP_POOL_NAME: Name = "default".parse().unwrap();
    pub static ref DEMO_IP_POOL_CREATE: params::IpPoolCreate =
        params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_IP_POOL_NAME.clone(),
                description: String::from("an IP pool"),
            },
            silo: None,
            is_default: true,
        };
    pub static ref DEMO_IP_POOL_PROJ_URL: String =
        format!("/v1/ip-pools/{}?project={}", *DEMO_IP_POOL_NAME, *DEMO_PROJECT_NAME);
    pub static ref DEMO_IP_POOL_URL: String = format!("/v1/system/ip-pools/{}", *DEMO_IP_POOL_NAME);
    pub static ref DEMO_IP_POOL_UPDATE: params::IpPoolUpdate =
        params::IpPoolUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: Some(String::from("a new IP pool")),
            },
        };
    pub static ref DEMO_IP_POOL_RANGE: IpRange = IpRange::V4(Ipv4Range::new(
        std::net::Ipv4Addr::new(10, 0, 0, 0),
        std::net::Ipv4Addr::new(10, 0, 0, 255),
    ).unwrap());
    pub static ref DEMO_IP_POOL_RANGES_URL: String = format!("{}/ranges", *DEMO_IP_POOL_URL);
    pub static ref DEMO_IP_POOL_RANGES_ADD_URL: String = format!("{}/add", *DEMO_IP_POOL_RANGES_URL);
    pub static ref DEMO_IP_POOL_RANGES_DEL_URL: String = format!("{}/remove", *DEMO_IP_POOL_RANGES_URL);

    // IP Pools (Services)
    pub static ref DEMO_IP_POOL_SERVICE_URL: &'static str = "/v1/system/ip-pools-service";
    pub static ref DEMO_IP_POOL_SERVICE_RANGES_URL: String = format!("{}/ranges", *DEMO_IP_POOL_SERVICE_URL);
    pub static ref DEMO_IP_POOL_SERVICE_RANGES_ADD_URL: String = format!("{}/add", *DEMO_IP_POOL_SERVICE_RANGES_URL);
    pub static ref DEMO_IP_POOL_SERVICE_RANGES_DEL_URL: String = format!("{}/remove", *DEMO_IP_POOL_SERVICE_RANGES_URL);

    // Snapshots
    pub static ref DEMO_SNAPSHOT_NAME: Name = "demo-snapshot".parse().unwrap();
    pub static ref DEMO_SNAPSHOT_URL: String =
        format!("/v1/snapshots/{}?project={}", *DEMO_SNAPSHOT_NAME, *DEMO_PROJECT_NAME);
    pub static ref DEMO_SNAPSHOT_CREATE: params::SnapshotCreate =
        params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_SNAPSHOT_NAME.clone(),
                description: String::from(""),
            },
            disk: DEMO_DISK_NAME.clone().into(),
        };

    // SSH keys
    pub static ref DEMO_SSHKEYS_URL: &'static str = "/v1/me/ssh-keys";
    pub static ref DEMO_SSHKEY_NAME: Name = "aaaaa-ssh-key".parse().unwrap();
    pub static ref DEMO_SSHKEY_CREATE: params::SshKeyCreate = params::SshKeyCreate {
        identity: IdentityMetadataCreateParams {
            name: DEMO_SSHKEY_NAME.clone(),
            description: "a demo key".to_string(),
        },

        public_key: "AAAAAAAAAAAAAAA".to_string(),
    };

    pub static ref DEMO_SPECIFIC_SSHKEY_URL: String =
        format!("{}/{}", *DEMO_SSHKEYS_URL, *DEMO_SSHKEY_NAME);

    // System update

    pub static ref DEMO_SYSTEM_UPDATE_PARAMS: params::SystemUpdatePath = params::SystemUpdatePath {
        version: SemverVersion::new(1,0,0),
    };
}

lazy_static! {
    // Project Floating IPs
    pub static ref DEMO_FLOAT_IP_NAME: Name = "float-ip".parse().unwrap();
    pub static ref DEMO_FLOAT_IP_URL: String =
        format!("/v1/floating-ips/{}?project={}", *DEMO_FLOAT_IP_NAME, *DEMO_PROJECT_NAME);
    pub static ref DEMO_FLOAT_IP_CREATE: params::FloatingIpCreate =
        params::FloatingIpCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_FLOAT_IP_NAME.clone(),
                description: String::from("a new IP pool"),
            },
            address: Some(std::net::Ipv4Addr::new(10, 0, 0, 141).into()),
            pool: None,
        };
    pub static ref DEMO_FLOAT_IP_ATTACH: params::ExternalIpCreate =
        params::ExternalIpCreate::Floating { floating_ip_name: DEMO_FLOAT_IP_NAME.clone() };
    pub static ref DEMO_FLOAT_IP_DETACH: params::ExternalIpDelete =
        params::ExternalIpDelete::Floating { floating_ip_name: DEMO_FLOAT_IP_NAME.clone() };
}

lazy_static! {
    // Identity providers
    pub static ref IDENTITY_PROVIDERS_URL: String = format!("/v1/system/identity-providers?silo=demo-silo");
    pub static ref SAML_IDENTITY_PROVIDERS_URL: String = format!("/v1/system/identity-providers/saml?silo=demo-silo");

    pub static ref DEMO_SAML_IDENTITY_PROVIDER_NAME: Name = "demo-saml-provider".parse().unwrap();
    pub static ref SPECIFIC_SAML_IDENTITY_PROVIDER_URL: String = format!("/v1/system/identity-providers/saml/{}?silo=demo-silo", *DEMO_SAML_IDENTITY_PROVIDER_NAME);

    pub static ref SAML_IDENTITY_PROVIDER: params::SamlIdentityProviderCreate =
        params::SamlIdentityProviderCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_SAML_IDENTITY_PROVIDER_NAME.clone(),
                description: "a demo provider".to_string(),
            },

            idp_metadata_source: params::IdpMetadataSource::Url { url: HTTP_SERVER.url("/descriptor").to_string() },

            idp_entity_id: "entity_id".to_string(),
            sp_client_id: "client_id".to_string(),
            acs_url: "http://acs".to_string(),
            slo_url: "http://slo".to_string(),
            technical_contact_email: "technical@fake".to_string(),

            signing_keypair: None,

            group_attribute_name: None,
        };

    pub static ref DEMO_SYSTEM_METRICS_URL: String =
        format!(
            "/v1/system/metrics/virtual_disk_space_provisioned?start_time={:?}&end_time={:?}",
            Utc::now(),
            Utc::now(),
        );

    pub static ref DEMO_SILO_METRICS_URL: String =
        format!(
            "/v1/metrics/virtual_disk_space_provisioned?start_time={:?}&end_time={:?}",
            Utc::now(),
            Utc::now(),
        );

    // Users
    pub static ref DEMO_USER_CREATE: params::UserCreate = params::UserCreate {
        external_id: params::UserId::from_str("dummy-user").unwrap(),
        password: params::UserPassword::LoginDisallowed,
    };
}

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
            AllowedMethod::Get => &Method::GET,
            AllowedMethod::GetNonexistent => &Method::GET,
            AllowedMethod::GetUnimplemented => &Method::GET,
            AllowedMethod::GetWebsocket => &Method::GET,
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
            | AllowedMethod::GetWebsocket => None,
            AllowedMethod::Post(body) => Some(&body),
            AllowedMethod::Put(body) => Some(&body),
        }
    }
}

lazy_static! {
    pub static ref URL_USERS_DB_INIT: String =
        format!("/v1/system/users-builtin/{}", authn::USER_DB_INIT.name);

    /// List of endpoints to be verified
    pub static ref VERIFY_ENDPOINTS: Vec<VerifyEndpoint> = vec![
        // Global IAM policy
        VerifyEndpoint {
            url: &SYSTEM_POLICY_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(
                        &shared::Policy::<shared::FleetRole> {
                            role_assignments: vec![]
                        }
                    ).unwrap()
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
                    serde_json::to_value(&*DEMO_IP_POOL_CREATE).unwrap()
                ),
            ],
        },
        VerifyEndpoint {
            url: &DEMO_IP_POOLS_PROJ_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get
            ],
        },

        // Single IP Pool endpoint
        VerifyEndpoint {
            url: &DEMO_IP_POOL_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(&*DEMO_IP_POOL_UPDATE).unwrap()
                ),
                AllowedMethod::Delete,
            ],
        },
        VerifyEndpoint {
            url: &DEMO_IP_POOL_PROJ_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get
            ],
        },

        // IP Pool ranges endpoint
        VerifyEndpoint {
            url: &DEMO_IP_POOL_RANGES_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get
            ],
        },

        // IP Pool ranges/add endpoint
        VerifyEndpoint {
            url: &DEMO_IP_POOL_RANGES_ADD_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap()
                ),
            ],
        },

        // IP Pool ranges/delete endpoint
        VerifyEndpoint {
            url: &DEMO_IP_POOL_RANGES_DEL_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap()
                ),
            ],
        },

        // IP Pool endpoint (Oxide services)
        VerifyEndpoint {
            url: &DEMO_IP_POOL_SERVICE_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get
            ],
        },

        // IP Pool ranges endpoint (Oxide services)
        VerifyEndpoint {
            url: &DEMO_IP_POOL_SERVICE_RANGES_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get
            ],
        },

        // IP Pool ranges/add endpoint (Oxide services)
        VerifyEndpoint {
            url: &DEMO_IP_POOL_SERVICE_RANGES_ADD_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap()
                ),
            ],
        },

        // IP Pool ranges/delete endpoint (Oxide services)
        VerifyEndpoint {
            url: &DEMO_IP_POOL_SERVICE_RANGES_DEL_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap()
                ),
            ],
        },

        /* Silos */
        VerifyEndpoint {
            url: "/v1/system/silos",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_SILO_CREATE).unwrap()
                )
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
            url: &DEMO_SILO_POLICY_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(
                        &shared::Policy::<shared::SiloRole> {
                            role_assignments: vec![]
                        }
                    ).unwrap()
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
                    serde_json::to_value(
                        params::SiloQuotasCreate::empty()
                    ).unwrap()
                )
            ],
        },
        VerifyEndpoint {
            url: "/v1/system/silo-quotas",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get
            ],
        },
        VerifyEndpoint {
            url: "/v1/system/utilization/silos",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get
            ]
        },
        VerifyEndpoint {
            url: &DEMO_SILO_UTIL_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get
            ]
        },
        VerifyEndpoint {
            url: "/v1/utilization",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get
            ]
        },
        VerifyEndpoint {
            url: "/v1/policy",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(
                        &shared::Policy::<shared::SiloRole> {
                            role_assignments: vec![]
                        }
                    ).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: "/v1/users",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: "/v1/groups",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            // non-existent UUID that will 404
            url: "/v1/groups/8d90b9a5-1cea-4a2b-9af4-71467dd33a04",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::GetNonexistent,
            ],
        },

        VerifyEndpoint {
            url: &DEMO_SILO_USERS_LIST_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![ AllowedMethod::Get ],
        },

        VerifyEndpoint {
            url: &DEMO_SILO_USERS_CREATE_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(
                        &*DEMO_USER_CREATE
                    ).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: &DEMO_SILO_USER_ID_GET_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &DEMO_SILO_USER_ID_DELETE_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Delete,
            ],
        },

        VerifyEndpoint {
            url: &DEMO_SILO_USER_ID_SET_PASSWORD_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::to_value(
                    params::UserPassword::LoginDisallowed
                ).unwrap()),
            ],
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
                    serde_json::to_value(&*DEMO_PROJECT_CREATE).unwrap()
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
                    serde_json::to_value(params::ProjectUpdate{
                        identity: IdentityMetadataUpdateParams {
                            name: None,
                            description: Some("different".to_string())
                        },
                    }).unwrap()
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
                    serde_json::to_value(
                        &shared::Policy::<shared::ProjectRole> {
                            role_assignments: vec![]
                        }
                    ).unwrap()
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
                    serde_json::to_value(&*DEMO_VPC_CREATE).unwrap()
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
                            description: Some("different".to_string())
                        },
                        dns_name: None,
                    }).unwrap()
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
                    }).unwrap()
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
                    serde_json::to_value(&*DEMO_VPC_SUBNET_CREATE).unwrap()
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
                            description: Some("different".to_string())
                        },
                    }).unwrap()
                ),
                AllowedMethod::Delete,
            ],
        },

        VerifyEndpoint {
            url: &DEMO_VPC_SUBNET_INTERFACES_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        /* VPC Routers */

        VerifyEndpoint {
            url: &DEMO_VPC_URL_ROUTERS,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_VPC_ROUTER_CREATE).unwrap()
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
                            description: Some("different".to_string())
                        },
                    }).unwrap()
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
                    serde_json::to_value(&*DEMO_ROUTER_ROUTE_CREATE).unwrap()
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
                            description: Some("different".to_string())
                        },
                        target: RouteTarget::Ip(
                            IpAddr::from(Ipv4Addr::new(127, 0, 0, 1))),
                        destination: RouteDestination::Subnet(
                            "loopback".parse().unwrap()),
                    }).unwrap()
                ),
                AllowedMethod::Delete,
            ],
        },

        /* Disks */

        VerifyEndpoint {
            url: &DEMO_DISKS_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_DISK_CREATE).unwrap()
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
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &DEMO_INSTANCE_DISKS_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ]
        },

        VerifyEndpoint {
            url: &DEMO_INSTANCE_DISKS_ATTACH_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(params::DiskPath {
                        disk: DEMO_DISK_NAME.clone().into()
                    }).unwrap()
                )
            ],
        },

        VerifyEndpoint {
            url: &DEMO_INSTANCE_DISKS_DETACH_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(params::DiskPath {
                        disk: DEMO_DISK_NAME.clone().into()
                    }).unwrap()
                )
            ],
        },

        VerifyEndpoint {
            url: &DEMO_IMPORT_DISK_BULK_WRITE_START_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::value::Value::Null),
            ],
        },

        VerifyEndpoint {
            url: &DEMO_IMPORT_DISK_BULK_WRITE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(params::ImportBlocksBulkWrite {
                        offset: 0,
                        base64_encoded_data: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==".into(),
                    }).unwrap()),
            ],
        },

        VerifyEndpoint {
            url: &DEMO_IMPORT_DISK_BULK_WRITE_STOP_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::value::Value::Null),
            ],
        },

        VerifyEndpoint {
            url: &DEMO_IMPORT_DISK_FINALIZE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::from_str("{}").unwrap()),
            ],
        },

        /* Project images */

        VerifyEndpoint {
            url: &DEMO_PROJECT_URL_IMAGES,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IMAGE_CREATE).unwrap()
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
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::value::Value::Null),
            ],
        },

        VerifyEndpoint {
            url: &DEMO_SILO_DEMOTE_IMAGE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::value::Value::Null),
            ],
        },

        /* Snapshots */

        VerifyEndpoint {
            url: &DEMO_PROJECT_URL_SNAPSHOTS,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(DEMO_SNAPSHOT_CREATE.clone()).unwrap(),
                )
            ]
        },

        VerifyEndpoint {
            url: &DEMO_SNAPSHOT_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
            ]
        },

        /* Instances */
        VerifyEndpoint {
            url: &DEMO_PROJECT_URL_INSTANCES,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_INSTANCE_CREATE).unwrap()
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
            ],
        },

        VerifyEndpoint {
            url: &DEMO_INSTANCE_START_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &DEMO_INSTANCE_STOP_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &DEMO_INSTANCE_REBOOT_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &DEMO_INSTANCE_MIGRATE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::to_value(
                    params::InstanceMigrate {
                        dst_sled_id: uuid::Uuid::new_v4(),
                    }
                ).unwrap()),
            ],
        },
        VerifyEndpoint {
            url: &DEMO_INSTANCE_SERIAL_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetNonexistent // has required query parameters
            ],
        },
        VerifyEndpoint {
            url: &DEMO_INSTANCE_SERIAL_STREAM_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetWebsocket
            ],
        },

        /* Instance NICs */
        VerifyEndpoint {
            url: &DEMO_INSTANCE_NICS_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_INSTANCE_NIC_CREATE).unwrap()
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
                    serde_json::to_value(&*DEMO_INSTANCE_NIC_PUT).unwrap()
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
            url: &DEMO_INSTANCE_EXTERNAL_IP_ATTACH_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Post(
                serde_json::to_value(&*DEMO_FLOAT_IP_ATTACH).unwrap()
            )],
        },

        VerifyEndpoint {
            url: &DEMO_INSTANCE_EXTERNAL_IP_DETACH_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Post(
                serde_json::to_value(&*DEMO_FLOAT_IP_DETACH).unwrap()
            )],
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
            allowed_methods: vec![AllowedMethod::Get, AllowedMethod::Post(
                serde_json::to_value(&*DEMO_UNINITIALIZED_SLED).unwrap()
            )],
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
            url: &HARDWARE_SLED_PROVISION_STATE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Put(
                serde_json::to_value(&*DEMO_SLED_PROVISION_STATE).unwrap()
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
            url: &HARDWARE_DISK_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: &HARDWARE_SLED_DISK_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        /* Updates */

        VerifyEndpoint {
            url: "/v1/system/update/refresh",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Post(
                serde_json::Value::Null
            )],
        },

        VerifyEndpoint {
            url: "/v1/system/update/version",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/v1/system/update/components",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/v1/system/update/updates",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        // TODO: make system update endpoints work instead of expecting 404

        VerifyEndpoint {
            url: "/v1/system/update/updates/1.0.0",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/v1/system/update/updates/1.0.0/components",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/v1/system/update/start",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Post(
                serde_json::to_value(&*DEMO_SYSTEM_UPDATE_PARAMS).unwrap()
            )],
        },

        VerifyEndpoint {
            url: "/v1/system/update/stop",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Post(
                serde_json::Value::Null
            )],
        },

        VerifyEndpoint {
            url: "/v1/system/update/deployments",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/v1/system/update/deployments/120bbb6f-660a-440c-8cb7-199be202ddff",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::GetNonexistent],
        },

        /* Metrics */

        VerifyEndpoint {
            url: &DEMO_SYSTEM_METRICS_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &DEMO_SILO_METRICS_URL,
            visibility: Visibility::Public,
            // unprivileged user has silo read, otherwise they wouldn't be able
            // to do anything
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        /* Silo identity providers */

        VerifyEndpoint {
            url: &IDENTITY_PROVIDERS_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
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
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },
        VerifyEndpoint {
            url: "/v1/me/groups",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
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
                    serde_json::to_value(&*DEMO_CERTIFICATE_CREATE).unwrap(),
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
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },


        VerifyEndpoint {
            url: &DEMO_SWITCH_PORT_SETTINGS_APPLY_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Delete,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_SWITCH_PORT_SETTINGS).unwrap(),
                ),
            ],
        },

        VerifyEndpoint {
            url: &DEMO_ADDRESS_LOTS_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_ADDRESS_LOT_CREATE).unwrap(),
                ),
                AllowedMethod::Get
            ],
        },

        VerifyEndpoint {
            url: &DEMO_ADDRESS_LOT_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Delete,
            ]
        },

        VerifyEndpoint {
            url: &DEMO_ADDRESS_LOT_BLOCKS_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetNonexistent
            ],
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
            allowed_methods: vec![
                AllowedMethod::Delete
            ],
        },

        VerifyEndpoint {
            url: &DEMO_SWITCH_PORT_SETTINGS_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(
                        &*DEMO_SWITCH_PORT_SETTINGS_CREATE).unwrap(),
                ),
                AllowedMethod::Get,
                AllowedMethod::Delete
            ],
        },

        VerifyEndpoint {
            url: &DEMO_SWITCH_PORT_SETTINGS_INFO_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetNonexistent
            ],
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
                AllowedMethod::Delete
            ],
        },

        VerifyEndpoint {
            url: &DEMO_BGP_ANNOUNCE_SET_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_BGP_ANNOUNCE).unwrap(),
                ),
                AllowedMethod::GetNonexistent,
                AllowedMethod::Delete
            ],
        },

        VerifyEndpoint {
            url: &DEMO_BGP_STATUS_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetNonexistent,
            ],
        },

        VerifyEndpoint {
            url: &DEMO_BGP_ROUTES_IPV4_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetNonexistent,
            ],
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
                AllowedMethod::Delete,
            ],
        }
    ];
}
