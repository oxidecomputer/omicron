// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authz-related configuration for API endpoints
//!
//! This is used for various authz-related tests.
//! THERE ARE NO TESTS IN THIS FILE.

use crate::integration_tests::unauthorized::HTTP_SERVER;
use http::method::Method;
use lazy_static::lazy_static;
use nexus_test_utils::RACK_UUID;
use nexus_test_utils::SLED_AGENT_UUID;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Name;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteUpdateParams;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_nexus::authn;
use omicron_nexus::authz;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::shared;
use omicron_nexus::external_api::shared::IpRange;
use omicron_nexus::external_api::shared::Ipv4Range;
use std::net::IpAddr;
use std::net::Ipv4Addr;

lazy_static! {
    pub static ref HARDWARE_RACK_URL: String =
        format!("/hardware/racks/{}", RACK_UUID);
    pub static ref HARDWARE_SLED_URL: String =
        format!("/hardware/sleds/{}", SLED_AGENT_UUID);

    // Global policy
    pub static ref POLICY_URL: &'static str = "/policy";

    // Silo used for testing
    pub static ref DEMO_SILO_NAME: Name = "demo-silo".parse().unwrap();
    pub static ref DEMO_SILO_URL: String =
        format!("/silos/{}", *DEMO_SILO_NAME);
    pub static ref DEMO_SILO_POLICY_URL: String =
        format!("/silos/{}/policy", *DEMO_SILO_NAME);
    pub static ref DEMO_SILO_CREATE: params::SiloCreate =
        params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_SILO_NAME.clone(),
                description: String::from(""),
            },
            discoverable: true,
            user_provision_type: shared::UserProvisionType::Fixed,
            admin_group_name: None,
        };

    // Organization used for testing
    pub static ref DEMO_ORG_NAME: Name = "demo-org".parse().unwrap();
    pub static ref DEMO_ORG_URL: String =
        format!("/organizations/{}", *DEMO_ORG_NAME);
    pub static ref DEMO_ORG_POLICY_URL: String =
        format!("{}/policy", *DEMO_ORG_URL);
    pub static ref DEMO_ORG_PROJECTS_URL: String =
        format!("{}/projects", *DEMO_ORG_URL);
    pub static ref DEMO_ORG_CREATE: params::OrganizationCreate =
        params::OrganizationCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_ORG_NAME.clone(),
                description: String::from(""),
            },
        };

    // Project used for testing
    pub static ref DEMO_PROJECT_NAME: Name = "demo-project".parse().unwrap();
    pub static ref DEMO_PROJECT_URL: String =
        format!("{}/{}", *DEMO_ORG_PROJECTS_URL, *DEMO_PROJECT_NAME);
    pub static ref DEMO_PROJECT_POLICY_URL: String =
        format!("{}/policy", *DEMO_PROJECT_URL);
    pub static ref DEMO_PROJECT_URL_DISKS: String =
        format!("{}/disks", *DEMO_PROJECT_URL);
    pub static ref DEMO_PROJECT_URL_IMAGES: String =
        format!("{}/images", *DEMO_PROJECT_URL);
    pub static ref DEMO_PROJECT_URL_INSTANCES: String =
        format!("{}/instances", *DEMO_PROJECT_URL);
    pub static ref DEMO_PROJECT_URL_SNAPSHOTS: String =
        format!("{}/snapshots", *DEMO_PROJECT_URL);
    pub static ref DEMO_PROJECT_URL_VPCS: String =
        format!("{}/vpcs", *DEMO_PROJECT_URL);
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
        format!("{}/{}", *DEMO_PROJECT_URL_VPCS, *DEMO_VPC_NAME);
    pub static ref DEMO_VPC_URL_FIREWALL_RULES: String =
        format!("{}/firewall/rules", *DEMO_VPC_URL);
    pub static ref DEMO_VPC_URL_ROUTERS: String =
        format!("{}/routers", *DEMO_VPC_URL);
    pub static ref DEMO_VPC_URL_SUBNETS: String =
        format!("{}/subnets", *DEMO_VPC_URL);
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
        format!("{}/{}", *DEMO_VPC_URL_SUBNETS, *DEMO_VPC_SUBNET_NAME);
    pub static ref DEMO_VPC_SUBNET_INTERFACES_URL: String =
        format!("{}/network-interfaces", *DEMO_VPC_SUBNET_URL);
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
        format!("{}/{}", *DEMO_VPC_URL_ROUTERS, *DEMO_VPC_ROUTER_NAME);
    pub static ref DEMO_VPC_ROUTER_URL_ROUTES: String =
        format!("{}/routes", *DEMO_VPC_ROUTER_URL);
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
        format!("{}/{}", *DEMO_VPC_ROUTER_URL_ROUTES, *DEMO_ROUTER_ROUTE_NAME);
    pub static ref DEMO_ROUTER_ROUTE_CREATE: RouterRouteCreateParams =
        RouterRouteCreateParams {
            identity: IdentityMetadataCreateParams {
                name: DEMO_ROUTER_ROUTE_NAME.clone(),
                description: String::from(""),
            },
            target: RouteTarget::Ip(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1))),
            destination: RouteDestination::Subnet("loopback".parse().unwrap()),
        };

    // Disk used for testing
    pub static ref DEMO_DISK_NAME: Name = "demo-disk".parse().unwrap();
    pub static ref DEMO_DISK_URL: String =
        format!("{}/{}", *DEMO_PROJECT_URL_DISKS, *DEMO_DISK_NAME);
    pub static ref DEMO_DISK_CREATE: params::DiskCreate =
        params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_DISK_NAME.clone(),
                description: "".parse().unwrap(),
            },
            disk_source: params::DiskSource::Blank { block_size: params::BlockSize::try_from(4096).unwrap() },
            size: ByteCount::from_gibibytes_u32(16),
        };

    // Instance used for testing
    pub static ref DEMO_INSTANCE_NAME: Name = "demo-instance".parse().unwrap();
    pub static ref DEMO_INSTANCE_URL: String =
        format!("{}/{}", *DEMO_PROJECT_URL_INSTANCES, *DEMO_INSTANCE_NAME);
    pub static ref DEMO_INSTANCE_START_URL: String =
        format!("{}/start", *DEMO_INSTANCE_URL);
    pub static ref DEMO_INSTANCE_STOP_URL: String =
        format!("{}/stop", *DEMO_INSTANCE_URL);
    pub static ref DEMO_INSTANCE_REBOOT_URL: String =
        format!("{}/reboot", *DEMO_INSTANCE_URL);
    pub static ref DEMO_INSTANCE_MIGRATE_URL: String =
        format!("{}/migrate", *DEMO_INSTANCE_URL);
    pub static ref DEMO_INSTANCE_DISKS_URL: String =
        format!("{}/disks", *DEMO_INSTANCE_URL);
    pub static ref DEMO_INSTANCE_DISKS_ATTACH_URL: String =
        format!("{}/attach", *DEMO_INSTANCE_DISKS_URL);
    pub static ref DEMO_INSTANCE_DISKS_DETACH_URL: String =
        format!("{}/detach", *DEMO_INSTANCE_DISKS_URL);
    pub static ref DEMO_INSTANCE_NICS_URL: String =
        format!("{}/network-interfaces", *DEMO_INSTANCE_URL);
    pub static ref DEMO_INSTANCE_SERIAL_URL: String =
        format!("{}/serial-console", *DEMO_INSTANCE_URL);
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
            disks: vec![],
        };

    // The instance needs a network interface, too.
    pub static ref DEMO_INSTANCE_NIC_NAME: Name =
        omicron_nexus::defaults::DEFAULT_PRIMARY_NIC_NAME.parse().unwrap();
    pub static ref DEMO_INSTANCE_NIC_URL: String =
        format!("{}/{}", *DEMO_INSTANCE_NICS_URL, *DEMO_INSTANCE_NIC_NAME);
    pub static ref DEMO_INSTANCE_NIC_CREATE: params::NetworkInterfaceCreate =
        params::NetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_INSTANCE_NIC_NAME.clone(),
                description: String::from(""),
            },
            vpc_name: DEMO_VPC_NAME.clone(),
            subnet_name: DEMO_VPC_SUBNET_NAME.clone(),
            ip: None,
        };
    pub static ref DEMO_INSTANCE_NIC_PUT: params::NetworkInterfaceUpdate = {
        params::NetworkInterfaceUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: Some(String::from("an updated description")),
            },
            make_primary: false,
        }
    };
}

// Separate lazy_static! blocks to avoid hitting some recursion limit when compiling
lazy_static! {
    // Project Images
    pub static ref DEMO_IMAGE_NAME: Name = "demo-image".parse().unwrap();
    pub static ref DEMO_PROJECT_IMAGE_URL: String =
        format!("{}/{}", *DEMO_PROJECT_URL_IMAGES, *DEMO_IMAGE_NAME);
    pub static ref DEMO_IMAGE_CREATE: params::ImageCreate =
        params::ImageCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_IMAGE_NAME.clone(),
                description: String::from(""),
            },
            source: params::ImageSource::Url { url: HTTP_SERVER.url("/image.raw").to_string() },
            block_size: params::BlockSize::try_from(4096).unwrap(),
        };

    // Global Images
    pub static ref DEMO_GLOBAL_IMAGE_NAME: Name = "alpine-edge".parse().unwrap();
    pub static ref DEMO_GLOBAL_IMAGE_URL: String =
        format!("/images/{}", *DEMO_GLOBAL_IMAGE_NAME);
    pub static ref DEMO_GLOBAL_IMAGE_CREATE: params::GlobalImageCreate =
        params::GlobalImageCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_GLOBAL_IMAGE_NAME.clone(),
                description: String::from(""),
            },
            source: params::ImageSource::Url { url: HTTP_SERVER.url("/image.raw").to_string() },
            distribution: params::Distribution {
                name: "alpine".parse().unwrap(),
                version: String::from("edge"),
            },
            block_size: params::BlockSize::try_from(4096).unwrap(),
        };

    // IP Pools
    pub static ref DEMO_IP_POOLS_URL: &'static str = "/ip-pools";
    pub static ref DEMO_IP_POOL_NAME: Name = "pool0".parse().unwrap();
    pub static ref DEMO_IP_POOL_CREATE: params::IpPoolCreate =
        params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_IP_POOL_NAME.clone(),
                description: String::from("an IP pool"),
            },
        };
    pub static ref DEMO_IP_POOL_URL: String = format!("/ip-pools/{}", *DEMO_IP_POOL_NAME);
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

    // Snapshots
    pub static ref DEMO_SNAPSHOT_NAME: Name = "demo-snapshot".parse().unwrap();
    pub static ref DEMO_SNAPSHOT_URL: String =
        format!("{}/{}", *DEMO_PROJECT_URL_SNAPSHOTS, *DEMO_SNAPSHOT_NAME);
    pub static ref DEMO_SNAPSHOT_CREATE: params::SnapshotCreate =
        params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: DEMO_SNAPSHOT_NAME.clone(),
                description: String::from(""),
            },
            disk: DEMO_DISK_NAME.clone(),
        };

    // SSH keys
    pub static ref DEMO_SSHKEYS_URL: &'static str = "/session/me/sshkeys";
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
}

lazy_static! {
    // Identity providers
    pub static ref IDENTITY_PROVIDERS_URL: String = format!("/silos/default-silo/identity-providers");
    pub static ref SAML_IDENTITY_PROVIDERS_URL: String = format!("/silos/default-silo/saml-identity-providers");

    pub static ref DEMO_SAML_IDENTITY_PROVIDER_NAME: Name = "demo-saml-provider".parse().unwrap();
    pub static ref SPECIFIC_SAML_IDENTITY_PROVIDER_URL: String = format!("{}/{}", *SAML_IDENTITY_PROVIDERS_URL, *DEMO_SAML_IDENTITY_PROVIDER_NAME);

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
    /// If it's [`Visibility::Public`] (like "/organizations"), unauthorized
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
    /// "/organizations" is Public, for example.
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
    GetUnimplemented,
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
            | AllowedMethod::GetUnimplemented => None,
            AllowedMethod::Post(body) => Some(&body),
            AllowedMethod::Put(body) => Some(&body),
        }
    }
}

lazy_static! {
    pub static ref URL_USERS_DB_INIT: String =
        format!("/system/user/{}", authn::USER_DB_INIT.name);

    /// List of endpoints to be verified
    pub static ref VERIFY_ENDPOINTS: Vec<VerifyEndpoint> = vec![
        // Global IAM policy
        VerifyEndpoint {
            url: *POLICY_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(
                        &shared::Policy::<authz::FleetRole> {
                            role_assignments: vec![]
                        }
                    ).unwrap()
                ),
            ],
        },

        // IP Pools top-level endpoint
        VerifyEndpoint {
            url: *DEMO_IP_POOLS_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_CREATE).unwrap()
                ),
            ],
        },

        // Single IP Pool endpoint
        VerifyEndpoint {
            url: &*DEMO_IP_POOL_URL,
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

        // IP Pool ranges endpoint
        VerifyEndpoint {
            url: &*DEMO_IP_POOL_RANGES_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get
            ],
        },

        // IP Pool ranges/add endpoint
        VerifyEndpoint {
            url: &*DEMO_IP_POOL_RANGES_ADD_URL,
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
            url: &*DEMO_IP_POOL_RANGES_DEL_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IP_POOL_RANGE).unwrap()
                ),
            ],
        },

        /* Silos */
        VerifyEndpoint {
            url: "/silos",
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
            url: &*DEMO_SILO_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_SILO_POLICY_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(
                        &shared::Policy::<authz::SiloRole> {
                            role_assignments: vec![]
                        }
                    ).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: "/users",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        /* Organizations */

        VerifyEndpoint {
            url: "/organizations",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_ORG_CREATE).unwrap()
                )
            ],
        },

        VerifyEndpoint {
            url: "/by-id/organizations/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_ORG_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
                AllowedMethod::Put(
                    serde_json::to_value(&params::OrganizationUpdate {
                        identity: IdentityMetadataUpdateParams {
                            name: None,
                            description: Some("different".to_string())
                        }
                    }).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_ORG_POLICY_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(
                        &shared::Policy::<authz::OrganizationRole> {
                            role_assignments: vec![]
                        }
                    ).unwrap()
                ),
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
        VerifyEndpoint {
            url: &*DEMO_ORG_PROJECTS_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_PROJECT_CREATE).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: "/by-id/projects/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_PROJECT_URL,
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
            url: &*DEMO_PROJECT_POLICY_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(
                        &shared::Policy::<authz::ProjectRole> {
                            role_assignments: vec![]
                        }
                    ).unwrap()
                ),
            ],
        },

        /* VPCs */
        VerifyEndpoint {
            url: &*DEMO_PROJECT_URL_VPCS,
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
            url: "/by-id/vpcs/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_VPC_URL,
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
            url: &*DEMO_VPC_URL_FIREWALL_RULES,
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
            url: &*DEMO_VPC_URL_SUBNETS,
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
            url: "/by-id/vpc-subnets/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_VPC_SUBNET_URL,
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
            url: &*DEMO_VPC_SUBNET_INTERFACES_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        /* VPC Routers */

        VerifyEndpoint {
            url: &*DEMO_VPC_URL_ROUTERS,
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
            url: "/by-id/vpc-routers/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_VPC_ROUTER_URL,
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
            url: &*DEMO_VPC_ROUTER_URL_ROUTES,
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
            url: "/by-id/vpc-router-routes/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_ROUTER_ROUTE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Put(
                    serde_json::to_value(&RouterRouteUpdateParams {
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
            url: &*DEMO_PROJECT_URL_DISKS,
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
            url: "/by-id/disks/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_DISK_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_INSTANCE_DISKS_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_DISKS_ATTACH_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(params::DiskIdentifier {
                        name: DEMO_DISK_NAME.clone()
                    }).unwrap()
                )
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_DISKS_DETACH_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(
                    serde_json::to_value(params::DiskIdentifier {
                        name: DEMO_DISK_NAME.clone()
                    }).unwrap()
                )
            ],
        },

        /* Project images */

        VerifyEndpoint {
            url: &*DEMO_PROJECT_URL_IMAGES,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetUnimplemented,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_IMAGE_CREATE).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: "/by-id/images/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetUnimplemented,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_PROJECT_IMAGE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetUnimplemented,
                AllowedMethod::Delete,
            ],
        },

        /* Snapshots */

        VerifyEndpoint {
            url: &*DEMO_PROJECT_URL_SNAPSHOTS,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetUnimplemented,
                AllowedMethod::Post(
                    serde_json::to_value(DEMO_SNAPSHOT_CREATE.clone()).unwrap(),
                )
            ]
        },

        VerifyEndpoint {
            url: "/by-id/snapshots/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetUnimplemented,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_SNAPSHOT_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetUnimplemented,
                AllowedMethod::Delete,
            ]
        },

        /* Instances */
        VerifyEndpoint {
            url: &*DEMO_PROJECT_URL_INSTANCES,
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
            url: "/by-id/instances/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_INSTANCE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_INSTANCE_START_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_STOP_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_REBOOT_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Post(serde_json::Value::Null)
            ],
        },
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_MIGRATE_URL,
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
            url: &*DEMO_INSTANCE_SERIAL_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::GetNonexistent // has required query parameters
            ],
        },

        /* Instance NICs */
        VerifyEndpoint {
            url: &*DEMO_INSTANCE_NICS_URL,
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
            url: "/by-id/network-interfaces/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_INSTANCE_NIC_URL,
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

        /* IAM */

        VerifyEndpoint {
            url: "/roles",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },
        VerifyEndpoint {
            url: "/roles/fleet.admin",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/system/user",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },
        VerifyEndpoint {
            url: &*URL_USERS_DB_INIT,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        /* Hardware */

        VerifyEndpoint {
            url: "/hardware/racks",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: &*HARDWARE_RACK_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/hardware/sleds",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: &*HARDWARE_SLED_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        /* Sagas */

        VerifyEndpoint {
            url: "/sagas",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        VerifyEndpoint {
            url: "/sagas/48a1b8c8-fc1c-6fea-9de9-fdeb8dda7823",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::GetNonexistent],
        },

        /* Timeseries schema */

        VerifyEndpoint {
            url: "/timeseries/schema",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        /* Updates */

        VerifyEndpoint {
            url: "/updates/refresh",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Post(
                serde_json::Value::Null
            )],
        },

        /* Global Images */

        VerifyEndpoint {
            url: "/images",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Post(
                    serde_json::to_value(&*DEMO_GLOBAL_IMAGE_CREATE).unwrap()
                ),
            ],
        },

        VerifyEndpoint {
            url: "/by-id/global-images/{id}",
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*DEMO_GLOBAL_IMAGE_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
            ],
        },

        /* Silo identity providers */

        VerifyEndpoint {
            url: &*IDENTITY_PROVIDERS_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        VerifyEndpoint {
            url: &*SAML_IDENTITY_PROVIDERS_URL,
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Post(
                serde_json::to_value(&*SAML_IDENTITY_PROVIDER).unwrap(),
            )],
        },
        VerifyEndpoint {
            url: &*SPECIFIC_SAML_IDENTITY_PROVIDER_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::None,
            allowed_methods: vec![AllowedMethod::Get],
        },

        /* Misc */

        VerifyEndpoint {
            url: "/session/me",
            visibility: Visibility::Public,
            unprivileged_access: UnprivilegedAccess::ReadOnly,
            allowed_methods: vec![
                AllowedMethod::Get,
            ],
        },

        /* SSH keys */

        VerifyEndpoint {
            url: &*DEMO_SSHKEYS_URL,
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
            url: &*DEMO_SPECIFIC_SSHKEY_URL,
            visibility: Visibility::Protected,
            unprivileged_access: UnprivilegedAccess::Full,
            allowed_methods: vec![
                AllowedMethod::Get,
                AllowedMethod::Delete,
            ],
        },
    ];
}
