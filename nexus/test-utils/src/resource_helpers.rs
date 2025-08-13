// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ControlPlaneTestContext;
use crate::TEST_SUITE_PASSWORD;
use crate::http_testing::RequestBuilder;

use super::http_testing::AuthnMode;
use super::http_testing::NexusRequest;
use crucible_agent_client::types::State as RegionState;
use dropshot::HttpErrorResponseBody;
use dropshot::Method;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::header;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_interface::NexusServer;
use nexus_types::deployment::Blueprint;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::Baseboard;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::views;
use nexus_types::external_api::views::AffinityGroup;
use nexus_types::external_api::views::AntiAffinityGroup;
use nexus_types::external_api::views::Certificate;
use nexus_types::external_api::views::FloatingIp;
use nexus_types::external_api::views::InternetGateway;
use nexus_types::external_api::views::InternetGatewayIpAddress;
use nexus_types::external_api::views::InternetGatewayIpPool;
use nexus_types::external_api::views::IpPool;
use nexus_types::external_api::views::IpPoolRange;
use nexus_types::external_api::views::User;
use nexus_types::external_api::views::VpcSubnet;
use nexus_types::external_api::views::{Project, Silo, Vpc, VpcRouter};
use nexus_types::identity::Resource;
use nexus_types::internal_api::params as internal_params;
use omicron_common::api::external::AffinityPolicy;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::Error;
use omicron_common::api::external::FailureDomain;
use omicron_common::api::external::Generation;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceAutoRestartPolicy;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::InstanceCpuPlatform;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRoute;
use omicron_common::api::external::UserId;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::SharedDatasetConfig;
use omicron_common::zpool_name::ZpoolName;
use omicron_sled_agent::sim::SledAgent;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SiloUserUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use slog::debug;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub async fn objects_list_page_authz<ItemType>(
    client: &ClientTestContext,
    path: &str,
) -> dropshot::ResultsPage<ItemType>
where
    ItemType: serde::de::DeserializeOwned,
{
    NexusRequest::object_get(client, path)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body()
        .unwrap()
}

pub async fn object_get<OutputType>(
    client: &ClientTestContext,
    path: &str,
) -> OutputType
where
    OutputType: serde::de::DeserializeOwned,
{
    NexusRequest::object_get(client, path)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to make \"GET\" request to {path}: {e}")
        })
        .parsed_body()
        .unwrap()
}

pub async fn object_get_error(
    client: &ClientTestContext,
    path: &str,
    status: StatusCode,
) -> HttpErrorResponseBody {
    NexusRequest::new(
        RequestBuilder::new(client, Method::GET, path)
            .expect_status(Some(status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<HttpErrorResponseBody>()
    .unwrap()
}

pub async fn object_create<InputType, OutputType>(
    client: &ClientTestContext,
    path: &str,
    input: &InputType,
) -> OutputType
where
    InputType: serde::Serialize,
    OutputType: serde::de::DeserializeOwned,
{
    NexusRequest::objects_post(client, path, input)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to make \"POST\" request to {path}: {e}")
        })
        .parsed_body()
        .unwrap()
}

/// Make a POST, assert status code, return error response body
pub async fn object_create_error<InputType>(
    client: &ClientTestContext,
    path: &str,
    input: &InputType,
    status: StatusCode,
) -> HttpErrorResponseBody
where
    InputType: serde::Serialize,
{
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, path)
            .body(Some(&input))
            .expect_status(Some(status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|err| panic!("Error creating object with {path}: {err}"))
    .parsed_body::<HttpErrorResponseBody>()
    .unwrap()
}

pub async fn object_put<InputType, OutputType>(
    client: &ClientTestContext,
    path: &str,
    input: &InputType,
) -> OutputType
where
    InputType: serde::Serialize,
    OutputType: serde::de::DeserializeOwned,
{
    NexusRequest::object_put(client, path, Some(input))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to make \"PUT\" request to {path}: {e}")
        })
        .parsed_body()
        .unwrap()
}

pub async fn object_put_error<InputType>(
    client: &ClientTestContext,
    path: &str,
    input: &InputType,
    status: StatusCode,
) -> HttpErrorResponseBody
where
    InputType: serde::Serialize,
{
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, path)
            .body(Some(&input))
            .expect_status(Some(status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<HttpErrorResponseBody>()
    .unwrap()
}

pub async fn object_delete(client: &ClientTestContext, path: &str) {
    NexusRequest::object_delete(client, path)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to make \"DELETE\" request to {path}: {e}")
        });
}

pub async fn object_delete_error(
    client: &ClientTestContext,
    path: &str,
    status: StatusCode,
) -> HttpErrorResponseBody {
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, path)
            .expect_status(Some(status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<HttpErrorResponseBody>()
    .unwrap()
}

/// Create an IP pool with a single range for testing.
///
/// The IP range may be specified if it's important for testing the behavior
/// around specific subnets, or a large subnet (2 ** 16 addresses) will be
/// provided, if the `ip_range` argument is `None`.
pub async fn create_ip_pool(
    client: &ClientTestContext,
    pool_name: &str,
    ip_range: Option<IpRange>,
) -> (IpPool, IpPoolRange) {
    let pool = object_create(
        client,
        "/v1/system/ip-pools",
        &params::IpPoolCreate::new(
            IdentityMetadataCreateParams {
                name: pool_name.parse().unwrap(),
                description: String::from("an ip pool"),
            },
            ip_range
                .map(|r| r.version())
                .unwrap_or_else(|| views::IpVersion::V4),
        ),
    )
    .await;

    let ip_range = ip_range.unwrap_or_else(|| {
        use std::net::Ipv4Addr;
        IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 0),
            Ipv4Addr::new(10, 0, 255, 255),
        ))
        .unwrap()
    });
    let url = format!("/v1/system/ip-pools/{}/ranges/add", pool_name);
    let range = object_create(client, &url, &ip_range).await;
    (pool, range)
}

/// Create a multicast IP pool with a multicast range for testing.
///
/// The multicast IP range may be specified if it's important for testing specific
/// multicast addresses, or a default multicast range (224.1.0.0 - 224.1.255.255)
/// will be provided if the `ip_range` argument is `None`.
pub async fn create_multicast_ip_pool(
    client: &ClientTestContext,
    pool_name: &str,
    ip_range: Option<IpRange>,
) -> (IpPool, IpPoolRange) {
    let pool = object_create(
        client,
        "/v1/system/ip-pools",
        &params::IpPoolCreate::new_multicast(
            IdentityMetadataCreateParams {
                name: pool_name.parse().unwrap(),
                description: String::from("a multicast ip pool"),
            },
            ip_range
                .map(|r| r.version())
                .unwrap_or_else(|| views::IpVersion::V4),
            None, // No switch port uplinks for test helper
            None, // No VLAN ID for test helper
        ),
    )
    .await;

    let ip_range = ip_range.unwrap_or_else(|| {
        use std::net::Ipv4Addr;
        IpRange::try_from((
            Ipv4Addr::new(224, 1, 0, 0),
            Ipv4Addr::new(224, 1, 255, 255),
        ))
        .unwrap()
    });
    let url = format!("/v1/system/ip-pools/{}/ranges/add", pool_name);
    let range = object_create(client, &url, &ip_range).await;
    (pool, range)
}

pub async fn link_ip_pool(
    client: &ClientTestContext,
    pool_name: &str,
    silo_id: &Uuid,
    is_default: bool,
) {
    let link =
        params::IpPoolLinkSilo { silo: NameOrId::Id(*silo_id), is_default };
    let url = format!("/v1/system/ip-pools/{pool_name}/silos");
    object_create::<params::IpPoolLinkSilo, views::IpPoolSiloLink>(
        client, &url, &link,
    )
    .await;
}

/// What you want for any test that is not testing IP logic specifically
pub async fn create_default_ip_pool(
    client: &ClientTestContext,
) -> views::IpPool {
    let (pool, ..) = create_ip_pool(&client, "default", None).await;
    link_ip_pool(&client, "default", &DEFAULT_SILO.id(), true).await;
    pool
}

pub async fn create_floating_ip(
    client: &ClientTestContext,
    fip_name: &str,
    project: &str,
    ip: Option<IpAddr>,
    parent_pool_name: Option<&str>,
) -> FloatingIp {
    object_create(
        client,
        &format!("/v1/floating-ips?project={project}"),
        &params::FloatingIpCreate {
            identity: IdentityMetadataCreateParams {
                name: fip_name.parse().unwrap(),
                description: String::from("a floating ip"),
            },
            ip,
            pool: parent_pool_name.map(|v| NameOrId::Name(v.parse().unwrap())),
        },
    )
    .await
}

pub async fn create_certificate(
    client: &ClientTestContext,
    cert_name: &str,
    cert: String,
    key: String,
) -> Certificate {
    let url = "/v1/certificates".to_string();
    object_create(
        client,
        &url,
        &params::CertificateCreate {
            identity: IdentityMetadataCreateParams {
                name: cert_name.parse().unwrap(),
                description: String::from("sells rainsticks"),
            },
            cert,
            key,
            service: shared::ServiceUsingCertificate::ExternalApi,
        },
    )
    .await
}

pub async fn delete_certificate(client: &ClientTestContext, cert_name: &str) {
    let url = format!("/v1/certificates/{}", cert_name);
    object_delete(client, &url).await
}

pub async fn create_switch(
    client: &ClientTestContext,
    serial: &str,
    part: &str,
    revision: u32,
    rack_id: Uuid,
) -> views::Switch {
    object_put(
        client,
        "/switches",
        &internal_params::SwitchPutRequest {
            baseboard: Baseboard {
                serial: serial.to_string(),
                part: part.to_string(),
                revision,
            },
            rack_id,
        },
    )
    .await
}

pub async fn create_silo(
    client: &ClientTestContext,
    silo_name: &str,
    discoverable: bool,
    identity_mode: shared::SiloIdentityMode,
) -> Silo {
    object_create(
        client,
        "/v1/system/silos",
        &params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: silo_name.parse().unwrap(),
                description: "a silo".to_string(),
            },
            quotas: params::SiloQuotasCreate::arbitrarily_high_default(),
            discoverable,
            identity_mode,
            admin_group_name: None,
            tls_certificates: vec![],
            mapped_fleet_roles: Default::default(),
        },
    )
    .await
}

/// This contains slight variations of Nexus external API types that are to be
/// used for testing purposes only. These new types are needed because we want
/// special protections for how passwords are handled in the Nexus server,
/// including protections against user passwords accidentally becoming
/// serialized.
pub mod test_params {
    /// Testing only version of the the create-time parameters for a `User`
    #[derive(Clone, serde::Serialize)]
    pub struct UserCreate {
        /// username used to log in
        pub external_id: super::UserId,
        /// how to set the user's login password
        pub password: UserPassword,
    }

    /// Testing only version of parameters for setting a user's password
    #[derive(Clone, serde::Serialize)]
    #[serde(rename_all = "snake_case")]
    #[serde(tag = "mode", content = "value")]
    pub enum UserPassword {
        /// Sets the user's password to the provided value
        Password(String),
        /// Invalidates any current password (disabling password authentication)
        LoginDisallowed,
    }

    /// Testing only credentials for local user login
    #[derive(Clone, serde::Serialize)]
    pub struct UsernamePasswordCredentials {
        pub username: super::UserId,
        pub password: String,
    }
}

pub async fn create_local_user(
    client: &ClientTestContext,
    silo: &views::Silo,
    username: &UserId,
    password: test_params::UserPassword,
) -> User {
    let silo_name = &silo.identity.name;
    let url =
        format!("/v1/system/identity-providers/local/users?silo={}", silo_name);
    object_create(
        client,
        &url,
        &test_params::UserCreate { external_id: username.to_owned(), password },
    )
    .await
}

pub async fn create_project(
    client: &ClientTestContext,
    project_name: &str,
) -> Project {
    object_create(
        client,
        "/v1/projects",
        &params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: project_name.parse().unwrap(),
                description: "a pier".to_string(),
            },
        },
    )
    .await
}

pub async fn create_disk(
    client: &ClientTestContext,
    project_name: &str,
    disk_name: &str,
) -> Disk {
    let url = format!("/v1/disks?project={}", project_name);
    object_create(
        client,
        &url,
        &params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: disk_name.parse().unwrap(),
                description: String::from("sells rainsticks"),
            },
            disk_source: params::DiskSource::Blank {
                block_size: params::BlockSize::try_from(512).unwrap(),
            },
            size: ByteCount::from_gibibytes_u32(1),
        },
    )
    .await
}

pub async fn create_disk_from_snapshot(
    client: &ClientTestContext,
    project_name: &str,
    disk_name: &str,
    snapshot_id: Uuid,
) -> Disk {
    let url = format!("/v1/disks?project={}", project_name);
    object_create(
        client,
        &url,
        &params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: disk_name.parse().unwrap(),
                description: String::from("sells rainsticks"),
            },
            disk_source: params::DiskSource::Snapshot { snapshot_id },
            size: ByteCount::from_gibibytes_u32(1),
        },
    )
    .await
}

pub async fn create_snapshot(
    client: &ClientTestContext,
    project_name: &str,
    disk_name: &str,
    snapshot_name: &str,
) -> views::Snapshot {
    let snapshots_url = format!("/v1/snapshots?project={}", project_name);

    object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: snapshot_name.parse().unwrap(),
                description: format!("snapshot {:?}", snapshot_name),
            },
            disk: disk_name.to_string().try_into().unwrap(),
        },
    )
    .await
}

pub async fn delete_disk(
    client: &ClientTestContext,
    project_name: &str,
    disk_name: &str,
) {
    let url = format!("/v1/disks/{}?project={}", disk_name, project_name);
    object_delete(client, &url).await
}

pub async fn delete_snapshot(
    client: &ClientTestContext,
    project_name: &str,
    snapshot_name: &str,
) {
    let url =
        format!("/v1/snapshots/{}?project={}", snapshot_name, project_name);
    object_delete(client, &url).await
}

pub async fn create_alpine_project_image(
    client: &ClientTestContext,
    project_name: &str,
    image_name: &str,
) -> views::Image {
    let images_url = format!("/v1/images?project={}", project_name);
    object_create(
        client,
        &images_url,
        &params::ImageCreate {
            identity: IdentityMetadataCreateParams {
                name: image_name.parse().unwrap(),
                description: String::from(
                    "you can boot any image, as long as it's alpine",
                ),
            },
            source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
            os: "alpine".to_string(),
            version: "edge".to_string(),
        },
    )
    .await
}

pub async fn create_project_image_from_snapshot(
    client: &ClientTestContext,
    project_name: &str,
    image_name: &str,
    snapshot_id: Uuid,
) -> views::Image {
    let images_url = format!("/v1/images?project={}", project_name);
    object_create(
        client,
        &images_url,
        &params::ImageCreate {
            identity: IdentityMetadataCreateParams {
                name: image_name.parse().unwrap(),
                description: String::from("it's an image alright"),
            },
            source: params::ImageSource::Snapshot { id: snapshot_id },
            os: "os".to_string(),
            version: "version".to_string(),
        },
    )
    .await
}

pub async fn delete_image(
    client: &ClientTestContext,
    project_name: &str,
    image_name: &str,
) {
    let url = format!("/v1/image/{}?project={}", image_name, project_name);
    object_delete(client, &url).await
}

/// Creates an instance with a default NIC and no disks.
///
/// Wrapper around [`create_instance_with`].
pub async fn create_instance(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
) -> Instance {
    create_instance_with(
        client,
        project_name,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        // Disks=
        Vec::<params::InstanceDiskAttachment>::new(),
        // External IPs=
        Vec::<params::ExternalIpCreate>::new(),
        true,
        Default::default(),
        None,
    )
    .await
}

/// Creates an instance with attached resources.
// I know, Clippy. I don't like it either...
#[allow(clippy::too_many_arguments)]
pub async fn create_instance_with(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
    nics: &params::InstanceNetworkInterfaceAttachment,
    disks: Vec<params::InstanceDiskAttachment>,
    external_ips: Vec<params::ExternalIpCreate>,
    start: bool,
    auto_restart_policy: Option<InstanceAutoRestartPolicy>,
    cpu_platform: Option<InstanceCpuPlatform>,
) -> Instance {
    let url = format!("/v1/instances?project={}", project_name);

    object_create(
        client,
        &url,
        &params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: format!("instance {:?}", instance_name),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: "the-host".parse().unwrap(),
            user_data:
                b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
            ssh_public_keys: Some(Vec::new()),
            network_interfaces: nics.clone(),
            external_ips,
            disks,
            boot_disk: None,
            cpu_platform,
            start,
            auto_restart_policy,
            anti_affinity_groups: Vec::new(),
            multicast_groups: Vec::new(),
        },
    )
    .await
}

/// Creates an instance, asserting a status code and returning the error.
///
/// Note that this accepts any serializable body, which allows users to create
/// invalid inputs to test our parameter validation.
pub async fn create_instance_with_error<T>(
    client: &ClientTestContext,
    project_name: &str,
    body: &T,
    status: StatusCode,
) -> HttpErrorResponseBody
where
    T: serde::Serialize,
{
    let url = format!("/v1/instances?project={project_name}");
    object_create_error(client, &url, body, status).await
}

pub async fn create_affinity_group(
    client: &ClientTestContext,
    project_name: &str,
    group_name: &str,
) -> AffinityGroup {
    object_create(
        &client,
        format!("/v1/affinity-groups?project={}", &project_name).as_str(),
        &params::AffinityGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: group_name.parse().unwrap(),
                description: String::from("affinity group description"),
            },
            policy: AffinityPolicy::Fail,
            failure_domain: FailureDomain::Sled,
        },
    )
    .await
}

pub async fn create_anti_affinity_group(
    client: &ClientTestContext,
    project_name: &str,
    group_name: &str,
) -> AntiAffinityGroup {
    object_create(
        &client,
        format!("/v1/anti-affinity-groups?project={}", &project_name).as_str(),
        &params::AntiAffinityGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: group_name.parse().unwrap(),
                description: String::from("anti-affinity group description"),
            },
            policy: AffinityPolicy::Fail,
            failure_domain: FailureDomain::Sled,
        },
    )
    .await
}

pub async fn create_vpc(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
) -> Vpc {
    object_create(
        &client,
        format!("/v1/vpcs?project={}", &project_name).as_str(),
        &params::VpcCreate {
            identity: IdentityMetadataCreateParams {
                name: vpc_name.parse().unwrap(),
                description: "vpc description".to_string(),
            },
            ipv6_prefix: None,
            dns_name: "abc".parse().unwrap(),
        },
    )
    .await
}

// TODO: probably would be cleaner to replace these helpers with something that
// just generates the create params since that's the noisiest part
pub async fn create_vpc_with_error(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    status: StatusCode,
) -> HttpErrorResponseBody {
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            format!("/v1/vpcs?project={}", &project_name).as_str(),
        )
        .body(Some(&params::VpcCreate {
            identity: IdentityMetadataCreateParams {
                name: vpc_name.parse().unwrap(),
                description: String::from("vpc description"),
            },
            ipv6_prefix: None,
            dns_name: "abc".parse().unwrap(),
        }))
        .expect_status(Some(status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

pub async fn create_vpc_subnet(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    subnet_name: &str,
    ipv4_block: Ipv4Net,
    ipv6_block: Option<Ipv6Net>,
    custom_router: Option<&str>,
) -> VpcSubnet {
    object_create(
        &client,
        &format!("/v1/vpc-subnets?project={project_name}&vpc={vpc_name}"),
        &params::VpcSubnetCreate {
            identity: IdentityMetadataCreateParams {
                name: subnet_name.parse().unwrap(),
                description: "vpc description".to_string(),
            },
            ipv4_block,
            ipv6_block,
            custom_router: custom_router
                .map(|n| NameOrId::Name(n.parse().unwrap())),
        },
    )
    .await
}

pub async fn create_router(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    router_name: &str,
) -> VpcRouter {
    NexusRequest::objects_post(
        &client,
        format!("/v1/vpc-routers?project={}&vpc={}", &project_name, &vpc_name)
            .as_str(),
        &params::VpcRouterCreate {
            identity: IdentityMetadataCreateParams {
                name: router_name.parse().unwrap(),
                description: String::from("router description"),
            },
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

pub async fn create_route(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    router_name: &str,
    route_name: &str,
    destination: RouteDestination,
    target: RouteTarget,
) -> RouterRoute {
    NexusRequest::objects_post(
        &client,
        format!(
            "/v1/vpc-router-routes?project={}&vpc={}&router={}",
            &project_name, &vpc_name, &router_name
        )
        .as_str(),
        &params::RouterRouteCreate {
            identity: IdentityMetadataCreateParams {
                name: route_name.parse().unwrap(),
                description: String::from("route description"),
            },
            target,
            destination,
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
pub async fn create_route_with_error(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    router_name: &str,
    route_name: &str,
    destination: RouteDestination,
    target: RouteTarget,
    status: StatusCode,
) -> HttpErrorResponseBody {
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            format!(
                "/v1/vpc-router-routes?project={}&vpc={}&router={}",
                &project_name, &vpc_name, &router_name
            )
            .as_str(),
        )
        .body(Some(&params::RouterRouteCreate {
            identity: IdentityMetadataCreateParams {
                name: route_name.parse().unwrap(),
                description: String::from("route description"),
            },
            target,
            destination,
        }))
        .expect_status(Some(status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

pub async fn create_internet_gateway(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    internet_gateway_name: &str,
) -> InternetGateway {
    NexusRequest::objects_post(
        &client,
        format!(
            "/v1/internet-gateways?project={}&vpc={}",
            &project_name, &vpc_name
        )
        .as_str(),
        &params::VpcRouterCreate {
            identity: IdentityMetadataCreateParams {
                name: internet_gateway_name.parse().unwrap(),
                description: String::from("internet gateway description"),
            },
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

pub async fn delete_internet_gateway(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    internet_gateway_name: &str,
    cascade: bool,
) {
    NexusRequest::object_delete(
        &client,
        format!(
            "/v1/internet-gateways/{}?project={}&vpc={}&cascade={}",
            &internet_gateway_name, &project_name, &vpc_name, cascade
        )
        .as_str(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

pub async fn attach_ip_pool_to_igw(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
    ip_pool_name: &str,
    attachment_name: &str,
) -> InternetGatewayIpPool {
    let url = format!(
        "/v1/internet-gateway-ip-pools?project={}&vpc={}&gateway={}",
        project_name, vpc_name, igw_name,
    );

    let ip_pool: Name = ip_pool_name.parse().unwrap();
    NexusRequest::objects_post(
        &client,
        url.as_str(),
        &params::InternetGatewayIpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: attachment_name.parse().unwrap(),
                description: String::from("attached pool descriptoion"),
            },
            ip_pool: NameOrId::Name(ip_pool),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

pub async fn detach_ip_pool_from_igw(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
    ip_pool_name: &str,
    cascade: bool,
) {
    let url = format!(
        "/v1/internet-gateway-ip-pools/{}?project={}&vpc={}&gateway={}&cascade={}",
        ip_pool_name, project_name, vpc_name, igw_name, cascade,
    );

    NexusRequest::object_delete(&client, url.as_str())
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
}

pub async fn attach_ip_address_to_igw(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
    address: IpAddr,
    attachment_name: &str,
) -> InternetGatewayIpAddress {
    let url = format!(
        "/v1/internet-gateway-ip-addresses?project={}&vpc={}&gateway={}",
        project_name, vpc_name, igw_name,
    );

    NexusRequest::objects_post(
        &client,
        url.as_str(),
        &params::InternetGatewayIpAddressCreate {
            identity: IdentityMetadataCreateParams {
                name: attachment_name.parse().unwrap(),
                description: String::from("attached pool descriptoion"),
            },
            address,
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

pub async fn detach_ip_address_from_igw(
    client: &ClientTestContext,
    project_name: &str,
    vpc_name: &str,
    igw_name: &str,
    attachment_name: &str,
    cascade: bool,
) {
    let url = format!(
        "/v1/internet-gateway-ip-addresses/{}?project={}&vpc={}&gateway={}&cascade={}",
        attachment_name, project_name, vpc_name, igw_name, cascade
    );

    NexusRequest::object_delete(&client, url.as_str())
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
}

/// Assert that the utilization of the provided pool matches expectations.
///
/// Note that the third argument is the number of _allocated_ addresses as an
/// integer. This is compared against the count of remaining addresses
/// internally, which is what the API returns.
pub async fn assert_ip_pool_utilization(
    client: &ClientTestContext,
    pool_name: &str,
    allocated: u32,
    capacity: f64,
) {
    let url = format!("/v1/system/ip-pools/{}/utilization", pool_name);
    let utilization: views::IpPoolUtilization = object_get(client, &url).await;
    let remaining = capacity - f64::from(allocated);
    assert_eq!(
        remaining, utilization.remaining,
        "IP pool '{}': expected {} remaining, got {}",
        pool_name, remaining, utilization.remaining,
    );
    assert_eq!(
        capacity, utilization.capacity,
        "IP pool '{}': expected {} capacity, got {:?}",
        pool_name, capacity, utilization.capacity,
    );
}

/// Grant a role on a resource to a user
///
/// * `grant_resource_url`: URL of the resource we're granting the role on
/// * `grant_role`: the role we're granting
/// * `grant_user`: the uuid of the user we're granting the role to
/// * `run_as`: the user _doing_ the granting
pub async fn grant_iam<T>(
    client: &ClientTestContext,
    grant_resource_url: &str,
    grant_role: T,
    grant_user: SiloUserUuid,
    run_as: AuthnMode,
) where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let policy_url = format!("{}/policy", grant_resource_url);
    let existing_policy: shared::Policy<T> =
        NexusRequest::object_get(client, &policy_url)
            .authn_as(run_as.clone())
            .execute()
            .await
            .expect("failed to fetch policy")
            .parsed_body()
            .expect("failed to parse policy");
    let new_role_assignment =
        shared::RoleAssignment::for_silo_user(grant_user, grant_role);
    let new_role_assignments = existing_policy
        .role_assignments
        .into_iter()
        .chain(std::iter::once(new_role_assignment))
        .collect();

    let new_policy = shared::Policy { role_assignments: new_role_assignments };

    // TODO-correctness use etag when we have it
    NexusRequest::object_put(client, &policy_url, Some(&new_policy))
        .authn_as(run_as)
        .execute()
        .await
        .expect("failed to update policy");
}

pub async fn project_get(
    client: &ClientTestContext,
    project_url: &str,
) -> Project {
    NexusRequest::object_get(client, project_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to get project")
        .parsed_body()
        .expect("failed to parse Project")
}

pub async fn projects_list(
    client: &ClientTestContext,
    projects_url: &str,
    initial_params: &str,
    limit: Option<usize>,
) -> Vec<Project> {
    NexusRequest::iter_collection_authn(
        client,
        projects_url,
        initial_params,
        limit,
    )
    .await
    .expect("failed to list projects")
    .all_items
    .into_iter()
    .collect()
}

/// Log in with test suite password, return session cookie
pub async fn create_console_session<N: NexusServer>(
    cptestctx: &ControlPlaneTestContext<N>,
) -> String {
    let testctx = &cptestctx.external_client;
    let url = format!("/v1/login/{}/local", cptestctx.silo_name);
    let credentials = test_params::UsernamePasswordCredentials {
        username: cptestctx.user_name.as_ref().parse().unwrap(),
        password: TEST_SUITE_PASSWORD.to_string(),
    };
    let login = RequestBuilder::new(&testctx, Method::POST, &url)
        .body(Some(&credentials))
        .expect_status(Some(StatusCode::NO_CONTENT))
        .execute()
        .await
        .expect("failed to log in");

    let session_cookie = {
        let header_name = header::SET_COOKIE;
        login.headers.get(header_name).unwrap().to_str().unwrap().to_string()
    };
    let (session_token, rest) = session_cookie.split_once("; ").unwrap();

    assert!(session_token.starts_with("session="));
    assert_eq!(rest, "Path=/; HttpOnly; SameSite=Lax; Max-Age=86400");

    session_token.to_string()
}

#[derive(Debug)]
pub struct TestDataset {
    pub id: DatasetUuid,
    pub kind: DatasetKind,
}

pub struct TestZpool {
    pub id: ZpoolUuid,
    pub size: ByteCount,
    datasets: Vec<TestDataset>,
}

impl TestZpool {
    /// Returns the crucible dataset within a zpool.
    ///
    /// Panics if there are zero or more than one crucible datasets within the pool
    pub fn crucible_dataset(&self) -> &TestDataset {
        fn is_crucible(d: &&TestDataset) -> bool {
            d.kind == DatasetKind::Crucible
        }

        assert_eq!(self.datasets.iter().filter(is_crucible).count(), 1);
        self.datasets.iter().find(is_crucible).unwrap()
    }

    /// Returns the debug dataset within a zpool.
    ///
    /// Panics if there are zero or more than one debug datasets within the pool
    pub fn debug_dataset(&self) -> &TestDataset {
        fn is_debug(d: &&TestDataset) -> bool {
            d.kind == DatasetKind::Debug
        }

        assert_eq!(self.datasets.iter().filter(is_debug).count(), 1);
        self.datasets.iter().find(is_debug).unwrap()
    }
}

enum WhichSledAgents {
    Specific(SledUuid),
    List(Vec<SledUuid>),
    All,
}

/// A test utility to add zpools to some sleds.
///
/// This builder helps initialize a set of zpools on sled agents with
/// minimal overhead, though additional disks can be added via the [DiskTest]
/// API after initialization.
pub struct DiskTestBuilder<'a, N: NexusServer> {
    cptestctx: &'a ControlPlaneTestContext<N>,
    sled_agents: WhichSledAgents,
    zpool_count: u32,
}

impl<'a, N: NexusServer> DiskTestBuilder<'a, N> {
    /// Creates a new [DiskTestBuilder] with default configuration options.
    pub fn new(cptestctx: &'a ControlPlaneTestContext<N>) -> Self {
        Self {
            cptestctx,
            sled_agents: WhichSledAgents::Specific(cptestctx.first_sled_id()),
            zpool_count: DiskTest::<'a, N>::DEFAULT_ZPOOL_COUNT,
        }
    }

    /// Specifies that zpools should be added on all sleds
    pub fn on_all_sleds(mut self) -> Self {
        self.sled_agents = WhichSledAgents::All;
        self
    }

    /// Chooses a specific sled where zpools should be added
    pub fn on_specific_sled(mut self, sled_id: SledUuid) -> Self {
        self.sled_agents = WhichSledAgents::Specific(sled_id);
        self
    }

    /// Supply a list of sleds where zpools should be added
    pub fn on_these_sleds(mut self, sled_ids: Vec<SledUuid>) -> Self {
        self.sled_agents = WhichSledAgents::List(sled_ids);
        self
    }

    /// Selects a specific number of zpools to be created
    pub fn with_zpool_count(mut self, count: u32) -> Self {
        self.zpool_count = count;
        self
    }

    /// Creates a DiskTest, actually creating the requested zpools.
    pub async fn build(self) -> DiskTest<'a, N> {
        DiskTest::new_from_builder(
            self.cptestctx,
            self.sled_agents,
            self.zpool_count,
        )
        .await
    }
}

struct PerSledDiskState {
    zpools: Vec<TestZpool>,
}

pub struct ZpoolIterator<'a> {
    sleds: &'a BTreeMap<SledUuid, PerSledDiskState>,
    sled: Option<SledUuid>,
    index: usize,
}

impl<'a> Iterator for ZpoolIterator<'a> {
    type Item = &'a TestZpool;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(sled_id) = self.sled else {
                return None;
            };

            let Some(pools) = self.sleds.get(&sled_id) else {
                return None;
            };

            if let Some(pool) = pools.zpools.get(self.index) {
                self.index += 1;
                return Some(pool);
            }

            self.sled = self
                .sleds
                .range((
                    std::ops::Bound::Excluded(&sled_id),
                    std::ops::Bound::Unbounded,
                ))
                .map(|(id, _)| *id)
                .next();
            self.index = 0;
        }
    }
}

pub struct DiskTest<'a, N: NexusServer> {
    cptestctx: &'a ControlPlaneTestContext<N>,
    sleds: BTreeMap<SledUuid, PerSledDiskState>,
    generation: Generation,
}

impl<'a, N: NexusServer> DiskTest<'a, N> {
    pub const DEFAULT_ZPOOL_SIZE_GIB: u32 = 16;
    pub const DEFAULT_ZPOOL_COUNT: u32 = 3;

    /// Creates a new [DiskTest] with default configuration.
    ///
    /// This is the same as calling [DiskTestBuilder::new] and
    /// [DiskTestBuilder::build].
    pub async fn new(cptestctx: &'a ControlPlaneTestContext<N>) -> Self {
        DiskTestBuilder::new(cptestctx).build().await
    }

    async fn new_from_builder(
        cptestctx: &'a ControlPlaneTestContext<N>,
        sled_agents: WhichSledAgents,
        zpool_count: u32,
    ) -> Self {
        let input_sleds = match sled_agents {
            WhichSledAgents::Specific(id) => {
                vec![id]
            }

            WhichSledAgents::List(ids) => ids.clone(),

            WhichSledAgents::All => cptestctx
                .all_sled_agents()
                .map(|agent| agent.sled_agent.id)
                .collect(),
        };

        // ControlPlaneTestContext sets up initial configs; find the max
        // generation of all our sleds and use that as the base for any new
        // configs we produce.
        let generation = cptestctx
            .all_sled_agents()
            .filter_map(|agent| {
                if input_sleds.contains(&agent.sled_agent.id) {
                    Some(agent
                    .sled_agent
                    .datasets_config_list()
                    .expect(
                        "dataset config populated by ControlPlaneTestContext",
                    )
                    .generation)
                } else {
                    None
                }
            })
            .max()
            .expect("at least one sled specified");

        let mut sleds = BTreeMap::new();
        for sled_id in input_sleds {
            sleds.insert(sled_id, PerSledDiskState { zpools: vec![] });
        }

        let mut disk_test = Self { cptestctx, sleds, generation };

        for sled_id in
            disk_test.sleds.keys().cloned().collect::<Vec<SledUuid>>()
        {
            for _ in 0..zpool_count {
                disk_test.add_zpool_with_datasets(sled_id).await;
            }
        }
        disk_test.propagate_datasets_to_sleds().await;

        disk_test
    }

    pub async fn add_blueprint_disks(&mut self, blueprint: &Blueprint) {
        for (sled_id, sled_config) in blueprint.sleds.iter() {
            for disk in &sled_config.disks {
                self.add_zpool_with_datasets_ext(
                    *sled_id,
                    disk.id,
                    disk.pool_id,
                    vec![TestDataset {
                        id: DatasetUuid::new_v4(),
                        kind: DatasetKind::Crucible,
                    }],
                    Self::DEFAULT_ZPOOL_SIZE_GIB,
                )
                .await;
            }
        }
    }

    /// Adds the zpool and datasets into the database.
    ///
    /// Does not inform sled agents to use these pools.
    ///
    /// See: [Self::propagate_datasets_to_sleds] if you want to send
    /// this configuration to a simulated sled agent.
    pub async fn add_zpool_with_datasets(&mut self, sled_id: SledUuid) {
        self.add_zpool_with_datasets_ext(
            sled_id,
            PhysicalDiskUuid::new_v4(),
            ZpoolUuid::new_v4(),
            vec![
                TestDataset {
                    id: DatasetUuid::new_v4(),
                    kind: DatasetKind::Crucible,
                },
                TestDataset {
                    id: DatasetUuid::new_v4(),
                    kind: DatasetKind::Debug,
                },
            ],
            Self::DEFAULT_ZPOOL_SIZE_GIB,
        )
        .await
    }

    /// Propagate the dataset configuration to all Sled Agents.
    // TODO: Ideally, we should do the following:
    // Also call a similar method to invoke the "omicron_physical_disks_ensure" API. Right now,
    // we aren't calling this at all for the simulated sled agent, which only works because
    // the simulated sled agent simply treats this as a stored config, rather than processing it
    // to actually provide a different view of storage.
    pub async fn propagate_datasets_to_sleds(&mut self) {
        let cptestctx = self.cptestctx;

        for (sled_id, PerSledDiskState { zpools }) in &self.sleds {
            // Grab the "SledAgent" object -- we'll be contacting it shortly.
            let sleds = cptestctx.all_sled_agents();
            let sled_agent = sleds
                .into_iter()
                .find_map(|server| {
                    if server.sled_agent.id == *sled_id {
                        Some(server.sled_agent.clone())
                    } else {
                        None
                    }
                })
                .expect("Cannot find sled");

            // Configure the Sled to use all datasets we created
            let datasets = zpools
                .iter()
                .flat_map(|zpool| zpool.datasets.iter().map(|d| (zpool.id, d)))
                .map(|(zpool_id, TestDataset { id, kind })| {
                    (
                        *id,
                        DatasetConfig {
                            id: *id,
                            name: DatasetName::new(
                                ZpoolName::new_external(zpool_id),
                                kind.clone(),
                            ),
                            inner: SharedDatasetConfig::default(),
                        },
                    )
                })
                .collect();

            self.generation = self.generation.next();
            let generation = self.generation;
            let dataset_config = DatasetsConfig { generation, datasets };
            let res = sled_agent.datasets_ensure(dataset_config).expect(
                "Should have been able to ensure datasets, but could not.
                     Did someone else already attempt to ensure datasets?",
            );
            assert!(!res.has_error());
        }
    }

    fn get_sled(&self, sled_id: SledUuid) -> Arc<SledAgent> {
        let sleds = self.cptestctx.all_sled_agents();
        sleds
            .into_iter()
            .find_map(|server| {
                if server.sled_agent.id == sled_id {
                    Some(server.sled_agent.clone())
                } else {
                    None
                }
            })
            .expect("Cannot find sled")
    }

    pub fn zpools(&self) -> ZpoolIterator<'_> {
        ZpoolIterator {
            sleds: &self.sleds,
            sled: self.sleds.keys().next().copied(),
            index: 0,
        }
    }

    /// Adds the zpool and datasets into the database, with additional
    /// configuration.
    ///
    /// Does not inform sled agents to use these pools.
    ///
    /// See: [Self::propagate_datasets_to_sleds] if you want to send
    /// this configuration to a simulated sled agent.
    pub async fn add_zpool_with_datasets_ext(
        &mut self,
        sled_id: SledUuid,
        physical_disk_id: PhysicalDiskUuid,
        zpool_id: ZpoolUuid,
        datasets: Vec<TestDataset>,
        gibibytes: u32,
    ) {
        let cptestctx = self.cptestctx;

        // To get a dataset, we actually need to create a new simulated physical
        // disk, zpool, and dataset, all contained within one another.
        let zpool = TestZpool {
            id: zpool_id,
            size: ByteCount::from_gibibytes_u32(gibibytes),
            datasets,
        };

        let disk_identity = DiskIdentity {
            vendor: "test-vendor".into(),
            serial: format!("totally-unique-serial: {}", physical_disk_id),
            model: "test-model".into(),
        };

        let physical_disk_request =
            nexus_types::internal_api::params::PhysicalDiskPutRequest {
                id: physical_disk_id,
                vendor: disk_identity.vendor.clone(),
                serial: disk_identity.serial.clone(),
                model: disk_identity.model.clone(),
                variant:
                    nexus_types::external_api::params::PhysicalDiskKind::U2,
                sled_id,
            };

        let zpool_request =
            nexus_types::internal_api::params::ZpoolPutRequest {
                id: zpool.id,
                physical_disk_id,
                sled_id,
            };

        // Find the sled on which we're adding a zpool
        let sleds = cptestctx.all_sled_agents();
        let sled_agent = sleds
            .into_iter()
            .find_map(|server| {
                if server.sled_agent.id == sled_id {
                    Some(server.sled_agent.clone())
                } else {
                    None
                }
            })
            .expect("Cannot find sled");

        let zpools = &mut self
            .sleds
            .entry(sled_id)
            .or_insert_with(|| PerSledDiskState { zpools: Vec::new() })
            .zpools;

        // Tell the simulated sled agent to create the disk and zpool containing
        // these datasets.

        sled_agent.create_external_physical_disk(
            physical_disk_id,
            disk_identity.clone(),
        );
        sled_agent.create_zpool(
            zpool.id,
            physical_disk_id,
            zpool.size.to_bytes(),
        );

        for dataset in &zpool.datasets {
            let address = if matches!(dataset.kind, DatasetKind::Crucible) {
                // Sled Agent side: Create the Dataset, make sure regions can be
                // created immediately if Nexus requests anything.
                let address =
                    sled_agent.create_crucible_dataset(zpool.id, dataset.id);
                let crucible =
                    sled_agent.get_crucible_dataset(zpool.id, dataset.id);
                crucible
                    .set_create_callback(Box::new(|_| RegionState::Created));

                // Nexus side: Notify Nexus of the physical disk/zpool/dataset
                // combination that exists.

                match address {
                    std::net::SocketAddr::V6(addr) => Some(addr),
                    _ => panic!("Unsupported address type: {address} "),
                }
            } else {
                None
            };

            cptestctx
                .server
                .upsert_test_dataset(
                    physical_disk_request.clone(),
                    zpool_request.clone(),
                    dataset.id,
                    dataset.kind.clone(),
                    address,
                )
                .await;
        }

        let log = &cptestctx.logctx.log;

        // Wait until Nexus has successfully completed an inventory collection
        // which includes this zpool
        wait_for_condition(
            || async {
                let result = cptestctx
                    .server
                    .inventory_collect_and_get_latest_collection()
                    .await;
                let log_result = match &result {
                    Ok(Some(_)) => Ok("found"),
                    Ok(None) => Ok("not found"),
                    Err(error) => Err(error),
                };
                debug!(
                    log,
                    "attempt to fetch latest inventory collection";
                    "result" => ?log_result,
                );

                match result {
                    Ok(None) => Err(CondCheckError::NotYet),
                    Ok(Some(c)) => {
                        let all_zpools = c
                            .sled_agents
                            .iter()
                            .flat_map(|sled_agent| {
                                sled_agent.zpools.iter().map(|z| z.id)
                            })
                            .collect::<std::collections::HashSet<ZpoolUuid>>();

                        if all_zpools.contains(&zpool.id) {
                            Ok(())
                        } else {
                            Err(CondCheckError::NotYet)
                        }
                    }
                    Err(Error::ServiceUnavailable { .. }) => {
                        Err(CondCheckError::NotYet)
                    }
                    Err(error) => Err(CondCheckError::Failed(error)),
                }
            },
            &Duration::from_millis(50),
            &Duration::from_secs(120),
        )
        .await
        .expect("expected to find inventory collection");

        zpools.push(zpool);
    }

    /// Configures all region requests within Crucible datasets to return
    /// "Requested", then "Created".
    pub async fn set_requested_then_created_callback(&self) {
        for (sled_id, state) in &self.sleds {
            for zpool in &state.zpools {
                for dataset in &zpool.datasets {
                    if !matches!(dataset.kind, DatasetKind::Crucible) {
                        continue;
                    }
                    let crucible = self
                        .get_sled(*sled_id)
                        .get_crucible_dataset(zpool.id, dataset.id);
                    let called = std::sync::atomic::AtomicBool::new(false);
                    crucible.set_create_callback(Box::new(move |_| {
                        if !called.load(std::sync::atomic::Ordering::SeqCst) {
                            called.store(
                                true,
                                std::sync::atomic::Ordering::SeqCst,
                            );
                            RegionState::Requested
                        } else {
                            RegionState::Created
                        }
                    }));
                }
            }
        }
    }

    /// Configures all region requests within Crucible datasets to fail
    pub async fn set_always_fail_callback(&self) {
        for (sled_id, state) in &self.sleds {
            for zpool in &state.zpools {
                for dataset in &zpool.datasets {
                    if !matches!(dataset.kind, DatasetKind::Crucible) {
                        continue;
                    }
                    let crucible = self
                        .get_sled(*sled_id)
                        .get_crucible_dataset(zpool.id, dataset.id);
                    crucible
                        .set_create_callback(Box::new(|_| RegionState::Failed));
                }
            }
        }
    }

    /// Returns true if all Crucible resources were cleaned up, false otherwise.
    ///
    /// Note: be careful performing this test when also peforming physical disk
    /// expungement, as Nexus will consider resources on those physical disks
    /// gone and will not attempt to clean them up!
    pub async fn crucible_resources_deleted(&self) -> bool {
        for (sled_id, state) in &self.sleds {
            for zpool in &state.zpools {
                for dataset in &zpool.datasets {
                    if !matches!(dataset.kind, DatasetKind::Crucible) {
                        continue;
                    }
                    let crucible = self
                        .get_sled(*sled_id)
                        .get_crucible_dataset(zpool.id, dataset.id);
                    if !crucible.is_empty() {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Drop all of a zpool's resources
    ///
    /// Call this before checking `crucible_resources_deleted` if the test has
    /// also performed a physical disk policy change to "expunged". Nexus will
    /// _not_ clean up crucible resources on an expunged disk (due to the "gone"
    /// check that it performs), but it's useful for tests to be able to assert
    /// all crucible resources are cleaned up.
    pub async fn remove_zpool(&mut self, zpool_id: ZpoolUuid) {
        for sled in self.sleds.values_mut() {
            sled.zpools.retain(|zpool| zpool.id != zpool_id);
        }
    }
}
