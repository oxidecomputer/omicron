// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_testing::RequestBuilder;
use crate::ControlPlaneTestContext;

use super::http_testing::AuthnMode;
use super::http_testing::NexusRequest;
use crucible_agent_client::types::State as RegionState;
use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use dropshot::Method;
use http::StatusCode;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_interface::NexusServer;
use nexus_types::external_api::params;
use nexus_types::external_api::params::PhysicalDiskKind;
use nexus_types::external_api::params::UserId;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::Baseboard;
use nexus_types::external_api::shared::IdentityType;
use nexus_types::external_api::shared::IpRange;
use nexus_types::external_api::views;
use nexus_types::external_api::views::Certificate;
use nexus_types::external_api::views::FloatingIp;
use nexus_types::external_api::views::IpPool;
use nexus_types::external_api::views::IpPoolRange;
use nexus_types::external_api::views::User;
use nexus_types::external_api::views::{Project, Silo, Vpc, VpcRouter};
use nexus_types::identity::Resource;
use nexus_types::internal_api::params as internal_params;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::NameOrId;
use omicron_sled_agent::sim::SledAgent;
use std::net::IpAddr;
use std::sync::Arc;
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
    .unwrap()
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
        &params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: pool_name.parse().unwrap(),
                description: String::from("an ip pool"),
            },
        },
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
    revision: i64,
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

pub async fn create_physical_disk(
    client: &ClientTestContext,
    vendor: &str,
    serial: &str,
    model: &str,
    variant: PhysicalDiskKind,
    sled_id: Uuid,
) -> internal_params::PhysicalDiskPutResponse {
    object_put(
        client,
        "/physical-disk",
        &internal_params::PhysicalDiskPutRequest {
            vendor: vendor.to_string(),
            serial: serial.to_string(),
            model: model.to_string(),
            variant,
            sled_id,
        },
    )
    .await
}

pub async fn delete_physical_disk(
    client: &ClientTestContext,
    vendor: &str,
    serial: &str,
    model: &str,
    sled_id: Uuid,
) {
    let body = internal_params::PhysicalDiskDeleteRequest {
        vendor: vendor.to_string(),
        serial: serial.to_string(),
        model: model.to_string(),
        sled_id,
    };

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, "/physical-disk")
            .body(Some(&body))
            .expect_status(Some(http::StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|_| {
        panic!("failed to make \"delete\" request of physical disk")
    });
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

pub async fn create_local_user(
    client: &ClientTestContext,
    silo: &views::Silo,
    username: &UserId,
    password: params::UserPassword,
) -> User {
    let silo_name = &silo.identity.name;
    let url =
        format!("/v1/system/identity-providers/local/users?silo={}", silo_name);
    object_create(
        client,
        &url,
        &params::UserCreate { external_id: username.to_owned(), password },
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

pub async fn delete_disk(
    client: &ClientTestContext,
    project_name: &str,
    disk_name: &str,
) {
    let url = format!("/v1/disks/{}?project={}", disk_name, project_name,);
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
    )
    .await
}

/// Creates an instance with attached resources.
pub async fn create_instance_with(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
    nics: &params::InstanceNetworkInterfaceAttachment,
    disks: Vec<params::InstanceDiskAttachment>,
    external_ips: Vec<params::ExternalIpCreate>,
    start: bool,
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
            start,
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
    grant_user: Uuid,
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
    let new_role_assignment = shared::RoleAssignment {
        identity_type: IdentityType::SiloUser,
        identity_id: grant_user,
        role_name: grant_role,
    };
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

pub struct TestDataset {
    pub id: Uuid,
}

pub struct TestZpool {
    pub id: Uuid,
    pub size: ByteCount,
    pub datasets: Vec<TestDataset>,
}

pub struct DiskTest {
    pub sled_agent: Arc<SledAgent>,
    pub zpools: Vec<TestZpool>,
}

impl DiskTest {
    pub const DEFAULT_ZPOOL_SIZE_GIB: u32 = 10;

    // Creates fake physical storage, an organization, and a project.
    pub async fn new<N: NexusServer>(
        cptestctx: &ControlPlaneTestContext<N>,
    ) -> Self {
        let sled_agent = cptestctx.sled_agent.sled_agent.clone();

        let mut disk_test = Self { sled_agent, zpools: vec![] };

        // Create three Zpools, each 10 GiB, each with one Crucible dataset.
        for _ in 0..3 {
            disk_test
                .add_zpool_with_dataset(cptestctx, Self::DEFAULT_ZPOOL_SIZE_GIB)
                .await;
        }

        disk_test
    }

    pub async fn add_zpool_with_dataset<N: NexusServer>(
        &mut self,
        cptestctx: &ControlPlaneTestContext<N>,
        gibibytes: u32,
    ) {
        let zpool = TestZpool {
            id: Uuid::new_v4(),
            size: ByteCount::from_gibibytes_u32(gibibytes),
            datasets: vec![TestDataset { id: Uuid::new_v4() }],
        };

        self.sled_agent
            .create_external_physical_disk(
                "test-vendor".into(),
                "test-serial".into(),
                "test-model".into(),
            )
            .await;
        self.sled_agent
            .create_zpool(
                zpool.id,
                "test-vendor".into(),
                "test-serial".into(),
                "test-model".into(),
                zpool.size.to_bytes(),
            )
            .await;

        for dataset in &zpool.datasets {
            let address = self
                .sled_agent
                .create_crucible_dataset(zpool.id, dataset.id)
                .await;

            // By default, regions are created immediately.
            let crucible = self
                .sled_agent
                .get_crucible_dataset(zpool.id, dataset.id)
                .await;
            crucible
                .set_create_callback(Box::new(|_| RegionState::Created))
                .await;

            let address = match address {
                std::net::SocketAddr::V6(addr) => addr,
                _ => panic!("Unsupported address type: {address} "),
            };

            cptestctx
                .server
                .upsert_crucible_dataset(dataset.id, zpool.id, address)
                .await;
        }

        self.zpools.push(zpool);
    }

    pub async fn set_requested_then_created_callback(&self) {
        for zpool in &self.zpools {
            for dataset in &zpool.datasets {
                let crucible = self
                    .sled_agent
                    .get_crucible_dataset(zpool.id, dataset.id)
                    .await;
                let called = std::sync::atomic::AtomicBool::new(false);
                crucible
                    .set_create_callback(Box::new(move |_| {
                        if !called.load(std::sync::atomic::Ordering::SeqCst) {
                            called.store(
                                true,
                                std::sync::atomic::Ordering::SeqCst,
                            );
                            RegionState::Requested
                        } else {
                            RegionState::Created
                        }
                    }))
                    .await;
            }
        }
    }

    pub async fn set_always_fail_callback(&self) {
        for zpool in &self.zpools {
            for dataset in &zpool.datasets {
                let crucible = self
                    .sled_agent
                    .get_crucible_dataset(zpool.id, dataset.id)
                    .await;
                crucible
                    .set_create_callback(Box::new(|_| RegionState::Failed))
                    .await;
            }
        }
    }

    /// Returns true if all Crucible resources were cleaned up, false otherwise.
    pub async fn crucible_resources_deleted(&self) -> bool {
        for zpool in &self.zpools {
            for dataset in &zpool.datasets {
                let crucible = self
                    .sled_agent
                    .get_crucible_dataset(zpool.id, dataset.id)
                    .await;
                if !crucible.is_empty().await {
                    return false;
                }
            }
        }

        true
    }
}
