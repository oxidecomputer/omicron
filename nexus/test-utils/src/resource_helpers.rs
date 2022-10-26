// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_testing::RequestBuilder;
use crate::ControlPlaneTestContext;

use super::http_testing::AuthnMode;
use super::http_testing::NexusRequest;
use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use dropshot::Method;
use http::StatusCode;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_nexus::crucible_agent_client::types::State as RegionState;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::shared;
use omicron_nexus::external_api::shared::IdentityType;
use omicron_nexus::external_api::shared::IpRange;
use omicron_nexus::external_api::views::IpPool;
use omicron_nexus::external_api::views::IpPoolRange;
use omicron_nexus::external_api::views::{
    Organization, Project, Silo, Vpc, VpcRouter,
};
use omicron_sled_agent::sim::SledAgent;
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
        .expect("failed to make \"create\" request")
        .parsed_body()
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
    project_path: Option<params::ProjectPath>,
) -> (IpPool, IpPoolRange) {
    let ip_range = ip_range.unwrap_or_else(|| {
        use std::net::Ipv4Addr;
        IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 0),
            Ipv4Addr::new(10, 0, 255, 255),
        ))
        .unwrap()
    });
    let pool = object_create(
        client,
        "/system/ip-pools",
        &params::IpPoolCreate {
            identity: IdentityMetadataCreateParams {
                name: pool_name.parse().unwrap(),
                description: String::from("an ip pool"),
            },
            project: project_path,
        },
    )
    .await;
    let range = object_create(
        client,
        format!("/system/ip-pools/{}/ranges/add", pool_name).as_str(),
        &ip_range,
    )
    .await;
    (pool, range)
}

pub async fn create_silo(
    client: &ClientTestContext,
    silo_name: &str,
    discoverable: bool,
    identity_mode: shared::SiloIdentityMode,
) -> Silo {
    object_create(
        client,
        "/system/silos",
        &params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: silo_name.parse().unwrap(),
                description: "a silo".to_string(),
            },
            discoverable,
            identity_mode,
            admin_group_name: None,
        },
    )
    .await
}

pub async fn create_organization(
    client: &ClientTestContext,
    organization_name: &str,
) -> Organization {
    object_create(
        client,
        "/organizations",
        &params::OrganizationCreate {
            identity: IdentityMetadataCreateParams {
                name: organization_name.parse().unwrap(),
                description: "an org".to_string(),
            },
        },
    )
    .await
}

pub async fn create_project(
    client: &ClientTestContext,
    organization_name: &str,
    project_name: &str,
) -> Project {
    let url = format!("/organizations/{}/projects", &organization_name);
    object_create(
        client,
        &url,
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
    organization_name: &str,
    project_name: &str,
    disk_name: &str,
) -> Disk {
    let url = format!(
        "/organizations/{}/projects/{}/disks",
        organization_name, project_name
    );
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

/// Creates an instance with a default NIC and no disks.
///
/// Wrapper around [`create_instance_with`].
pub async fn create_instance(
    client: &ClientTestContext,
    organization_name: &str,
    project_name: &str,
    instance_name: &str,
) -> Instance {
    create_instance_with(
        client,
        organization_name,
        project_name,
        instance_name,
        &params::InstanceNetworkInterfaceAttachment::Default,
        // Disks=
        vec![],
    )
    .await
}

/// Creates an instance with attached resources.
pub async fn create_instance_with(
    client: &ClientTestContext,
    organization_name: &str,
    project_name: &str,
    instance_name: &str,
    nics: &params::InstanceNetworkInterfaceAttachment,
    disks: Vec<params::InstanceDiskAttachment>,
) -> Instance {
    let url = format!(
        "/organizations/{}/projects/{}/instances",
        organization_name, project_name
    );
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
            hostname: String::from("the_host"),
            user_data:
                b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
            network_interfaces: nics.clone(),
            external_ips: vec![],
            disks,
            start: true,
        },
    )
    .await
}

pub async fn create_vpc(
    client: &ClientTestContext,
    organization_name: &str,
    project_name: &str,
    vpc_name: &str,
) -> Vpc {
    object_create(
        &client,
        format!(
            "/organizations/{}/projects/{}/vpcs",
            &organization_name, &project_name
        )
        .as_str(),
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
    organization_name: &str,
    project_name: &str,
    vpc_name: &str,
    status: StatusCode,
) -> HttpErrorResponseBody {
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            format!(
                "/organizations/{}/projects/{}/vpcs",
                &organization_name, &project_name
            )
            .as_str(),
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
    organization_name: &str,
    project_name: &str,
    vpc_name: &str,
    router_name: &str,
) -> VpcRouter {
    NexusRequest::objects_post(
        &client,
        format!(
            "/organizations/{}/projects/{}/vpcs/{}/routers",
            &organization_name, &project_name, &vpc_name
        )
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
    pub async fn new<N>(cptestctx: &ControlPlaneTestContext<N>) -> Self {
        let sled_agent = cptestctx.sled_agent.sled_agent.clone();

        let mut disk_test = Self { sled_agent, zpools: vec![] };

        // Create three Zpools, each 10 GiB, each with one Crucible dataset.
        for _ in 0..3 {
            disk_test
                .add_zpool_with_dataset(Self::DEFAULT_ZPOOL_SIZE_GIB)
                .await;
        }

        disk_test
    }

    pub async fn add_zpool_with_dataset(&mut self, gibibytes: u32) {
        let zpool = TestZpool {
            id: Uuid::new_v4(),
            size: ByteCount::from_gibibytes_u32(gibibytes),
            datasets: vec![TestDataset { id: Uuid::new_v4() }],
        };

        self.sled_agent.create_zpool(zpool.id, zpool.size.to_bytes()).await;

        for dataset in &zpool.datasets {
            self.sled_agent.create_crucible_dataset(zpool.id, dataset.id).await;

            // By default, regions are created immediately.
            let crucible = self
                .sled_agent
                .get_crucible_dataset(zpool.id, dataset.id)
                .await;
            crucible
                .set_create_callback(Box::new(|_| RegionState::Created))
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
