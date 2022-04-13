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

pub async fn create_silo(
    client: &ClientTestContext,
    silo_name: &str,
    discoverable: bool,
) -> Silo {
    object_create(
        client,
        "/silos",
        &params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: silo_name.parse().unwrap(),
                description: "a silo".to_string(),
            },
            discoverable,
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
            snapshot_id: None,
            image_id: None,
            size: ByteCount::from_gibibytes_u32(1),
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
    )
    .await
}

pub async fn create_instance(
    client: &ClientTestContext,
    organization_name: &str,
    project_name: &str,
    instance_name: &str,
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
            memory: ByteCount::from_mebibytes_u32(256),
            hostname: String::from("the_host"),
            user_data:
                b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::Default,
            disks: vec![],
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

pub struct DiskTest {
    pub sled_agent: Arc<SledAgent>,
    pub zpool_id: Uuid,
    pub zpool_size: ByteCount,
    pub dataset_ids: Vec<Uuid>,
}

impl DiskTest {
    // Creates fake physical storage, an organization, and a project.
    pub async fn new(cptestctx: &ControlPlaneTestContext) -> Self {
        let sled_agent = cptestctx.sled_agent.sled_agent.clone();

        // Create a Zpool.
        let zpool_id = Uuid::new_v4();
        let zpool_size = ByteCount::from_gibibytes_u32(10);
        sled_agent.create_zpool(zpool_id, zpool_size.to_bytes()).await;

        // Create multiple Datasets within that Zpool.
        let dataset_count = 3;
        let dataset_ids: Vec<_> =
            (0..dataset_count).map(|_| Uuid::new_v4()).collect();
        for id in &dataset_ids {
            sled_agent.create_crucible_dataset(zpool_id, *id).await;

            // By default, regions are created immediately.
            let crucible = sled_agent.get_crucible_dataset(zpool_id, *id).await;
            crucible
                .set_create_callback(Box::new(|_| RegionState::Created))
                .await;
        }

        Self { sled_agent, zpool_id, zpool_size, dataset_ids }
    }
}
