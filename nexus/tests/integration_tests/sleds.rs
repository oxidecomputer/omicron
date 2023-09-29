// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for APIs against sled-based endpoints.

use camino::Utf8Path;
use dropshot::test_util::ClientTestContext;
use nexus_test_interface::NexusServer;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_physical_disk;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::delete_physical_disk;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::populate_ip_pool;
use nexus_test_utils::start_sled_agent;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::PhysicalDiskKind;
use nexus_types::external_api::views::SledInstance;
use nexus_types::external_api::views::{PhysicalDisk, Sled};
use omicron_sled_agent::sim;
use std::str::FromStr;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

async fn sleds_list(client: &ClientTestContext, sleds_url: &str) -> Vec<Sled> {
    objects_list_page_authz::<Sled>(client, sleds_url).await.items
}

async fn physical_disks_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<PhysicalDisk> {
    objects_list_page_authz::<PhysicalDisk>(client, url).await.items
}

async fn sled_instance_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<SledInstance> {
    objects_list_page_authz::<SledInstance>(client, url).await.items
}

#[nexus_test]
async fn test_sleds_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Verify that there is one sled to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&client, &sleds_url).await.len(), 1);

    // Now start a few more sled agents.
    let nsleds = 3;
    let mut sas = Vec::with_capacity(nsleds);
    for _ in 0..nsleds {
        let sa_id = Uuid::new_v4();
        let log =
            cptestctx.logctx.log.new(o!( "sled_id" => sa_id.to_string() ));
        let addr = cptestctx.server.get_http_server_internal_address().await;
        let update_directory = Utf8Path::new("/should/not/be/used");
        sas.push(
            start_sled_agent(
                log,
                addr,
                sa_id,
                &update_directory,
                sim::SimMode::Explicit,
            )
            .await
            .unwrap(),
        );
    }

    // List sleds again.
    let sleds_found = sleds_list(&client, &sleds_url).await;
    assert_eq!(sleds_found.len(), nsleds + 1);

    let sledids_found =
        sleds_found.iter().map(|sv| sv.identity.id).collect::<Vec<Uuid>>();
    let mut sledids_found_sorted = sledids_found.clone();
    sledids_found_sorted.sort();
    assert_eq!(sledids_found, sledids_found_sorted);

    // Tear down the agents.
    for sa in sas {
        sa.http_server.close().await.unwrap();
    }
}

#[nexus_test]
async fn test_physical_disk_create_list_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let external_client = &cptestctx.external_client;
    let internal_client = &cptestctx.internal_client;

    // Verify that there is one sled to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&external_client, &sleds_url).await.len(), 1);

    // Verify that there are no disks.
    let disks_url =
        format!("/v1/system/hardware/sleds/{SLED_AGENT_UUID}/disks");
    assert!(physical_disks_list(&external_client, &disks_url).await.is_empty());

    // Insert a new disk using the internal API, observe it in the external API
    let sled_id = Uuid::from_str(&SLED_AGENT_UUID).unwrap();
    create_physical_disk(
        &internal_client,
        "v",
        "s",
        "m",
        PhysicalDiskKind::U2,
        sled_id,
    )
    .await;
    let disks = physical_disks_list(&external_client, &disks_url).await;
    assert_eq!(disks.len(), 1);
    assert_eq!(disks[0].vendor, "v");
    assert_eq!(disks[0].serial, "s");
    assert_eq!(disks[0].model, "m");

    // Delete that disk using the internal API, observe it in the external API
    delete_physical_disk(&internal_client, "v", "s", "m", sled_id).await;
    assert!(physical_disks_list(&external_client, &disks_url).await.is_empty());
}

#[nexus_test]
async fn test_sled_instance_list(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;

    // Verify that there is one sled to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&external_client, &sleds_url).await.len(), 1);

    // Verify that there are no instances.
    let instances_url =
        format!("/v1/system/hardware/sleds/{SLED_AGENT_UUID}/instances");
    assert!(sled_instance_list(&external_client, &instances_url)
        .await
        .is_empty());

    // Create an IP pool and project that we'll use for testing.
    populate_ip_pool(&external_client, "default", None).await;
    let project = create_project(&external_client, "test-project").await;
    let instance =
        create_instance(&external_client, "test-project", "test-instance")
            .await;

    let sled_instances =
        sled_instance_list(&external_client, &instances_url).await;

    // Ensure 1 instance was created on the sled
    assert_eq!(sled_instances.len(), 1);

    assert_eq!(project.identity.name, sled_instances[0].project_name);
    assert_eq!(instance.identity.name, sled_instances[0].name);
}
