// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for APIs against sled-based endpoints.

use camino::Utf8Path;
use dropshot::test_util::ClientTestContext;
use nexus_db_model::PhysicalDisk as DbPhysicalDisk;
use nexus_db_model::PhysicalDiskKind as DbPhysicalDiskKind;
use nexus_db_queries::context::OpContext;
use nexus_test_interface::NexusServer;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::start_sled_agent;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::SledInstance;
use nexus_types::external_api::views::{PhysicalDisk, Sled};
use omicron_sled_agent::sim;
use omicron_uuid_kinds::SledUuid;
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

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&client, &sleds_url).await.len(), 2);

    // Now start a few more sled agents.
    let nsleds = 3;
    let mut sas = Vec::with_capacity(nsleds);
    for _ in 0..nsleds {
        let sa_id = SledUuid::new_v4();
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
    assert_eq!(sleds_found.len(), nsleds + 2);

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

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&external_client, &sleds_url).await.len(), 2);

    // The test framework may set up some disks initially.
    let disks_url =
        format!("/v1/system/hardware/sleds/{SLED_AGENT_UUID}/disks");
    let disks_initial = physical_disks_list(&external_client, &disks_url).await;

    // Inject a disk into the database, observe it in the external API
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let sled_id = Uuid::from_str(&SLED_AGENT_UUID).unwrap();
    let physical_disk = DbPhysicalDisk::new(
        Uuid::new_v4(),
        "v".into(),
        "s".into(),
        "m".into(),
        DbPhysicalDiskKind::U2,
        sled_id,
    );

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let _disk_id = datastore
        .physical_disk_insert(&opctx, physical_disk.clone())
        .await
        .expect("Failed to upsert physical disk");

    let disks = physical_disks_list(&external_client, &disks_url).await;
    assert_eq!(disks.len(), disks_initial.len() + 1);
    let _new_disk = disks
        .iter()
        .find(|found_disk| {
            found_disk.vendor == "v"
                && found_disk.serial == "s"
                && found_disk.model == "m"
        })
        .expect("did not find the new disk");

    // Delete that disk using the internal API, observe it in the external API
    datastore
        .physical_disk_delete(
            &opctx,
            "v".into(),
            "s".into(),
            "m".into(),
            sled_id,
        )
        .await
        .expect("Failed to upsert physical disk");

    let list = physical_disks_list(&external_client, &disks_url).await;
    assert_eq!(list, disks_initial, "{:#?}", list,);
}

#[nexus_test]
async fn test_sled_instance_list(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&external_client, &sleds_url).await.len(), 2);

    // Verify that there are no instances.
    let instances_url =
        format!("/v1/system/hardware/sleds/{SLED_AGENT_UUID}/instances");
    assert!(sled_instance_list(&external_client, &instances_url)
        .await
        .is_empty());

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pool(&external_client).await;
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
