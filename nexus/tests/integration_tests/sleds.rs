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
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::{start_sled_agent, start_sled_agent_with_config};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::SledInstance;
use nexus_types::external_api::views::{PhysicalDisk, Sled, SledCpuFamily};
use omicron_sled_agent::sim;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use std::str::FromStr;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

pub(crate) async fn sleds_list(
    client: &ClientTestContext,
    sleds_url: &str,
) -> Vec<Sled> {
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

#[nexus_test(extra_sled_agents = 1)]
async fn test_sleds_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&client, &sleds_url).await.len(), 2);

    // Now start a few more sled agents.
    let mut sas = Vec::new();
    let nexus_address =
        cptestctx.server.get_http_server_internal_address().await;
    let update_directory = Utf8Path::new("/should/not/be/used");
    let simulated_upstairs = &cptestctx.first_sled_agent().simulated_upstairs;

    for _ in 0..4 {
        let sa_id = SledUuid::new_v4();
        let log =
            cptestctx.logctx.log.new(o!( "sled_id" => sa_id.to_string() ));
        sas.push(
            start_sled_agent(
                log,
                nexus_address,
                sa_id,
                // Index starts at 2: the `nexus_test` macro already created two
                // sled agents as part of the ControlPlaneTestContext setup.
                2 + sas.len() as u16 + 1,
                &update_directory,
                sim::SimMode::Explicit,
                &simulated_upstairs,
            )
            .await
            .unwrap(),
        );
    }

    let turin_sled_id = SledUuid::new_v4();
    let turin_sled_agent_log =
        cptestctx.logctx.log.new(o!( "sled_id" => turin_sled_id.to_string() ));

    let turin_config = omicron_sled_agent::sim::Config::for_testing(
        turin_sled_id,
        omicron_sled_agent::sim::SimMode::Explicit,
        Some(nexus_address),
        Some(&update_directory),
        omicron_sled_agent::sim::ZpoolConfig::None,
        nexus_client::types::SledCpuFamily::AmdTurin,
    );

    sas.push(
        start_sled_agent_with_config(
            turin_sled_agent_log,
            &turin_config,
            2 + sas.len() as u16 + 1,
            &simulated_upstairs,
        )
        .await
        .unwrap(),
    );

    // List sleds again.
    let sleds_found = sleds_list(&client, &sleds_url).await;
    assert_eq!(sleds_found.len(), sas.len() + 2);

    let sledids_found =
        sleds_found.iter().map(|sv| sv.identity.id).collect::<Vec<Uuid>>();
    let mut sledids_found_sorted = sledids_found.clone();
    sledids_found_sorted.sort();
    assert_eq!(sledids_found, sledids_found_sorted);

    let milans_found = sleds_found
        .iter()
        .filter(|sv| sv.cpu_family == SledCpuFamily::AmdMilan)
        .count();
    // Simulated sled-agents report Milan processors by default. The two fake
    // sled-agents created by `#[nexus_test]` as well as the four manually
    // created above should be counted here.
    assert_eq!(milans_found, 2 + 4);

    let turins_found = sleds_found
        .iter()
        .filter(|sv| sv.cpu_family == SledCpuFamily::AmdTurin)
        .count();
    assert_eq!(turins_found, 1);

    // Tear down the agents.
    for sa in sas {
        sa.http_server.close().await.unwrap();
    }
}

#[nexus_test(extra_sled_agents = 1)]
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
        PhysicalDiskUuid::new_v4(),
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
    let new_disk = disks
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
            PhysicalDiskUuid::from_untyped_uuid(new_disk.identity.id),
        )
        .await
        .expect("Failed to upsert physical disk");

    let list = physical_disks_list(&external_client, &disks_url).await;
    assert_eq!(list, disks_initial, "{:#?}", list,);
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_sled_instance_list(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    let sleds = sleds_list(&external_client, &sleds_url).await;
    assert_eq!(sleds.len(), 2);

    // Verify that there are no instances on the sleds.
    for sled in &sleds {
        let sled_id = sled.identity.id;
        let instances_url =
            format!("/v1/system/hardware/sleds/{sled_id}/instances");
        assert!(
            sled_instance_list(&external_client, &instances_url)
                .await
                .is_empty()
        );
    }

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pool(&external_client).await;
    let project = create_project(&external_client, "test-project").await;
    let instance =
        create_instance(&external_client, "test-project", "test-instance")
            .await;

    // Ensure 1 instance was created on a sled
    let sled_instances = wait_for_condition(
        || {
            let sleds = sleds.clone();

            async move {
                let mut total_instances = vec![];

                for sled in &sleds {
                    let sled_id = sled.identity.id;

                    let instances_url = format!(
                        "/v1/system/hardware/sleds/{sled_id}/instances"
                    );

                    let mut sled_instances =
                        sled_instance_list(&external_client, &instances_url)
                            .await;

                    total_instances.append(&mut sled_instances);
                }

                if total_instances.len() == 1 {
                    Ok(total_instances)
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            }
        },
        &std::time::Duration::from_millis(500),
        &std::time::Duration::from_secs(60),
    )
    .await
    .expect("one sled instance");

    assert_eq!(project.identity.name, sled_instances[0].project_name);
    assert_eq!(instance.identity.name, sled_instances[0].name);
}
