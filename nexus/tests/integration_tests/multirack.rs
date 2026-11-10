// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for multirack regional cluster APIs.

use super::endpoints::MULTIRACK_RACKS_UNADOPTED_URL;
use iddqd::IdOrdMap;
use nexus_db_queries::context::OpContext;
use nexus_inventory::CollectionBuilder;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::multirack::UnadoptedRack;
use nexus_types::inventory::DiscoveredRack;
use nexus_types::inventory::DiscoveredSled;
use omicron_common::address::Ipv6Subnet;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types_versions::latest::multirack::SledAgentStartState;
use sled_hardware_types::BaseboardId;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const REGION_PREFIX: &str = "fd00:aaaa:bbbb:10";

/// Construct `count` discovered sleds for the rack identified by `rack_idx`.
///
/// Every field is distinct to ensure we catch differences in the event of
/// something getting round-tripped incorrectly.
fn make_discovered_sleds(rack_idx: u8, count: u8) -> IdOrdMap<DiscoveredSled> {
    (0..count)
        .map(|sled_idx| DiscoveredSled {
            sled_id: SledUuid::new_v4(),
            baseboard_id: BaseboardId {
                part_number: format!("test-part-{rack_idx}-{sled_idx}"),
                serial_number: format!("test-serial-{rack_idx}-{sled_idx}"),
            },
            sled_subnet: Ipv6Subnet::new(
                format!("{REGION_PREFIX}{rack_idx}:{sled_idx}::")
                    .parse()
                    .unwrap(),
            ),
            sled_agent_start_state: match sled_idx % 3 {
                0 => SledAgentStartState::Started,
                1 => SledAgentStartState::InProgress { last_error: None },
                2 => SledAgentStartState::InProgress {
                    last_error: Some(format!(
                        "test error from sled {rack_idx}-{sled_idx}"
                    )),
                },
                _ => unreachable!(),
            },
        })
        .collect()
}

async fn list_unadopted_racks(
    cptestctx: &ControlPlaneTestContext,
    page_size: usize,
) -> Vec<UnadoptedRack> {
    // `iter_collection_authn` also checks that no page contains more items
    // than `page_size`
    NexusRequest::iter_collection_authn(
        &cptestctx.external_client,
        MULTIRACK_RACKS_UNADOPTED_URL,
        "",
        Some(page_size),
    )
    .await
    .expect("listing unadopted racks should succeed")
    .all_items
}

#[nexus_test]
async fn test_rack_list_unadopted_empty(cptestctx: &ControlPlaneTestContext) {
    // By default, no racks should be showing up in inventory, and even if our
    // own rack somehow showed up as a discovered rack it should have a row in
    // `rack` and count as adopted, so either way nothing should show up in the
    // API output by default.
    cptestctx
        .wait_for_at_least_one_inventory_collection(Duration::from_secs(60))
        .await;

    let racks = list_unadopted_racks(cptestctx, 10).await;
    assert!(racks.is_empty(), "unexpected unadopted racks: {racks:#?}");
}

#[tokio::test]
async fn test_rack_list_unadopted() {
    let cptestctx =
        nexus_test_utils::ControlPlaneBuilder::new("test_rack_list_unadopted")
            .customize_nexus_config(&|config| {
                // Disable the inventory collection background task to avoid a
                // race.
                config.pkg.background_tasks.inventory.disable_collect = true;
            })
            .start::<omicron_nexus::Server>()
            .await;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    // Build an inventory collection that has discovered four racks. One of
    // them is our own rack, which is already adopted (it has a row in the
    // `rack` table), so it must not be reported as unadopted. (This is a
    // slightly weird test case because normally in a single-rack cluster we
    // wouldn't see our own rack in the discovered racks in inventory, but it's
    // easier to test the filtering mechanism for adopted racks this way and
    // adding other racks into `rack` might cause other weird side effects in
    // the test since they wouldn't have actually been joined into the cluster.)
    let adopted_rack_id = nexus.rack_id();
    let make_rack =
        |rack_id: RackUuid, rack_idx: u8, nsleds: u8| DiscoveredRack {
            rack_id,
            rack_subnet: Ipv6Subnet::new(
                format!("{REGION_PREFIX}{rack_idx}::").parse().unwrap(),
            ),
            join_service_ip: format!("{REGION_PREFIX}{rack_idx}::1")
                .parse()
                .unwrap(),
            discovered_sleds: make_discovered_sleds(rack_idx, nsleds),
        };
    // Give the racks differing numbers of sleds:
    let mut expected: Vec<DiscoveredRack> = [3, 1, 0]
        .into_iter()
        .enumerate()
        .map(|(i, nsleds)| {
            make_rack(RackUuid::new_v4(), u8::try_from(i).unwrap(), nsleds)
        })
        .collect();
    // The ordering of the API results is by rack ID, so sort this accordingly:
    expected.sort_by_key(|rack| rack.rack_id);

    let mut builder = CollectionBuilder::new("multirack-test");
    builder.found_discovered_rack(make_rack(adopted_rack_id, 10, 2)).unwrap();
    for rack in &expected {
        builder.found_discovered_rack(rack.clone()).unwrap();
    }
    datastore
        .inventory_insert_collection(&opctx, &builder.build())
        .await
        .expect("failed to insert inventory collection");
    nexus_test_utils::background::run_inventory_loader(
        &cptestctx.lockstep_client,
    )
    .await;

    // Regardless of page size, we should see exactly the unadopted racks, in
    // order of rack ID:
    for page_size in [1, 2, 3, 10] {
        let racks = list_unadopted_racks(&cptestctx, page_size).await;
        assert_eq!(
            racks.len(),
            expected.len(),
            "unexpected number of racks with page size {page_size}: \
             {racks:#?}"
        );
        for (actual, expected) in racks.iter().zip(&expected) {
            assert_eq!(
                actual.rack_id, expected.rack_id,
                "racks out of order or unexpected with page size {page_size}"
            );
            assert_eq!(actual.rack_subnet, expected.rack_subnet.net());

            assert_eq!(
                actual.unadopted_sleds.len(),
                expected.discovered_sleds.len(),
                "unexpected number of sleds for rack {}",
                actual.rack_id
            );
            for (actual_sled, expected_sled) in
                actual.unadopted_sleds.iter().zip(&expected.discovered_sleds)
            {
                assert_eq!(actual_sled.sled_id, expected_sled.sled_id);
                assert_eq!(
                    actual_sled.baseboard_id,
                    expected_sled.baseboard_id
                );
                match &expected_sled.sled_agent_start_state {
                    SledAgentStartState::Started => {
                        assert!(actual_sled.sled_agent_started);
                        assert_eq!(actual_sled.last_start_err, None);
                    }
                    SledAgentStartState::InProgress { last_error } => {
                        assert!(!actual_sled.sled_agent_started);
                        assert_eq!(&actual_sled.last_start_err, last_error);
                    }
                }
            }
        }
        assert!(
            racks.iter().all(|r| r.rack_id != adopted_rack_id),
            "adopted rack should not be listed: {racks:#?}"
        );
    }

    cptestctx.teardown().await;
}
