// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for multirack regional cluster APIs.

use super::endpoints::MULTIRACK_RACKS_UNADOPTED_URL;
use iddqd::id_ord_map;
use nexus_db_queries::context::OpContext;
use nexus_inventory::CollectionBuilder;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::multirack::UnadoptedRack;
use nexus_types::inventory::DiscoveredRack;
use omicron_common::address::Ipv6Subnet;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const REGION_PREFIX: &str = "fd00:aaaa:bbbb:10";

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

#[nexus_test]
async fn test_rack_list_unadopted(cptestctx: &ControlPlaneTestContext) {
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
    let make_rack = |rack_id: RackUuid, rack_idx: u8| DiscoveredRack {
        rack_id,
        rack_subnet: Ipv6Subnet::new(
            format!("{REGION_PREFIX}{rack_idx}::").parse().unwrap(),
        ),
        join_service_ip: format!("{REGION_PREFIX}{rack_idx}::1")
            .parse()
            .unwrap(),
        discovered_sleds: id_ord_map! {},
    };
    let mut expected: Vec<DiscoveredRack> =
        (0..3).map(|i| make_rack(RackUuid::new_v4(), i)).collect();
    // The ordering of the API results is by rack ID, so sort this accordingly:
    expected.sort_by_key(|rack| rack.rack_id);

    let mut builder = CollectionBuilder::new("multirack-test");
    builder.found_discovered_rack(make_rack(adopted_rack_id, 10)).unwrap();
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
        let racks = list_unadopted_racks(cptestctx, page_size).await;
        assert_eq!(
            racks.len(),
            expected.len(),
            "unexpected number of racks with page size {page_size}: \
             {racks:#?}"
        );
        for (actual, expected) in racks.iter().zip(&expected) {
            assert_eq!(
                actual.rack_id,
                expected.rack_id.into_untyped_uuid(),
                "racks out of order or unexpected with page size {page_size}"
            );
            assert_eq!(actual.rack_subnet, expected.rack_subnet.net());
            assert_eq!(actual.join_service_ip, expected.join_service_ip);
            assert!(actual.unadopted_sleds.is_empty());
        }
        assert!(
            racks
                .iter()
                .all(|r| r.rack_id != adopted_rack_id.into_untyped_uuid()),
            "adopted rack should not be listed: {racks:#?}"
        );
    }
}
