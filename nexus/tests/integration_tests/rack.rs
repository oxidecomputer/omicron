// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::external_api::views::Rack;
use omicron_nexus::TestInterfaces;

#[nexus_test]
async fn test_list_own_rack(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let racks_url = "/system/hardware/racks";
    let racks: Vec<Rack> =
        NexusRequest::iter_collection_authn(client, racks_url, "", None)
            .await
            .expect("failed to list racks")
            .all_items;

    assert_eq!(1, racks.len());
    assert_eq!(cptestctx.server.apictx.nexus.rack_id(), racks[0].identity.id);
}

#[nexus_test]
async fn test_get_own_rack(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let expected_id = cptestctx.server.apictx.nexus.rack_id();
    let rack_url = format!("/system/hardware/racks/{}", expected_id);
    let rack = NexusRequest::object_get(client, &rack_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to get rack")
        .parsed_body::<Rack>()
        .unwrap();

    assert_eq!(expected_id, rack.identity.id);
}
