// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests BFD support in the API

use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::bfd::BfdStatus;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const STATUS_URL: &str = "/v1/system/networking/bfd-status";

#[nexus_test]
async fn test_empty_bfd_status(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let status = NexusRequest::object_get(client, STATUS_URL)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<Vec<BfdStatus>>()
        .await;

    // `#[nexus_test]` doesn't set up BFD, so we should have no status. But we
    // should still be able to ask for that! (#[nexus_test] also only sets up
    // one fake scrimlet - that used to cause this endpoint to fail.)
    assert_eq!(status, Vec::new());
}
