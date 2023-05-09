// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::{Method, StatusCode};
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::{load_test_config, test_setup_with_config};
use omicron_sled_agent::sim;

#[tokio::test]
async fn test_nexus_boots_before_cockroach() {
    let mut config = load_test_config();

    // stand up the test environment
    let cptestctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_nexus_boots_before_cockroach",
        &mut config,
        sim::SimMode::Explicit,
    )
    .await;
    let client = &cptestctx.external_client;

    // TODO: FIX THIS REQUEST
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, "/v1/system/update/refresh")
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    cptestctx.teardown().await;
}
