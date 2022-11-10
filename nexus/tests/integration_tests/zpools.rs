// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::ByteCount;
use omicron_nexus::internal_api::params::ZpoolPutRequest;
use uuid::Uuid;

use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils_macros::nexus_test;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Tests the "normal" case of zpool_put: inserting a known Zpool.
//
// This will typically be invoked by the Sled Agent, after performing inventory.
#[nexus_test]
async fn test_zpool_put_success(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.internal_client;

    let zpool_id = Uuid::new_v4();
    let zpool_put_url =
        format!("/sled-agents/{}/zpools/{}", SLED_AGENT_UUID, zpool_id);

    let request = ZpoolPutRequest { size: ByteCount::from_gibibytes_u32(1) };
    client
        .make_request(
            Method::PUT,
            &zpool_put_url,
            Some(request),
            StatusCode::OK,
        )
        .await
        .unwrap();
}

// Tests a failure case of zpool_put: Inserting a zpool into a sled agent that
// does not exist.
#[nexus_test]
async fn test_zpool_put_bad_sled_returns_not_found(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.internal_client;

    // A sled with the "nil" UUID should not exist.
    let sled_id = Uuid::nil();
    let zpool_id = Uuid::new_v4();
    let zpool_put_url = format!("/sled_agents/{}/zpools/{}", sled_id, zpool_id);

    let request = ZpoolPutRequest { size: ByteCount::from_gibibytes_u32(1) };
    client
        .make_request_error_body(
            Method::PUT,
            &zpool_put_url,
            request,
            StatusCode::NOT_FOUND,
        )
        .await;
}
