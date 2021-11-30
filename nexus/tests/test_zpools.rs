// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::ByteCount;
use omicron_nexus::internal_api::params::ZpoolPutRequest;
use uuid::Uuid;

pub mod common;
use common::{test_setup, SLED_AGENT_UUID};

extern crate slog;

// Tests the "normal" case of zpool_put: inserting a known Zpool.
//
// This will typically be invoked by the Sled Agent, after performing inventory.
#[tokio::test]
async fn test_zpool_put_success() {
    let cptestctx = test_setup("test_zpool_put_success").await;
    let client = &cptestctx.internal_client;

    let zpool_id = Uuid::new_v4();
    let zpool_put_url =
        format!("/sled_agents/{}/zpools/{}", SLED_AGENT_UUID, zpool_id);

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

    cptestctx.teardown().await;
}

// Tests a failure case of zpool_put: Inserting a zpool into a sled agent that
// does not exist.
#[tokio::test]
async fn test_zpool_put_bad_sled_returns_not_found() {
    let cptestctx =
        test_setup("test_zpool_put_bad_sled_returns_not_found").await;
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
    cptestctx.teardown().await;
}
