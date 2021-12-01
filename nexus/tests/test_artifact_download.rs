// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;

pub mod common;
use common::test_setup;

extern crate slog;

// Tests the "normal" case of downloading an artifact.
//
// This will typically be invoked by the Sled Agent, after instructed
// to access an artifact.
#[tokio::test]
async fn test_download_known_artifact_returns_ok() {
    let cptestctx = test_setup("test_download_known_artifact_returns_ok").await;
    let client = &cptestctx.internal_client;

    // TODO: Can we replace this with a "real" small file that must be
    // downloaded, instead of synthetically created?
    let filename = "testfile";
    let artifact_get_url = format!("/artifacts/{}", filename);

    let response = client
        .make_request_no_body(Method::GET, &artifact_get_url, StatusCode::OK)
        .await
        .unwrap();

    assert_eq!(
        hyper::body::to_bytes(response.into_body()).await.unwrap(),
        "testfile contents"
    );
    cptestctx.teardown().await;
}

// Tests that missing artifacts return "NOT_FOUND".
#[tokio::test]
async fn test_download_bad_artifact_not_found() {
    let cptestctx = test_setup("test_download_bad_artifact_not_found").await;
    let client = &cptestctx.internal_client;

    let filename = "not_a_real_artifact";
    let artifact_get_url = format!("/artifacts/{}", filename);

    client
        .make_request_error(
            Method::GET,
            &artifact_get_url,
            StatusCode::NOT_FOUND,
        )
        .await;

    cptestctx.teardown().await;
}

// Tests that ".." paths are disallowed by dropshot.
#[tokio::test]
async fn test_download_with_dots_fails() {
    let cptestctx = test_setup("test_download_with_dots_fails").await;
    let client = &cptestctx.internal_client;

    let filename = "hey/can/you/look/../../../../up/the/directory/tree";
    let artifact_get_url = format!("/artifacts/{}", filename);

    client
        .make_request_error(
            Method::GET,
            &artifact_get_url,
            StatusCode::BAD_REQUEST,
        )
        .await;

    cptestctx.teardown().await;
}
