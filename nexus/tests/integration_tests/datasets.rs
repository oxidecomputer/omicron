// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::ByteCount;
use omicron_nexus::internal_api::params::{
    DatasetKind, DatasetPutRequest, ZpoolPutRequest,
};
use std::net::{Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

use nexus_test_utils::{ControlPlaneTestContext, SLED_AGENT_UUID};
use nexus_test_utils_macros::nexus_test;

// Tests the "normal" case of dataset_put: inserting a dataset within a known
// zpool.
//
// This will typically be invoked by the Sled Agent, after performing inventory.
#[nexus_test]
async fn test_dataset_put_success(cptestctx: &ControlPlaneTestContext) {
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

    let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);
    let kind = DatasetKind::Crucible;
    let request = DatasetPutRequest { address, kind };
    let dataset_id = Uuid::new_v4();
    let dataset_put_url =
        format!("/zpools/{}/dataset/{}", zpool_id, dataset_id);

    client
        .make_request(
            Method::PUT,
            &dataset_put_url,
            Some(request),
            StatusCode::OK,
        )
        .await
        .unwrap();
}

// Tests a failure case of dataset_put: Inserting a dataset into a zpool that
// does not exist.
#[nexus_test]
async fn test_dataset_put_bad_zpool_returns_not_found(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.internal_client;

    // A zpool with the "nil" UUID should not exist.
    let zpool_id = Uuid::nil();
    let dataset_id = Uuid::new_v4();
    let dataset_put_url =
        format!("/zpools/{}/dataset/{}", zpool_id, dataset_id);

    let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);
    let kind = DatasetKind::Crucible;
    let request = DatasetPutRequest { address, kind };

    client
        .make_request_error_body(
            Method::PUT,
            &dataset_put_url,
            request,
            StatusCode::NOT_FOUND,
        )
        .await;
}
