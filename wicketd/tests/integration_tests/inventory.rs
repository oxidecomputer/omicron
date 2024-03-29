// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test out update functionality.

use std::time::Duration;

use super::setup::WicketdTestContext;
use gateway_messages::SpPort;
use gateway_test_utils::setup as gateway_setup;
use wicketd_client::types::{GetInventoryParams, GetInventoryResponse};

#[tokio::test]
async fn test_inventory() {
    let gateway =
        gateway_setup::test_setup("test_inventory", SpPort::One).await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;
    let params = GetInventoryParams { force_refresh: Vec::new() };

    let inventory_fut = async {
        loop {
            let response = wicketd_testctx
                .wicketd_client
                .get_inventory(&params)
                .await
                .expect("get_inventory succeeded")
                .into_inner();
            match response {
                GetInventoryResponse::Response { inventory, .. } => {
                    break inventory
                }
                GetInventoryResponse::Unavailable => {
                    // Keep polling wicketd until it receives its first results from MGS.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    };
    let inventory =
        tokio::time::timeout(Duration::from_secs(10), inventory_fut)
            .await
            .expect("get_inventory completed within 10 seconds");

    // 4 SPs attached to the inventory.
    assert_eq!(inventory.sps.len(), 4);

    wicketd_testctx.teardown().await;
}
