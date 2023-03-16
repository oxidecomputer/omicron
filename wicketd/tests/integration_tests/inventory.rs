// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test out update functionality.

use std::time::Duration;

use super::setup::WicketdTestContext;
use gateway_messages::SpPort;
use gateway_test_utils::setup as gateway_setup;
use wicketd_client::GetInventoryResponse;

#[tokio::test(start_paused = true)]
async fn test_inventory() {
    let gateway =
        gateway_setup::test_setup("test_wicket_updates", SpPort::One).await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;

    let inventory_fut = async {
        loop {
            let response = wicketd_testctx
                .wicketd_client
                .get_inventory()
                .await
                .expect("get_inventory failed")
                .into_inner();
            match response.into() {
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
            .expect("get_inventory timed out");

    // 4 SPs attached to the inventory.
    assert_eq!(inventory.sps.len(), 4);

    wicketd_testctx.teardown().await;
}
