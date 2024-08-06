// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test out update functionality.

use std::time::Duration;

use super::setup::WicketdTestContext;
use gateway_messages::SpPort;
use gateway_test_utils::setup as gateway_setup;
use serde::Deserialize;
use std::net::Ipv6Addr;
use wicket::OutputKind;
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

    // Test CLI command
    {
        let args = vec!["inventory", "configured-bootstrap-sleds", "--json"];
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let output = OutputKind::Captured {
            log: wicketd_testctx.log().clone(),
            stdout: &mut stdout,
            stderr: &mut stderr,
        };

        wicket::exec_with_args(wicketd_testctx.wicketd_addr, args, output)
            .await
            .expect("wicket inventory configured-bootstrap-sleds failed");

        // stdout should contain a JSON object.
        let response: Vec<ConfiguredBootstrapSledData> =
            serde_json::from_slice(&stdout).expect("stdout is valid JSON");

        // This is the data we have today but I want it to be different from
        // this to test having an address and such

        // I think this data comes from mgs?
        assert_eq!(
            response,
            vec![
                ConfiguredBootstrapSledData {
                    slot: 0,
                    identifier: "SimGimlet00".to_string(),
                    address: None,
                },
                ConfiguredBootstrapSledData {
                    slot: 1,
                    identifier: "SimGimlet01".to_string(),
                    address: None,
                },
            ]
        );
    }

    wicketd_testctx.teardown().await;
}

// XXX this is code dupe from cli (but where I have it there now is a private
// module.) We are using it here to verify the json output of the cli. so... do
// we want the dupe? or... where would it live?
#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ConfiguredBootstrapSledData {
    slot: u32,
    identifier: String,
    address: Option<Ipv6Addr>,
}
