// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test out update functionality.

use std::time::Duration;

use super::setup::WicketdTestContext;
use gateway_messages::SpPort;
use gateway_test_utils::setup as gateway_setup;
use http::StatusCode;
use sled_hardware_types::Baseboard;
use slog::{info, warn};
use wicket::OutputKind;
use wicket_common::inventory::{SpIdentifier, SpType};
use wicket_common::rack_setup::BootstrapSledDescription;
use wicketd_client::Error;
use wicketd_client::types::{GetInventoryParams, GetInventoryResponse};

#[tokio::test]
async fn test_inventory() {
    let gateway =
        gateway_setup::test_setup("test_inventory", SpPort::One, None).await;
    let wicketd_testctx = WicketdTestContext::setup(gateway).await;
    let params = GetInventoryParams { force_refresh: Vec::new() };

    let inventory_fut = async {
        loop {
            let response = wicketd_testctx
                .wicketd_client
                .get_inventory(&params)
                .await
                .map(|r| r.into_inner());
            match response {
                Ok(GetInventoryResponse::Response { inventory, .. }) => {
                    // Ensure that the SP state is populated -- if it's not,
                    // then the `configured-bootstrap-sleds` command below
                    // might return an empty list.
                    if let Some(mgs) = inventory.mgs {
                        let sp_state_none: Vec<_> = mgs
                            .inventory
                            .sps
                            .iter()
                            .filter(|sp| sp.state.is_none())
                            .collect();
                        if sp_state_none.is_empty() {
                            break mgs.inventory;
                        }
                        warn!(
                            wicketd_testctx.log(),
                            "SP state not yet populated for some SPs, retrying";
                            "sps" => ?sp_state_none
                        )
                    } else {
                        warn!(
                            wicketd_testctx.log(),
                            "MGS-derived inventory not yet populated, retrying"
                        );
                    }
                }
                // Successful response, but the MGS inventory isn't available.
                Ok(GetInventoryResponse::Unavailable) => {}

                // 503 means neither MGS nor transceiver inventory is available.
                Err(Error::ErrorResponse(rv))
                    if rv.status() == StatusCode::SERVICE_UNAVAILABLE => {}

                // Anything else is unexpected.
                Err(e) => panic!(
                    "get_inventory failed with unexpected response: {e:?}"
                ),
            }

            // Keep polling wicketd until it receives its first results from MGS.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let inventory =
        tokio::time::timeout(Duration::from_secs(10), inventory_fut)
            .await
            .expect("get_inventory completed within 10 seconds");

    info!(wicketd_testctx.log(), "inventory returned"; "inventory" => ?inventory);

    // 4 SPs attached to the inventory.
    assert_eq!(inventory.sps.len(), 4);

    // Test CLI with JSON output
    {
        let args =
            vec!["inventory", "configured-bootstrap-sleds", "--format", "json"];
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
        let response: Vec<BootstrapSledDescription> =
            serde_json::from_slice(&stdout).expect("stdout is valid JSON");

        // This only tests the case that we get sleds back with no current
        // bootstrap IP. This does provide some value: it checks that the
        // command exists, accesses data within wicket, and returns it in the
        // schema we expect. But it does not test the case where a sled does
        // have a bootstrap IP.
        //
        // Unfortunately, that's a difficult thing to test today. Wicket gets
        // that information by enumerating the IPs on the bootstrap network and
        // reaching out to the bootstrap_agent on them directly to ask them who
        // they are. Our testing setup does not have a way to provide such an
        // IP, or run a bootstrap_agent on an IP to respond. We should update
        // this test when we do have that capability.
        assert_eq!(
            response,
            vec![
                BootstrapSledDescription {
                    id: SpIdentifier { type_: SpType::Sled, slot: 0 },
                    baseboard: Baseboard::Gimlet {
                        identifier: "SimGimlet00".to_string(),
                        model: "i86pc".to_string(),
                        revision: 0
                    },
                    bootstrap_ip: None
                },
                BootstrapSledDescription {
                    id: SpIdentifier { type_: SpType::Sled, slot: 1 },
                    baseboard: Baseboard::Gimlet {
                        identifier: "SimGimlet01".to_string(),
                        model: "i86pc".to_string(),
                        revision: 0
                    },
                    bootstrap_ip: None
                },
            ]
        );
    }

    wicketd_testctx.teardown().await;
}
