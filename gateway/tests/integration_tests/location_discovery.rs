// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use super::setup;
use dropshot::test_util;
use gateway_messages::SpPort;
use omicron_gateway::http_entrypoints::SpInfo;
use omicron_gateway::http_entrypoints::SpState;

#[tokio::test]
async fn discovery_both_locations() {
    let testctx0 =
        setup::test_setup("discovery_both_locations_0", SpPort::One).await;
    let testctx1 =
        setup::test_setup("discovery_both_locations_1", SpPort::Two).await;

    let client0 = &testctx0.client;
    let client1 = &testctx1.client;

    // the two instances should've discovered that they were switch0 and
    // switch1, respectively
    assert_eq!(
        testctx0.server.apictx.mgmt_switch.location_name().unwrap(),
        "switch0"
    );
    assert_eq!(
        testctx1.server.apictx.mgmt_switch.location_name().unwrap(),
        "switch1"
    );

    // both instances should report the same serial number for switch 0 and
    // switch 1, and it should match the expected values from the config
    for (switch, expected_serial) in [
        (0, "00000000000000000000000000000001"),
        (1, "00000000000000000000000000000002"),
    ] {
        for client in [client0, client1] {
            let url =
                format!("{}", client0.url(&format!("/sp/switch/{}", switch)));

            let resp: SpInfo = test_util::object_get(client, &url).await;
            match resp.details {
                SpState::Enabled { serial_number } => {
                    assert_eq!(serial_number, expected_serial)
                }
                other => panic!("unexpected state {:?}", other),
            }
        }
    }

    testctx0.teardown().await;
    testctx1.teardown().await;
}
