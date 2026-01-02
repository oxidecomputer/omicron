// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use gateway_messages::SpPort;
use gateway_test_utils::setup;
use gateway_types::component::SpType;
use omicron_gateway::SpIdentifier;

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
        testctx0.server.management_switch().local_switch().unwrap(),
        SpIdentifier { typ: SpType::Switch.into(), slot: 0 },
    );
    assert_eq!(
        testctx1.server.management_switch().local_switch().unwrap(),
        SpIdentifier { typ: SpType::Switch.into(), slot: 1 },
    );

    // both instances should report the same serial number for switch 0 and
    // switch 1, and it should match the expected values from the config
    for (switch, expected_serial) in [(0, "SimSidecar0"), (1, "SimSidecar1")] {
        for client in [client0, client1] {
            let state = client
                .sp_get(&SpType::Switch, switch)
                .await
                .unwrap()
                .into_inner();
            assert_eq!(state.serial_number, expected_serial);
        }
    }

    testctx0.teardown().await;
    testctx1.teardown().await;
}
