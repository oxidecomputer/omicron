// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2025 Oxide Computer Company

use base64::prelude::*;
use dropshot::test_util;
use gateway_messages::SpPort;
use gateway_test_utils::current_simulator_state;
use gateway_test_utils::setup;
use gateway_types::component::SpType;
use gateway_types::task_dump::TaskDump;
use sp_sim::SIM_GIMLET_BOARD;

#[tokio::test]
async fn task_dump() {
    let testctx = setup::test_setup("task_dump", SpPort::One).await;
    let client = &testctx.client;
    let simrack = &testctx.simrack;

    // sanity check: we have at least 1 gimlet, and all SPs are enabled
    let sim_state = current_simulator_state(simrack).await;
    assert!(sim_state.iter().any(|sp| sp.ignition.id.typ == SpType::Sled));
    assert!(sim_state.iter().all(|sp| sp.state.is_ok()));

    // Get task dump count for sled 0.
    let url = format!("{}", client.url("/sp/sled/0/task-dump"));
    let resp: u32 = test_util::object_get(client, &url).await;

    assert_eq!(resp, 1);

    // Get the task dump.
    let url = format!("{}", client.url("/sp/sled/0/task-dump/0"));
    let TaskDump {
        task_index,
        timestamp,
        archive_id,
        bord,
        gitc,
        vers,
        base64_memory,
    } = test_util::object_get(client, &url).await;

    assert_eq!(0, task_index);
    assert_eq!(1, timestamp);
    assert_eq!(archive_id, "0000000000000000".to_string());
    assert_eq!(bord, SIM_GIMLET_BOARD.to_string());
    assert_eq!(gitc, "ffffffff".to_string());
    assert_eq!(vers, Some("0.0.2".to_string()));

    let encoded_mem = &base64_memory[&1];
    assert_eq!(
        "my cool SP dump".repeat(3).as_bytes(),
        BASE64_STANDARD.decode(encoded_mem).unwrap()
    );

    testctx.teardown().await;
}
