// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test host phase 1 hash flashing via MGS.
//!
//! This operation is implemented asynchronously on the SP side: we must first
//! ask it to start hashing, then we'll get "still hashing" errors for a few
//! seconds, then we'll get the hash result.

use gateway_client::SpComponent;
use gateway_client::types::ComponentFirmwareHashStatus;
use gateway_client::types::SpType;
use gateway_messages::SpPort;
use gateway_test_utils::setup as mgs_setup;

#[tokio::test]
async fn test_host_phase1_hashing() {
    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_host_phase1_updater_updates_sled",
        SpPort::One,
    )
    .await;

    // We'll only talk to one sp-sim for this test.
    let mgs_client = mgstestctx.client();
    let sp_type = SpType::Sled;
    let sp_component = SpComponent::HOST_CPU_BOOT_FLASH.const_as_str();
    let sp_slot = 0;

    // We haven't yet started hashing; we should get the error we expect for
    // both slots.
    for firmware_slot in [0, 1] {
        let status = mgs_client
            .sp_component_hash_firmware_get(
                sp_type,
                sp_slot,
                sp_component,
                firmware_slot,
            )
            .await
            .expect("got firmware hash status");
        match status.into_inner() {
            ComponentFirmwareHashStatus::HashNotCalculated => (),
            other => panic!("unexpected status: {other:?}"),
        }
    }
}
