// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test host phase 1 hash flashing via MGS.
//!
//! This operation is implemented asynchronously on the SP side: we must first
//! ask it to start hashing, then we'll get "still hashing" errors for a few
//! seconds, then we'll get the hash result.

use gateway_client::Client;
use gateway_client::SpComponent;
use gateway_client::types::ComponentFirmwareHashStatus;
use gateway_client::types::SpUpdateStatus;
use gateway_messages::SpPort;
use gateway_test_utils::setup as mgs_setup;
use nexus_types::inventory::SpType;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use sha2::Digest as _;
use sha2::Sha256;
use sp_sim::HostFlashHashPolicy;
use sp_sim::SimulatedSp;
use std::time::Duration;
use uuid::Uuid;

struct Phase1HashStatusChecker<'a> {
    mgs_client: &'a Client,
    sp_type: SpType,
    sp_slot: u16,
    sp_component: &'a str,
}

impl Phase1HashStatusChecker<'_> {
    async fn assert_status(
        &self,
        expected: &[(u16, ComponentFirmwareHashStatus)],
    ) {
        for (firmware_slot, expected_status) in expected {
            let status = self
                .mgs_client
                .sp_component_hash_firmware_get(
                    &self.sp_type,
                    self.sp_slot,
                    self.sp_component,
                    *firmware_slot,
                )
                .await
                .expect("got firmware hash status");
            assert_eq!(
                status.into_inner(),
                *expected_status,
                "unexpected status for slot {firmware_slot}"
            );
        }
    }
}

// This is primarily a test of the `sp-sim` implementation of host phase 1
// flashing, with a minor side test that MGS's endpoints wrap it faithfully.
#[tokio::test]
async fn test_host_phase1_hashing() {
    // Start MGS + Sim SP.
    let mgstestctx = mgs_setup::test_setup(
        "test_host_phase1_updater_updates_sled",
        SpPort::One,
    )
    .await;

    // We'll only talk to one sp-sim for this test.
    let mgs_client = &mgstestctx.client;
    let sp_sim = &mgstestctx.simrack.gimlets[0];
    let sp_type = SpType::Sled;
    let sp_component = SpComponent::HOST_CPU_BOOT_FLASH.const_as_str();
    let sp_slot = 0;
    let phase1_checker =
        Phase1HashStatusChecker { mgs_client, sp_type, sp_slot, sp_component };

    // We want explicit (i.e., not-timer-based) control over when hashing
    // completes.
    let hashing_complete_sender = {
        let (policy, sender) = HostFlashHashPolicy::channel();
        sp_sim.set_phase1_hash_policy(policy).await;
        sender
    };

    // We haven't yet started hashing; we should get the error we expect for
    // both slots.
    for firmware_slot in [0, 1] {
        let status = mgs_client
            .sp_component_hash_firmware_get(
                &sp_type,
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

    // Start hashing firmware slot 0.
    mgs_client
        .sp_component_hash_firmware_start(&sp_type, sp_slot, sp_component, 0)
        .await
        .expect("started firmware hashing");

    // We should see the expected status; hash is computing in slot 0 and not
    // yet started in slot 1.
    phase1_checker
        .assert_status(&[
            (0, ComponentFirmwareHashStatus::HashInProgress),
            (1, ComponentFirmwareHashStatus::HashNotCalculated),
        ])
        .await;

    // We can start hashing firmware slot 0 again; this should be a no-op while
    // hashing is being done.
    mgs_client
        .sp_component_hash_firmware_start(&sp_type, sp_slot, sp_component, 0)
        .await
        .expect("starting hashing while hashing should be okay");

    // Calculate the hashes we expect to see.
    let expected_sha256_0 = Sha256::digest(
        sp_sim
            .host_phase1_data(0)
            .await
            .as_deref()
            .expect("sled should have data in slot 0"),
    )
    .into();
    let expected_sha256_1 = Sha256::digest(
        sp_sim
            .host_phase1_data(1)
            .await
            .as_deref()
            .expect("sled should have data in slot 1"),
    )
    .into();

    // Allow the next `get()` to succeed.
    hashing_complete_sender.complete_next_hashing_attempt();

    // We should see the expected status; hash is complete in slot 0 and not
    // yet started in slot 1.
    phase1_checker
        .assert_status(&[
            (0, ComponentFirmwareHashStatus::Hashed(expected_sha256_0)),
            (1, ComponentFirmwareHashStatus::HashNotCalculated),
        ])
        .await;

    // Repeat, but slot 1.
    mgs_client
        .sp_component_hash_firmware_start(&sp_type, sp_slot, sp_component, 1)
        .await
        .expect("started firmware hashing");
    hashing_complete_sender.complete_next_hashing_attempt();
    phase1_checker
        .assert_status(&[
            (0, ComponentFirmwareHashStatus::Hashed(expected_sha256_0)),
            (1, ComponentFirmwareHashStatus::Hashed(expected_sha256_1)),
        ])
        .await;

    // Upload a new, fake phase1 to slot 1.
    let fake_phase1 = b"test_host_phase1_hashing_fake_data".as_slice();
    let expected_sha256_1 = Sha256::digest(fake_phase1).into();

    // Drive the update to completion.
    {
        let update_id = Uuid::new_v4();
        mgs_client
            .sp_component_update(
                &sp_type,
                sp_slot,
                sp_component,
                1,
                &update_id,
                fake_phase1,
            )
            .await
            .expect("started slot 1 update");
        wait_for_condition(
            || async {
                let update_status = mgs_client
                    .sp_component_update_status(&sp_type, sp_slot, sp_component)
                    .await
                    .expect("got update status")
                    .into_inner();
                match update_status {
                    // expected terminal state
                    SpUpdateStatus::Complete { id } => {
                        if id == update_id {
                            Ok(())
                        } else {
                            Err(CondCheckError::Failed(format!(
                                "unexpected complete ID \
                                 (got {id} expected {update_id})"
                            )))
                        }
                    }

                    // expected intermediate states
                    SpUpdateStatus::Preparing { .. }
                    | SpUpdateStatus::InProgress { .. } => {
                        Err(CondCheckError::NotYet)
                    }

                    // never-expect-to-see states
                    SpUpdateStatus::None
                    | SpUpdateStatus::Aborted { .. }
                    | SpUpdateStatus::Failed { .. }
                    | SpUpdateStatus::RotError { .. } => {
                        Err(CondCheckError::Failed(format!(
                            "unexpected status: {update_status:?}"
                        )))
                    }
                }
            },
            &Duration::from_millis(100),
            &Duration::from_secs(30),
        )
        .await
        .expect("update to sp-sim completed within timeout");
    }

    // Confirm the simulator wrote the expected data in slot 1.
    let slot_1_data = sp_sim.host_phase1_data(1).await.unwrap();
    assert_eq!(*slot_1_data, *fake_phase1);

    // Writing an update should have put slot 1 back into the "needs hashing"
    // state.
    phase1_checker
        .assert_status(&[
            (0, ComponentFirmwareHashStatus::Hashed(expected_sha256_0)),
            (1, ComponentFirmwareHashStatus::HashNotCalculated),
        ])
        .await;

    // Start hashing firmware slot 1.
    mgs_client
        .sp_component_hash_firmware_start(&sp_type, sp_slot, sp_component, 1)
        .await
        .expect("started firmware hashing");
    phase1_checker
        .assert_status(&[
            (0, ComponentFirmwareHashStatus::Hashed(expected_sha256_0)),
            (1, ComponentFirmwareHashStatus::HashInProgress),
        ])
        .await;

    // Allow hashing to complete.
    hashing_complete_sender.complete_next_hashing_attempt();
    phase1_checker
        .assert_status(&[
            (0, ComponentFirmwareHashStatus::Hashed(expected_sha256_0)),
            (1, ComponentFirmwareHashStatus::Hashed(expected_sha256_1)),
        ])
        .await;

    mgstestctx.teardown().await;
}
