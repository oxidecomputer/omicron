// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for Reconfigurator-based host phase 1 updates.

use super::*;
use crate::test_util::host_phase_2_test_state::HostPhase2TestContext;
use crate::test_util::sp_test_state::SpTestState;
use crate::test_util::test_artifacts::TestArtifacts;
use crate::test_util::updates::ExpectedSpComponent;
use crate::test_util::updates::UpdateDescription;
use assert_matches::assert_matches;
use gateway_client::types::SpType;
use gateway_messages::SpPort;
use gateway_test_utils::setup::GatewayTestContext;
use nexus_types::internal_api::views::UpdateAttemptStatus;
use nexus_types::internal_api::views::UpdateCompletedHow;
use nexus_types::inventory::BaseboardId;
use omicron_common::disk::M2Slot;
use slog_error_chain::InlineErrorChain;
use sp_sim::SimulatedSp;
use std::time::Duration;
use tufaceous_artifact::ArtifactHash;

struct HostPhase2TestContexts {
    sleds: Vec<HostPhase2TestContext>,
}

impl HostPhase2TestContexts {
    fn new(gwtestctx: &GatewayTestContext) -> Self {
        let sleds = gwtestctx
            .simrack
            .gimlets
            .iter()
            .map(|sp| {
                HostPhase2TestContext::new(
                    &gwtestctx.logctx.log,
                    sp.power_state_rx()
                        .expect("simulated sleds should have power states"),
                )
                .expect("created host phase 2 test context")
            })
            .collect();
        Self { sleds }
    }

    async fn teardown(self) {
        for sled in self.sleds {
            sled.teardown().await;
        }
    }
}

async fn run_one_successful_host_phase_1_update(
    gwtestctx: &GatewayTestContext,
    phase2ctx: &HostPhase2TestContexts,
    artifacts: &TestArtifacts,
    sp_type: SpType,
    slot_id: u16,
    artifact_hash: &ArtifactHash,
    expected_result: UpdateCompletedHow,
) {
    assert_matches!(sp_type, SpType::Sled);
    let host_phase_2_state = phase2ctx.sleds[usize::from(slot_id)].state_rx();

    let desc = UpdateDescription {
        gwtestctx,
        artifacts,
        sp_type,
        slot_id,
        artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state,
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    let in_progress = desc.setup().await;
    let finished = in_progress.finish().await;
    finished.expect_host_phase_1_success(expected_result);
}

/// Tests several happy-path cases of updating a host OS phase 1
#[tokio::test]
async fn basic() {
    let gwtestctx = gateway_test_utils::setup::test_setup_metrics_disabled(
        "test_host_phase_1_basic",
        SpPort::One,
    )
    .await;
    let log = &gwtestctx.logctx.log;
    let artifacts = TestArtifacts::new(log).await.unwrap();

    // Create a fake host phase 2 context for each simulated sled.
    let phase2ctx = HostPhase2TestContexts::new(&gwtestctx);

    // Basic case: normal update
    run_one_successful_host_phase_1_update(
        &gwtestctx,
        &phase2ctx,
        &artifacts,
        SpType::Sled,
        1,
        &artifacts.host_phase_1_artifact_hash,
        UpdateCompletedHow::CompletedUpdate,
    )
    .await;

    // Basic case: attempted update, found no changes needed
    run_one_successful_host_phase_1_update(
        &gwtestctx,
        &phase2ctx,
        &artifacts,
        SpType::Sled,
        1,
        &artifacts.host_phase_1_artifact_hash,
        UpdateCompletedHow::FoundNoChangesNeeded,
    )
    .await;

    // Unlike SP/RoT tests, we don't try other `SpType`s; only sleds have host
    // OS to be updated. We can't even describe the expected preconditions for
    // such a device, because there's no sled-agent to ask about its current
    // phase 2 contents.

    artifacts.teardown().await;
    phase2ctx.teardown().await;
    gwtestctx.teardown().await;
}

/// Tests the case where two updates run concurrently.  One notices another
/// is running and waits for it to complete.
#[tokio::test]
async fn update_watched() {
    let gwtestctx = gateway_test_utils::setup::test_setup_metrics_disabled(
        "test_host_phase_1_update_watched",
        SpPort::One,
    )
    .await;
    let log = &gwtestctx.logctx.log;
    let artifacts = TestArtifacts::new(log).await.unwrap();
    let phase2ctx = HostPhase2TestContexts::new(&gwtestctx);

    // We're going to start two concurrent update attempts.  The sequence
    // we want is:
    //
    // - update1 get far enough along to upload the phase 1 artifact to MGS, but
    //   does not finish waiting for it to finish being delivered to the SP.
    // - update2 runs to completion
    // - update1 resumes
    //
    // It's a little tricky to orchestrate this with the tools we have
    // available.  The most robust is actually have update2's precondition
    // reflect that update1 has finished its upload.
    //
    // Note that this ordering is different from the analogous test for SP
    // updates due to different SP behavior: when an SP update image has been
    // staged, any subsequent update attempts will fail. This is not true of
    // other components (including this one).
    let host_phase_2_state = phase2ctx.sleds[1].state_rx();

    let desc1 = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    let desc2 = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: Some(
                artifacts.host_phase_1_artifact_hash,
            ),
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    let mut in_progress1 = desc1.setup().await;
    let in_progress2 = desc2.setup().await;

    // Start one, but pause it while waiting for the update to upload.
    in_progress1.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

    // Run the other.
    let finished2 = in_progress2.finish().await;

    // Now finish the first update.
    let finished1 = in_progress1.finish().await;

    // Both should succeed, but with different codes.
    finished1.expect_host_phase_1_success(
        UpdateCompletedHow::WaitedForConcurrentUpdate,
    );
    finished2.expect_host_phase_1_success(UpdateCompletedHow::CompletedUpdate);

    artifacts.teardown().await;
    phase2ctx.teardown().await;
    gwtestctx.teardown().await;
}

/// Tests the case where an update takes over from a previously-started one.
#[tokio::test]
async fn update_takeover() {
    let gwtestctx = gateway_test_utils::setup::test_setup_metrics_disabled(
        "test_host_phase_1_update_takeover",
        SpPort::One,
    )
    .await;
    let log = &gwtestctx.logctx.log;
    let artifacts = TestArtifacts::new(log).await.unwrap();
    let phase2ctx = HostPhase2TestContexts::new(&gwtestctx);

    // See the notes in update_watched(). We start the same way, but this time
    // we pause the second test once it starts its upload and resume the first
    // update; it should perform a takeover.
    let host_phase_2_state = phase2ctx.sleds[1].state_rx();
    let target_sp_sim = &gwtestctx.simrack.gimlets[1];

    let desc1 = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        // This timeout (10 seconds) seeks to balance being long enough to
        // be relevant without making the tests take too long.  (It's
        // assumed that 10 seconds here is not a huge deal because this is
        // mostly idle time and this test is unlikely to be the long pole.)
        override_progress_timeout: Some(Duration::from_secs(10)),
    };

    let desc2 = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: Some(
                artifacts.host_phase_1_artifact_hash,
            ),
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    // Before we start, our simulated SP should have 0 power state changes.
    assert_eq!(target_sp_sim.power_state_changes(), 0);

    let mut in_progress1 = desc1.setup().await;
    let mut in_progress2 = desc2.setup().await;

    // Start one, but pause it while waiting for the update to upload.
    in_progress1.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

    // Start the other.  Pause it at the point where it's also waiting for
    // the upload to finish.
    in_progress2.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

    // This time, resume the first update. It will see that the second update's
    // upload has succeeded, wait for it to make progress, see no progress, and
    // perform a takeover.
    let finished1 = in_progress1.finish().await;
    finished1.expect_host_phase_1_success(
        UpdateCompletedHow::TookOverConcurrentUpdate,
    );

    // This should have caused 2 power state changes (transition to A2 then back
    // to A0; i.e., perform a host reset).
    assert_eq!(target_sp_sim.power_state_changes(), 2);

    // Now resume the second update. It should recognize that a takeover
    // happened and that an update is completed.
    let finished2 = in_progress2.finish().await;
    finished2.expect_host_phase_1_success(UpdateCompletedHow::CompletedUpdate);

    // Finishing this second update should _not_ perform another reset of the
    // host; it should have realized that the update was already complete.
    //
    // However, it currently does. We've gone through this sequence:
    //
    // 1. update1 uploads the artifact with its update ID; the SP update status
    //    is `Completed(update_id_1)`
    // 2. update2 uploads the artifact with its update ID; the SP update status
    //    is `Completed(update_id_2)`
    // 3. update1 waits for update 2 to make progress, then performs a takeover,
    //    then finishes the update (i.e., reboots the host)
    // 4. update2 resumes polling the update status; it sees
    //    `Completed(update_id_2)`, so believe it's still in charge, and so
    //    performs another host reset.
    //
    // Fixing this probably requires some support from the SP: we should tie the
    // update finialization steps (changing the active slot and power cycling
    // the host) to the update ID used to deliver the artifact. If we do that,
    // ideally we could uncomment this assertion.
    //
    // The toplevel issue for related SP work is
    // https://github.com/oxidecomputer/hubris/issues/2178.
    //
    // assert_eq!(target_sp_sim.power_state_changes(), 2);

    artifacts.teardown().await;
    phase2ctx.teardown().await;
    gwtestctx.teardown().await;
}

/// Tests a bunch of easy fast-failure cases.
#[tokio::test]
async fn basic_failures() {
    let gwtestctx = gateway_test_utils::setup::test_setup(
        "test_host_phase_1_basic_failures",
        SpPort::One,
    )
    .await;
    let log = &gwtestctx.logctx.log;
    let artifacts = TestArtifacts::new(log).await.unwrap();
    let phase2ctx = HostPhase2TestContexts::new(&gwtestctx);
    let host_phase_2_state = phase2ctx.sleds[1].state_rx();

    // We use `fff...fff` as our fake non-matching artifact hash in several
    // tests below; get the actual artifact hashes reported by our test setup
    // and ensure none of them match that.
    let (active_phase_1_hash, inactive_phase_1_hash, phase_1_slot) = {
        let sp_init = SpTestState::load(&gwtestctx.client(), SpType::Sled, 1)
            .await
            .expect("loading initial state");
        (
            sp_init.expect_host_phase_1_active_hash(),
            sp_init.expect_host_phase_1_inactive_hash(),
            sp_init.expect_host_phase_1_active_slot(),
        )
    };
    let (active_phase_2_hash, inactive_phase_2_hash, boot_disk) = {
        let sled_init = host_phase_2_state.borrow();
        (
            sled_init.active_slot_artifact(),
            sled_init.inactive_slot_artifact(),
            sled_init.boot_disk().expect("fake sled has booted"),
        )
    };
    let bad_hash = ArtifactHash([0xff; 32]);
    assert_ne!(active_phase_1_hash, bad_hash);
    assert_ne!(active_phase_2_hash, bad_hash);
    assert_ne!(inactive_phase_1_hash, bad_hash);
    assert_ne!(inactive_phase_2_hash, bad_hash);
    assert_eq!(phase_1_slot, M2Slot::A);
    assert_eq!(boot_disk, M2Slot::A);

    // Test a case of mistaken identity (reported baseboard does not match
    // the one that we expect).
    let desc = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: Some(BaseboardId {
            part_number: String::from("i86pc"),
            serial_number: String::from("SimGimlet0"),
        }),
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
        assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
        let message = InlineErrorChain::new(error).to_string();
        eprintln!("{}", message);
        assert!(message.contains(
            "in sled slot 1, expected to find part \"i86pc\" serial \
                     \"SimGimlet0\", but found part \"i86pc\" serial \
                     \"SimGimlet01\"",
        ));

        // No changes should have been made in this case.
        assert_eq!(sp1, sp2);
    });

    // Test a case where the active phase 1 slot doesn't match what we expect.
    let desc = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: Some(M2Slot::B),
            override_expected_boot_disk: None,
            override_expected_active_phase_1: Some(bad_hash),
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
        assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
        let message = InlineErrorChain::new(error).to_string();
        eprintln!("{}", message);
        assert!(message.contains(
            "expected to find active host phase 1 slot B, but found A"
        ));

        // No changes should have been made in this case.
        assert_eq!(sp1, sp2);
    });

    // Test a case where the sled boot disk doesn't match what we expect.
    let desc = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: Some(M2Slot::B),
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
        assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
        let message = InlineErrorChain::new(error).to_string();
        eprintln!("{}", message);
        assert!(
            message
                .contains("expected to find host OS boot disk B, but found A")
        );

        // No changes should have been made in this case.
        assert_eq!(sp1, sp2);
    });

    // Test a case where the active phase 1 artifact doesn't match what we
    // expect.
    let desc = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: Some(bad_hash),
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
        assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
        let message = InlineErrorChain::new(error).to_string();
        eprintln!("{}", message);
        assert!(message.contains(&format!(
            "expected to find active host_phase_1 artifact {bad_hash}, \
             but found {active_phase_1_hash}"
        )));

        // No changes should have been made in this case.
        assert_eq!(sp1, sp2);
    });

    // Test a case where the inactive phase 1 artifact doesn't match what it
    // should.
    let desc = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: Some(bad_hash),
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };

    desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
        assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
        let message = InlineErrorChain::new(error).to_string();
        eprintln!("{}", message);
        assert!(message.contains(&format!(
            "expected to find inactive host_phase_1 artifact {bad_hash}, \
             but found {inactive_phase_1_hash}"
        )));

        // No changes should have been made in this case.
        assert_eq!(sp1, sp2);
    });

    // Test a case where the active phase 2 artifact doesn't match what it
    // should.
    let desc = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: Some(bad_hash),
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };
    desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
        assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
        let message = InlineErrorChain::new(error).to_string();
        eprintln!("{}", message);
        assert!(message.contains(&format!(
            "expected to find active host_phase_2 artifact {bad_hash}, \
             but found {active_phase_2_hash}"
        )));

        // No changes should have been made in this case.
        assert_eq!(sp1, sp2);
    });

    // Test a case where the inactive phase 2 artifact doesn't match what it
    // should.
    let desc = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: Some(bad_hash),
        },
        override_progress_timeout: None,
    };

    desc.setup().await.finish().await.expect_failure(&|error, sp1, sp2| {
        assert_matches!(error, ApplyUpdateError::PreconditionFailed(..));
        let message = InlineErrorChain::new(error).to_string();
        eprintln!("{}", message);
        assert!(message.contains(&format!(
            "expected to find inactive host_phase_2 artifact {bad_hash}, \
             but found {inactive_phase_2_hash}"
        )));

        // No changes should have been made in this case.
        assert_eq!(sp1, sp2);
    });

    // Test a case where we fail to fetch the artifact.  We simulate this by
    // tearing down our artifact server before the update starts.
    let desc = UpdateDescription {
        gwtestctx: &gwtestctx,
        artifacts: &artifacts,
        sp_type: SpType::Sled,
        slot_id: 1,
        artifact_hash: &artifacts.host_phase_1_artifact_hash,
        override_baseboard_id: None,
        override_expected_sp_component: ExpectedSpComponent::HostPhase1 {
            host_phase_2_state: host_phase_2_state.clone(),
            override_expected_phase_1_slot: None,
            override_expected_boot_disk: None,
            override_expected_active_phase_1: None,
            override_expected_active_phase_2: None,
            override_expected_inactive_phase_1: None,
            override_expected_inactive_phase_2: None,
        },
        override_progress_timeout: None,
    };
    let in_progress = desc.setup().await;
    artifacts.teardown().await;
    in_progress.finish().await.expect_failure(&|error, sp1, sp2| {
        assert_matches!(error, ApplyUpdateError::FetchArtifact(..));
        // No changes should have been made in this case.
        assert_eq!(sp1, sp2);
    });

    phase2ctx.teardown().await;
    gwtestctx.teardown().await;
}
