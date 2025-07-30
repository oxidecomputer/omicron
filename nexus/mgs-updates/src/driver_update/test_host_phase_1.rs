// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for Reconfigurator-based host phase 1 updates.

// TODO-john remove and cleanup later
#![allow(unused_imports)]

use super::*;
use crate::test_util::host_phase_2_test_state::HostPhase2TestContext;
use crate::test_util::test_artifacts::TestArtifacts;
use crate::test_util::updates::ExpectedSpComponent;
use crate::test_util::updates::UpdateDescription;
use assert_matches::assert_matches;
use gateway_client::types::SpType;
use gateway_messages::SpPort;
use gateway_test_utils::setup::GatewayTestContext;
use gateway_types::rot::RotSlot;
use nexus_types::deployment::ExpectedActiveRotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::internal_api::views::UpdateAttemptStatus;
use nexus_types::internal_api::views::UpdateCompletedHow;
use nexus_types::inventory::BaseboardId;
use slog_error_chain::InlineErrorChain;
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

    let mut in_progress1 = desc1.setup().await;
    let mut in_progress2 = desc2.setup().await;

    // Start one, but pause it while waiting for the update to upload.
    in_progress1.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

    // Start the other.  Pause it at the point where it's also waiting for
    // the upload to finish.
    in_progress2.run_until_status(UpdateAttemptStatus::UpdateWaiting).await;

    // This time, resume the first update. It will take over the second one.
    let finished1 = in_progress1.finish().await;
    finished1.expect_host_phase_1_success(
        UpdateCompletedHow::TookOverConcurrentUpdate,
    );

    // Now resume the second update. It should recognize that a takeover
    // happened and that an update is completed.
    //
    // TODO we should confirm it doesn't reset the host again
    let finished2 = in_progress2.finish().await;
    finished2.expect_host_phase_1_success(UpdateCompletedHow::CompletedUpdate);

    artifacts.teardown().await;
    phase2ctx.teardown().await;
    gwtestctx.teardown().await;
}
