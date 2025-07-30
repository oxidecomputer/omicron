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
    let gwtestctx = gateway_test_utils::setup::test_setup(
        "test_sp_update_basic",
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

    artifacts.teardown().await;
    phase2ctx.teardown().await;
    gwtestctx.teardown().await;
}
