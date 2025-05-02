// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for carrying out updates using `apply_update()`
//!
//! These are factored to make it easy to write a variety of different kinds of
//! tests without having to put together too much boilerplate in each test.

use crate::ArtifactCache;
use crate::driver::UpdateAttemptStatusUpdater;
use crate::driver_update::ApplyUpdateError;
use crate::driver_update::SpComponentUpdate;
use crate::driver_update::apply_update;
use crate::sp_updater::ReconfiguratorSpUpdater;
use crate::test_util::cabooses_equal;
use crate::test_util::sp_test_state::SpTestState;
use crate::test_util::step_through::StepResult;
use crate::test_util::step_through::StepThrough;
use crate::test_util::test_artifacts::TestArtifacts;
use futures::FutureExt;
use gateway_client::types::SpType;
use gateway_test_utils::setup::GatewayTestContext;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::internal_api::views::InProgressUpdateStatus;
use nexus_types::internal_api::views::MgsUpdateDriverStatus;
use nexus_types::internal_api::views::UpdateAttemptStatus;
use nexus_types::internal_api::views::UpdateCompletedHow;
use nexus_types::inventory::BaseboardId;
use slog::debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;
use uuid::Uuid;

/// Describes an update operation that can later be executed any number of times
pub struct UpdateDescription<'a> {
    // Execution information
    pub gwtestctx: &'a GatewayTestContext,
    pub artifacts: &'a TestArtifacts,

    // Update parameters
    pub sp_type: SpType,
    pub slot_id: u32,
    pub artifact_hash: &'a ArtifactHash,

    // Overrides
    //
    // If `None`, the correct value is determined automatically.  These are
    // overridable in order to induce specific kinds of failures.
    pub override_baseboard_id: Option<BaseboardId>,
    pub override_expected_active: Option<ArtifactVersion>,
    pub override_expected_inactive: Option<ExpectedVersion>,
}

impl<'a> UpdateDescription<'a> {
    /// Fetches live state about an SP in preparation to begin an update
    /// attempt
    pub async fn load(&self) -> UpdateAttempt<'a> {
        let mgs_client = self.gwtestctx.client();

        // Fetch information about the device that we're going to update.
        let sp1 = SpTestState::load(&mgs_client, self.sp_type, self.slot_id)
            .await
            .expect("loading initial state");

        let update_id = Uuid::new_v4();
        let baseboard_id = Arc::new(
            self.override_baseboard_id
                .clone()
                .unwrap_or_else(|| sp1.baseboard_id()),
        );
        let expected_active_version = self
            .override_expected_active
            .clone()
            .unwrap_or_else(|| sp1.expect_sp_active_version());
        let expected_inactive_version = self
            .override_expected_inactive
            .clone()
            .unwrap_or_else(|| sp1.expect_sp_inactive_version());
        let deployed_caboose = self
            .artifacts
            .deployed_caboose(self.artifact_hash)
            .expect("caboose for generated artifact");
        let sp_update_request = PendingMgsUpdate {
            baseboard_id: baseboard_id.clone(),
            sp_type: self.sp_type,
            slot_id: self.slot_id,
            details: PendingMgsUpdateDetails::Sp {
                expected_active_version,
                expected_inactive_version,
            },
            artifact_hash: self.artifact_hash.clone(),
            artifact_version: std::str::from_utf8(
                deployed_caboose.version().unwrap(),
            )
            .unwrap()
            .parse()
            .unwrap(),
        };
        let log = self.gwtestctx.logctx.log.new(slog::o!(
            "update_id" => update_id.to_string(),
            baseboard_id,
            sp_update_request.clone()
        ));

        UpdateAttempt {
            gwtestctx: self.gwtestctx,
            artifact_cache: self.artifacts.artifact_cache.clone(),
            log,
            sp_type: self.sp_type,
            slot_id: self.slot_id,
            deployed_caboose: deployed_caboose.clone(),
            update_id,
            sp_update_request,
            sp1,
        }
    }
}

/// Describes an attempt to complete one update operation
// It's important that this not keep a reference to an `UpdateDescription`
// or its associated `TestArtifacts` because some tests will want to drop
// the `TestArtifacts`.
pub struct UpdateAttempt<'a> {
    // Execution information
    gwtestctx: &'a GatewayTestContext,
    artifact_cache: Arc<ArtifactCache>,
    log: slog::Logger,

    // Update parameters
    sp_type: SpType,
    slot_id: u32,
    deployed_caboose: hubtools::Caboose,
    update_id: Uuid,
    sp_update_request: PendingMgsUpdate,

    // Initial state
    sp1: SpTestState,
}

impl<'a> UpdateAttempt<'a> {
    pub async fn begin<'b>(
        self,
        progress_timeout: Duration,
    ) -> InProgressAttempt<'b>
    where
        'b: 'a,
    {
        let log = self.log.clone();
        let request = self.sp_update_request.clone();
        let baseboard_id = request.baseboard_id.clone();
        let mgs_backends = self.gwtestctx.mgs_backends();
        let (status_tx, status_rx) =
            watch::channel(MgsUpdateDriverStatus::default());
        status_tx.send_modify(|status| {
            status.in_progress.insert(
                baseboard_id.clone(),
                InProgressUpdateStatus {
                    time_started: chrono::Utc::now(),
                    status: UpdateAttemptStatus::NotStarted,
                    nattempts_done: 0,
                },
            );
        });
        let status_updater = UpdateAttemptStatusUpdater::new(
            status_tx.clone(),
            baseboard_id.clone(),
        );

        let future = async move {
            let artifact_cache = self.artifact_cache.clone();
            let sp_update =
                SpComponentUpdate::from_request(&log, &request, self.update_id);
            let sp_update_helper = Box::new(ReconfiguratorSpUpdater {});
            apply_update(
                artifact_cache,
                &sp_update,
                &*sp_update_helper,
                mgs_backends.clone(),
                &request,
                status_updater,
                progress_timeout,
            )
            .await
        }
        .boxed();

        InProgressAttempt {
            log: self.log,
            baseboard_id,
            status_rx,
            step: Some(StepResult::ReadyAgain(StepThrough::new(future))),
            sp_type: self.sp_type,
            slot_id: self.slot_id,
            mgs_client: self.gwtestctx.client(),
            sp1: self.sp1,
            deployed_caboose: self.deployed_caboose,
        }
    }
}

pub struct InProgressAttempt<'a> {
    log: slog::Logger,
    baseboard_id: Arc<BaseboardId>,
    status_rx: watch::Receiver<MgsUpdateDriverStatus>,
    step: Option<StepResult<'a, Result<UpdateCompletedHow, ApplyUpdateError>>>,
    sp_type: SpType,
    slot_id: u32,
    mgs_client: gateway_client::Client,
    sp1: SpTestState,
    deployed_caboose: hubtools::Caboose,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Paused {
    Paused,
    Done,
}

impl<'a> InProgressAttempt<'a> {
    pub async fn run_until_status(
        &mut self,
        status: UpdateAttemptStatus,
    ) -> Paused {
        loop {
            assert!(self.step.is_some());

            // If the condition is already true, return now.  If the actual
            // status is missing, then this must have already finished but
            // the future hasn't completed yet.  Ignore the condition and
            // wait for the future to finish.
            if let Some(current_status) =
                self.status_rx.borrow().in_progress.get(&self.baseboard_id)
            {
                if current_status.status == status {
                    debug!(
                        &self.log,
                        "run_until_status({:?}): pausing", status,
                    );
                    return Paused::Paused;
                }

                debug!(
                    &self.log,
                    "run_until_status({:?}): status = {:?}",
                    status,
                    current_status.status
                );
            }

            // If we're done, we're done.
            if let Some(StepResult::Done(_)) = &self.step {
                debug!(&self.log, "run_until_status({:?}): done early", status,);
                return Paused::Done;
            }

            // Otherwise, take a step.
            debug!(&self.log, "run_until_status({:?}): stepping", status);
            let step = self.step.take().expect("self.step is always non-None");
            self.step = Some(step.step().await);
        }
    }

    pub async fn finish(self) -> FinishedUpdateAttempt {
        debug!(&self.log, "finishing update");
        let result = match self.step.expect("self.step is always non-None") {
            StepResult::ReadyAgain(step_through) => step_through.finish().await,
            StepResult::Done(result) => result,
        };

        debug!(&self.log, "update done"; "result" => ?result);
        FinishedUpdateAttempt::new(
            self.sp_type,
            self.slot_id,
            self.sp1,
            self.deployed_caboose,
            result,
            self.mgs_client,
        )
        .await
    }
}

#[must_use]
pub struct FinishedUpdateAttempt {
    result: Result<UpdateCompletedHow, ApplyUpdateError>,
    deployed_caboose: hubtools::Caboose,
    sp1: SpTestState,
    sp2: SpTestState,
}

impl FinishedUpdateAttempt {
    async fn new(
        sp_type: SpType,
        slot_id: u32,
        sp1: SpTestState,
        deployed_caboose: hubtools::Caboose,
        result: Result<UpdateCompletedHow, ApplyUpdateError>,
        mgs_client: gateway_client::Client,
    ) -> FinishedUpdateAttempt {
        let sp2 = SpTestState::load(&mgs_client, sp_type, slot_id)
            .await
            .expect("SP state after update");
        FinishedUpdateAttempt { result, deployed_caboose, sp1, sp2 }
    }

    pub fn expect_success(&self, expected_result: UpdateCompletedHow) {
        let how = match self.result {
            Ok(how) if how == expected_result => how,
            _ => {
                panic!(
                    "unexpected result from apply_update(): {:?}",
                    self.result,
                );
            }
        };

        eprintln!("apply_update() -> {:?}", how);
        let sp2 = &self.sp2;

        // The active slot should contain what we just updated to.
        let deployed_caboose = &self.deployed_caboose;
        assert!(cabooses_equal(&sp2.caboose_sp_active, &deployed_caboose));
        // RoT information should not have changed.
        let sp1 = &self.sp1;
        assert_eq!(sp1.sp_boot_info, sp2.sp_boot_info);
        assert_eq!(sp1.expect_caboose_rot_a(), sp2.expect_caboose_rot_a());
        assert_eq!(sp1.expect_caboose_rot_b(), sp2.expect_caboose_rot_b());

        if how == UpdateCompletedHow::FoundNoChangesNeeded {
            assert_eq!(sp1.caboose_sp_active, sp2.caboose_sp_active);
            assert_eq!(
                sp1.expect_caboose_sp_inactive(),
                sp2.expect_caboose_sp_inactive()
            );
            assert_eq!(sp1.sp_state, sp2.sp_state);
            assert_eq!(sp1.sp_boot_info, sp2.sp_boot_info);
        } else {
            // One way or another, an update was completed.  The inactive
            // slot should contain what was in the active slot before.
            assert_eq!(
                sp1.expect_caboose_sp_active(),
                sp2.expect_caboose_sp_inactive()
            );
        }
    }

    pub fn expect_failure(
        &self,
        assert_error: &dyn Fn(
            &ApplyUpdateError,
            &SpTestState,
            &SpTestState,
        ),
    ) {
        let Err(error) = &self.result else {
            panic!("unexpected success from apply_update(): {:?}", self.result);
        };

        assert_error(error, &self.sp1, &self.sp2);
    }
}
