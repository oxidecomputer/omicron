// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for carrying out updates using `apply_update()`
//!
//! These are factored to make it easy to write a variety of different kinds of
//! tests without having to put together too much boilerplate in each test.

use crate::driver::UpdateAttemptStatusUpdater;
use crate::driver_update::ApplyUpdateError;
use crate::driver_update::PROGRESS_TIMEOUT;
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
use gateway_types::rot::RotSlot;
use nexus_types::deployment::ExpectedActiveRotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::internal_api::views::InProgressUpdateStatus;
use nexus_types::internal_api::views::MgsUpdateDriverStatus;
use nexus_types::internal_api::views::UpdateAttemptStatus;
use nexus_types::internal_api::views::UpdateCompletedHow;
use nexus_types::inventory::BaseboardId;
use omicron_uuid_kinds::SpUpdateUuid;
use slog::debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;

pub enum ExpectedSpComponent {
    Sp {
        override_expected_active: Option<ArtifactVersion>,
        override_expected_inactive: Option<ExpectedVersion>,
    },
    // TODO-K: Remove once fully implemented
    #[allow(dead_code)]
    Rot {
        override_expected_active_slot: Option<ExpectedActiveRotSlot>,
        override_expected_inactive_version: Option<ExpectedVersion>,
        override_expected_persistent_boot_preference: Option<RotSlot>,
        override_expected_pending_persistent_boot_preference: Option<RotSlot>,
        override_expected_transient_boot_preference: Option<RotSlot>,
    },
    // TODO-K: Remove once fully implemented
    #[allow(dead_code)]
    RotBootloader {},
}

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
    // TODO-K: change description "Overrides" above
    pub expected_sp_component: ExpectedSpComponent,
    pub override_progress_timeout: Option<Duration>,
}

impl UpdateDescription<'_> {
    /// Sets up a single attempt to execute the update described by `self`
    ///
    /// Execution does not start until you call `run_until_status()` or
    /// `finish()`on the returned value.
    pub async fn setup(&self) -> InProgressAttempt {
        let mgs_client = self.gwtestctx.client();

        // Fetch information about the device that we're going to update.
        // This will be used to configure the preconditions (expected baseboard
        // id and expected active/inactive slot contents).
        let sp1 = SpTestState::load(&mgs_client, self.sp_type, self.slot_id)
            .await
            .expect("loading initial state");
        let baseboard_id = Arc::new(
            self.override_baseboard_id
                .clone()
                .unwrap_or_else(|| sp1.baseboard_id()),
        );

        let details = match &self.expected_sp_component {
            ExpectedSpComponent::Sp {
                override_expected_active,
                override_expected_inactive,
            } => {
                let expected_active_version = override_expected_active
                    .clone()
                    .unwrap_or_else(|| sp1.expect_sp_active_version());
                let expected_inactive_version = override_expected_inactive
                    .clone()
                    .unwrap_or_else(|| sp1.expect_sp_inactive_version());

                PendingMgsUpdateDetails::Sp {
                    expected_active_version,
                    expected_inactive_version,
                }
            }
            ExpectedSpComponent::Rot {
                override_expected_active_slot,
                override_expected_inactive_version,
                override_expected_persistent_boot_preference,
                override_expected_pending_persistent_boot_preference,
                override_expected_transient_boot_preference,
            } => {
                let expected_active_slot = override_expected_active_slot
                    .clone()
                    .unwrap_or_else(|| sp1.expected_active_rot_slot());
                let expected_inactive_version =
                    override_expected_inactive_version
                        .clone()
                        .unwrap_or_else(|| sp1.expect_rot_inactive_version());
                let expected_persistent_boot_preference =
                    override_expected_persistent_boot_preference
                        .unwrap_or_else(|| {
                            sp1.expect_rot_persistent_boot_preference()
                        });
                let expected_pending_persistent_boot_preference =
                    override_expected_pending_persistent_boot_preference
                        .or_else(|| {
                            sp1.expect_rot_pending_persistent_boot_preference()
                        });
                let expected_transient_boot_preference =
                    override_expected_transient_boot_preference
                        .or_else(|| sp1.expect_rot_transient_boot_preference());

                PendingMgsUpdateDetails::Rot {
                    expected_active_slot,
                    expected_inactive_version,
                    expected_persistent_boot_preference,
                    expected_pending_persistent_boot_preference,
                    expected_transient_boot_preference,
                }
            }
            ExpectedSpComponent::RotBootloader {} => unimplemented!(),
        };

        let deployed_caboose = self
            .artifacts
            .deployed_caboose(self.artifact_hash)
            .expect("caboose for generated artifact");

        // Assemble the driver-level update request.
        let mgs_backends = self.gwtestctx.mgs_backends();
        let sp_update_request = PendingMgsUpdate {
            baseboard_id: baseboard_id.clone(),
            sp_type: self.sp_type,
            slot_id: self.slot_id,
            details,
            artifact_hash: *self.artifact_hash,
            artifact_version: std::str::from_utf8(
                deployed_caboose.version().unwrap(),
            )
            .unwrap()
            .parse()
            .unwrap(),
        };

        let request = sp_update_request.clone();
        let update_id = SpUpdateUuid::new_v4();
        let log = self.gwtestctx.logctx.log.new(slog::o!(
            "update_id" => update_id.to_string(),
            baseboard_id.clone(),
            sp_update_request.clone()
        ));
        let progress_timeout =
            self.override_progress_timeout.unwrap_or(PROGRESS_TIMEOUT);

        // Assemble the initial status object.
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
        let status_updater =
            UpdateAttemptStatusUpdater::new(status_tx.clone(), baseboard_id);

        // Create a future to actually do the update.
        let artifact_cache = self.artifacts.artifact_cache.clone();
        let future_log = log.clone();
        let future = async move {
            let sp_update = SpComponentUpdate::from_request(
                &future_log,
                &request,
                update_id,
            );
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
            log,
            status_rx,
            step: Some(StepResult::ReadyAgain(StepThrough::new(future))),
            sp_type: self.sp_type,
            slot_id: self.slot_id,
            mgs_client: self.gwtestctx.client(),
            sp1,
            deployed_caboose: deployed_caboose.clone(),
        }
    }
}

/// Describes the state of an in-progress attempt to perform an SP update
pub struct InProgressAttempt {
    // Execution state
    log: slog::Logger,
    mgs_client: gateway_client::Client,

    // `step` contains the future that actually applies the update, wrapped in a
    // `StepResult` that allows us to step through execution of that future.
    // This is non-None except inside `run_until_status()`.
    step: Option<
        StepResult<'static, Result<UpdateCompletedHow, ApplyUpdateError>>,
    >,

    // Parameters of the update itself
    sp_type: SpType,
    slot_id: u32,
    deployed_caboose: hubtools::Caboose,

    // Status of the driver
    // (allows us to run until we get to a specific "status")
    status_rx: watch::Receiver<MgsUpdateDriverStatus>,

    /// SP state before the update began
    // (used to verify behavior at the end)
    sp1: SpTestState,
}

/// Describes whether an in-progress update attempt is paused or finished after
/// `run_until_status()` returns.
#[derive(Debug, Eq, PartialEq)]
pub enum Paused {
    Paused,
    Done,
}

impl InProgressAttempt {
    /// Proceeds with execution of the update attempt but pauses execution once
    /// the status reaches `status`.
    ///
    /// If the future returns before being paused, this function returns.  You
    /// can look at the return value to figure out if the future was paused or
    /// if it finished early.  If it finishes early, you still have to use
    /// `finished()` to get a `FinishedUpdateAttempt` that lets you verify the
    /// end state.
    ///
    /// The pausing mechanism is somewhat approximate.  The current
    /// implementation will return as soon as the future wakes up from having
    /// blocked and the status matches `status`.  That means if the future sets
    /// the requested status but doesn't block, it won't be caught.  (Instead,
    /// this function will wind up running it until it finishes.)
    pub async fn run_until_status(
        &mut self,
        status: UpdateAttemptStatus,
    ) -> Paused {
        loop {
            assert!(self.step.is_some());

            // Check the current status of the update so that we can return
            // immediately if the status has reached the requested one.
            //
            // There is at most one entry in `in_progress` here.  If there's no
            // status here at all, that means the update already finished.
            // We won't return early in this case.  Instead, we'll wait for the
            // future itself to finish and then return indicating that it's
            // done.
            let overall_status = self.status_rx.borrow();
            let maybe_current_status =
                overall_status.in_progress.values().next();
            if let Some(current_status) = maybe_current_status {
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
            // Drop `overall_status` to avoid blocking the watch channel.
            drop(overall_status);

            // If the future is done, we're done.
            if let Some(StepResult::Done(_)) = &self.step {
                debug!(&self.log, "run_until_status({:?}): done early", status);
                return Paused::Done;
            }

            // Otherwise, take a step.
            debug!(&self.log, "run_until_status({:?}): stepping", status);
            let step = self.step.take().expect("self.step is always non-None");
            self.step = Some(step.step().await);
        }
    }

    /// Runs the future to completion, if it hasn't already finished, and
    /// returns a `FinishedUpdateAttempt` that lets you verify the final state.
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

/// Describes the final state of an update attempt and provides helpers for
/// verifying the result
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

    /// Asserts various conditions associated with successful updates.
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

    /// Asserts that the update failed and invokes `assert_error(error,
    /// initial_sp_state, final_sp_state)` for you to make your own assertions
    /// about why it failed and what state things were left in.
    pub fn expect_failure(
        &self,
        assert_error: &dyn Fn(&ApplyUpdateError, &SpTestState, &SpTestState),
    ) {
        let Err(error) = &self.result else {
            panic!("unexpected success from apply_update(): {:?}", self.result);
        };

        assert_error(error, &self.sp1, &self.sp2);
    }
}
