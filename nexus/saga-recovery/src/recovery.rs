// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Guts of saga recovery
// XXX-dap block comment goes here?  and maybe deserves review

use super::status::RecoveryFailure;
use super::status::RecoverySuccess;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use slog::{debug, error, info, warn};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use steno::SagaId;
use tokio::sync::mpsc;

/// Describes state related to saga recovery that needs to be maintained across
/// multiple passes
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RestState {
    sagas_started: BTreeMap<SagaId, SagaStartInfo>,
    remove_next: BTreeSet<SagaId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[allow(dead_code)]
struct SagaStartInfo {
    time_observed: DateTime<Utc>,
    source: SagaStartSource,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SagaStartSource {
    StartChannel,
    Recovered,
}

impl RestState {
    pub fn new() -> RestState {
        RestState {
            sagas_started: BTreeMap::new(),
            remove_next: BTreeSet::new(),
        }
    }

    /// Read messages from the channel (signaling sagas that have started
    /// running) and update our set of sagas that we believe to be running.
    pub fn update_started_sagas(
        &mut self,
        log: &slog::Logger,
        sagas_started_rx: &mut mpsc::UnboundedReceiver<SagaId>,
    ) {
        let (new_sagas, disconnected) = read_all_from_channel(sagas_started_rx);
        if disconnected {
            warn!(
                log,
                "sagas_started_rx disconnected (is Nexus shutting down?)"
            );
        }

        let time_observed = Utc::now();
        for saga_id in new_sagas {
            info!(log, "observed saga start"; "saga_id" => %saga_id);
            assert!(self
                .sagas_started
                .insert(
                    saga_id,
                    SagaStartInfo {
                        time_observed,
                        source: SagaStartSource::StartChannel,
                    }
                )
                .is_none());
        }
    }

    /// Update based on the results of a recovery pass.
    pub fn update_after_pass(&mut self, plan: &Plan, execution: &Execution) {
        let time_observed = Utc::now();

        for saga_id in plan.sagas_inferred_done() {
            assert!(self.sagas_started.remove(&saga_id).is_some());
        }

        for saga_id in execution.sagas_recovered_successfully() {
            assert!(self
                .sagas_started
                .insert(
                    saga_id,
                    SagaStartInfo {
                        time_observed,
                        source: SagaStartSource::Recovered,
                    }
                )
                .is_none());
        }

        self.remove_next = plan.sagas_maybe_done().collect();
    }
}

/// Read all message that are currently available on the given channel (without
/// blocking or waiting)
///
/// Returns the list of messages (as a `Vec`) plus a boolean that's true iff the
/// channel is now disconnected.
fn read_all_from_channel<T>(
    rx: &mut mpsc::UnboundedReceiver<T>,
) -> (Vec<T>, bool) {
    let mut values = Vec::new();
    let mut disconnected = false;

    loop {
        match rx.try_recv() {
            Ok(value) => {
                values.push(value);
            }

            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                disconnected = true;
                break;
            }
        }
    }

    (values, disconnected)
}

/// Describes what should happen during a particular recovery pass
///
/// This is constructed by the saga recovery background task via
/// [`Plan::new()`].  That function uses [`PlanBuilder`].  This all seems a
/// little overboard for such an internal structure but it helps separate
/// concerns, particularly when it comes to testing.
///
/// This structure is also much more detailed than it needs to be to support
/// better observability and testing.
pub struct Plan {
    /// sagas that need to be recovered
    needs_recovery: BTreeMap<SagaId, nexus_db_model::Saga>,

    /// sagas that were found in the database to be in-progress, but that don't
    /// need to be recovered because they are either already running or have
    /// actually finished
    skipped_running: BTreeSet<SagaId>,

    /// sagas that we infer have finished because they were missing from two
    /// consecutive database queries for in-progress sagas (with no intervening
    /// message indicating that they had been started)
    inferred_done: BTreeSet<SagaId>,

    /// sagas that may be done, but we can't tell yet.  These are sagas where we
    /// previously had them running in this process and the database state now
    /// says that they're not running, but the database snapshot was potentially
    /// from before the time that the saga started, so we cannot tell yet
    /// whether the saga finished or just started.  We'll be able to tell during
    /// the next pass and if it's done at that point then these sagas will move
    /// to `inferred_done`.
    maybe_done: BTreeSet<SagaId>,
}

impl Plan {
    /// For a given saga recovery pass, determine what to do with each found
    /// saga
    ///
    /// This function accepts:
    ///
    /// * `previously_maybe_done`: a list of sagas that we determined last time
    ///   might be done, but we wouldn't be able to tell until this pass
    /// * `sagas_started`: a set of sagas that have started or resumed running
    ///   in this process
    /// * `running_sagas_found`: a list of sagas that the database reports
    ///   in-progress
    ///
    /// It determines:
    ///
    /// * which in-progresss sagas we don't need to do anything about because
    ///   they're already running in this process (those sagas that are in both
    ///   `sagas_started` and `running_sagas_found`)
    /// * which sagas need to be recovered (those sagas in `running_sagas_found`
    ///   but not in `sagas_started`)
    /// * which sagas can be removed from `sagas_started` because they have
    ///   finished (those in `previously_maybe_done` and *not* in
    ///   `running_sagas_found`)
    pub fn new(
        log: &slog::Logger,
        rest_state: &RestState,
        mut running_sagas_found: BTreeMap<SagaId, nexus_db_model::Saga>,
    ) -> Plan {
        let mut builder = PlanBuilder::new(log);
        let sagas_started = &rest_state.sagas_started;
        let previously_maybe_done = &rest_state.remove_next;

        // First of all, remove finished sagas from our "ignore" set.
        //
        // `previously_maybe_done` was computed the last time we ran and
        // contains sagas that either just started or already finished.  We
        // couldn't really tell.  All we knew is that they were running
        // in-memory but were not included in our database query for in-progress
        // sagas.  At this point, though, we've done a second database query for
        // in-progress sagas.  Any items that aren't in that list either cannot
        // still be running, so we can safely remove them from our ignore set.
        for saga_id in previously_maybe_done {
            if !running_sagas_found.contains_key(saga_id) {
                builder.saga_infer_done(*saga_id);
            }
        }

        // Figure out which of the candidate sagas can clearly be skipped.
        // Correctness here requires that the caller has already updated the set
        // of sagas that we're ignoring to include any that may have been
        // created up to the beginning of the database query.  Since we now have
        // the list of sagas that were not-finished in the database, we can
        // compare these two sets.
        for running_saga_id in sagas_started.keys() {
            match running_sagas_found.remove(running_saga_id) {
                None => {
                    // If this saga is in `previously_maybe_done`, then we
                    // processed it above already.  We know it's done.
                    //
                    // Otherwise, the saga is in the ignore set, but not the
                    // database list of running sagas.  It's possible that the
                    // saga has simply finished.  And if the saga is definitely
                    // not running any more, then we can remove it from the
                    // ignore set.  This is important to keep that set from
                    // growing without bound.
                    //
                    // But it's also possible that the saga started immediately
                    // after the database query's snapshot, in which case we
                    // don't really know if it's still running.
                    //
                    // The way to resolve this is to do another database query
                    // for unfinished sagas.  If it's not in that list, the saga
                    // must have finished.  Rather than do that now, we'll just
                    // keep track of this list and take care of it in the next
                    // activation.
                    if !previously_maybe_done.contains(running_saga_id) {
                        builder.saga_maybe_done(*running_saga_id)
                    }
                }

                Some(_found_saga) => {
                    // The saga is in the ignore set and the database list of
                    // running sagas.  It may have been created in the lifetime
                    // of this program or we may have recovered it previously,
                    // but either way, we don't have to do anything else with
                    // this one.
                    builder.saga_recovery_not_needed(*running_saga_id);
                }
            }
        }

        // Whatever's left in `running_sagas_found` at this point was found in
        // the database list of running sagas but is not in the ignore set.  We
        // must recover it.  (It's not possible that we already did recover it
        // because we would have added it to our ignore set.  It's not possible
        // that it was newly started because the starter sends a message to add
        // this to the ignore set (and waits for it to make it to the channel)
        // before writing the database record, and we read everything off that
        // channel and added it to the set before calling this function.
        //
        // Load and resume all these sagas serially.  Too much parallelism here
        // could overload the database.  It wouldn't buy us much anyway to
        // parallelize this since these operations should generally be quick,
        // and there shouldn't be too many sagas outstanding, and Nexus has
        // already crashed so they've experienced a bit of latency already.
        for (saga_id, saga) in running_sagas_found.into_iter() {
            builder.saga_recovery_needed(saga_id, saga);
        }

        builder.build()
    }

    /// Iterate over the sagas that need to be recovered
    pub fn sagas_needing_recovery(
        &self,
    ) -> impl Iterator<Item = (&SagaId, &nexus_db_model::Saga)> + '_ {
        self.needs_recovery.iter()
    }

    /// Iterate over the sagas that were inferred to be done
    pub fn sagas_inferred_done(&self) -> impl Iterator<Item = SagaId> + '_ {
        self.inferred_done.iter().copied()
    }

    /// Iterate over the sagas that should be checked on the next pass to see if
    /// they're done
    pub fn sagas_maybe_done(&self) -> impl Iterator<Item = SagaId> + '_ {
        self.maybe_done.iter().copied()
    }

    pub fn nskipped(&self) -> usize {
        self.skipped_running.len()
    }

    pub fn ninferred_done(&self) -> usize {
        self.inferred_done.len()
    }
}

/// Internal helper used to construct `Plan`
struct PlanBuilder<'a> {
    log: &'a slog::Logger,
    needs_recovery: BTreeMap<SagaId, nexus_db_model::Saga>,
    skipped_running: BTreeSet<SagaId>,
    inferred_done: BTreeSet<SagaId>,
    maybe_done: BTreeSet<SagaId>,
}

impl<'a> PlanBuilder<'a> {
    /// Begin building a `Plan`
    pub fn new(log: &'a slog::Logger) -> PlanBuilder {
        PlanBuilder {
            log,
            needs_recovery: BTreeMap::new(),
            skipped_running: BTreeSet::new(),
            inferred_done: BTreeSet::new(),
            maybe_done: BTreeSet::new(),
        }
    }

    /// Turn this into a `Plan`
    pub fn build(self) -> Plan {
        Plan {
            needs_recovery: self.needs_recovery,
            skipped_running: self.skipped_running,
            inferred_done: self.inferred_done,
            maybe_done: self.maybe_done,
        }
    }

    /// Record that this saga appears to be done, based on it being missing from
    /// two different database queries for in-progress sagas with no intervening
    /// indication that a saga with this id was started in the meantime
    pub fn saga_infer_done(&mut self, saga_id: SagaId) {
        info!(
            self.log,
            "found saga that appears to be done \
             (missing from two database listings)";
            "saga_id" => %saga_id
        );
        assert!(!self.needs_recovery.contains_key(&saga_id));
        assert!(!self.skipped_running.contains(&saga_id));
        assert!(!self.maybe_done.contains(&saga_id));
        assert!(self.inferred_done.insert(saga_id));
    }

    /// Record that no action is needed for this saga in this recovery pass
    /// because it appears to already be running
    pub fn saga_recovery_not_needed(&mut self, saga_id: SagaId) {
        debug!(
            self.log,
            "found saga that can be ignored (already running)";
            "saga_id" => %saga_id,
        );
        assert!(!self.needs_recovery.contains_key(&saga_id));
        assert!(!self.inferred_done.contains(&saga_id));
        assert!(!self.maybe_done.contains(&saga_id));
        assert!(self.skipped_running.insert(saga_id));
    }

    /// Record that this saga might be done, but we won't be able to tell for
    /// sure until we complete the next recovery pass
    ///
    /// This sounds a little goofy but there's a race in comparing what our
    /// in-memory state reports is running vs. what's in the database.  Our
    /// solution is to only consider sagas done that are missing for two
    /// consecutive database queries with no intervening report that a saga with
    /// that id has just started.
    pub fn saga_maybe_done(&mut self, saga_id: SagaId) {
        debug!(
            self.log,
            "found saga that may be done (will be sure on the next pass)";
            "saga_id" => %saga_id
        );
        assert!(!self.needs_recovery.contains_key(&saga_id));
        assert!(!self.skipped_running.contains(&saga_id));
        assert!(!self.inferred_done.contains(&saga_id));
        assert!(self.maybe_done.insert(saga_id));
    }

    /// Record that this saga needs to be recovered, based on it being "in
    /// progress" according to the database but not yet resumed in this process
    pub fn saga_recovery_needed(
        &mut self,
        saga_id: SagaId,
        saga: nexus_db_model::Saga,
    ) {
        info!(
            self.log,
            "found saga that needs to be recovered";
            "saga_id" => %saga_id
        );
        assert!(!self.skipped_running.contains(&saga_id));
        assert!(!self.inferred_done.contains(&saga_id));
        assert!(!self.maybe_done.contains(&saga_id));
        assert!(self.needs_recovery.insert(saga_id, saga).is_none());
    }
}

/// Summarizes the results of executing a single saga recovery pass
///
/// This is constructed by the saga recovery background task (in
/// `recovery_execute()`) via [`SagaRecoveryDoneBuilder::new()`].  This seems a
/// little overboard for such an internal structure but it helps separate
/// concerns, particularly when it comes to testing.
pub struct Execution {
    /// list of sagas that were successfully recovered
    pub succeeded: Vec<RecoverySuccess>,
    /// list of sagas that failed to be recovered
    pub failed: Vec<RecoveryFailure>,
}

impl Execution {
    /// Iterate over the sagas that were successfully recovered during this pass
    pub fn sagas_recovered_successfully(
        &self,
    ) -> impl Iterator<Item = SagaId> + '_ {
        self.succeeded.iter().map(|s| s.saga_id)
    }

    pub fn into_results(self) -> (Vec<RecoverySuccess>, Vec<RecoveryFailure>) {
        (self.succeeded, self.failed)
    }
}

pub struct ExecutionBuilder {
    in_progress: BTreeMap<SagaId, slog::Logger>,
    succeeded: Vec<RecoverySuccess>,
    failed: Vec<RecoveryFailure>,
}

impl ExecutionBuilder {
    pub fn new() -> ExecutionBuilder {
        ExecutionBuilder {
            in_progress: BTreeMap::new(),
            succeeded: Vec::new(),
            failed: Vec::new(),
        }
    }

    pub fn build(self) -> Execution {
        assert!(
            self.in_progress.is_empty(),
            "attempted to build execution result while some recoveries are \
            still in progress"
        );
        Execution { succeeded: self.succeeded, failed: self.failed }
    }

    /// Record that we've started recovering this saga
    pub fn saga_recovery_start(
        &mut self,
        saga_id: SagaId,
        saga_logger: slog::Logger,
    ) {
        info!(&saga_logger, "recovering saga: start");
        assert!(self.in_progress.insert(saga_id, saga_logger).is_none());
    }

    /// Record that we've successfully recovered this saga
    pub fn saga_recovery_success(&mut self, saga_id: SagaId) {
        let saga_logger = self
            .in_progress
            .remove(&saga_id)
            .expect("recovered saga should have previously started");
        info!(saga_logger, "recovered saga");
        self.succeeded.push(RecoverySuccess { time: Utc::now(), saga_id });
    }

    /// Record that we failed to recover this saga
    pub fn saga_recovery_failure(&mut self, saga_id: SagaId, error: &Error) {
        let saga_logger = self
            .in_progress
            .remove(&saga_id)
            .expect("recovered saga should have previously started");
        error!(saga_logger, "failed to recover saga"; error);
        self.failed.push(RecoveryFailure {
            time: Utc::now(),
            saga_id,
            message: InlineErrorChain::new(error).to_string(),
        });
    }
}

#[cfg(test)]
mod test {
    // XXX-dap write a stress test that furiously performs saga recovery a lot
    // of times and ensures that each saga is recovered exactly once
    use super::*;
    use crate::status;
    use crate::test::make_fake_saga;
    use crate::test::make_saga_ids;
    use omicron_test_utils::dev::test_setup_log;

    #[test]
    fn test_read_all_from_channel() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // If we send nothing on the channel, reading from it should return
        // immediately, having found nothing.
        let (numbers, disconnected) = read_all_from_channel::<u32>(&mut rx);
        assert!(numbers.is_empty());
        assert!(!disconnected);

        // Send some numbers and make sure we get them back.
        let expected_numbers = vec![1, 7, 0, 1];
        for n in &expected_numbers {
            tx.send(*n).unwrap();
        }
        let (numbers, disconnected) = read_all_from_channel(&mut rx);
        assert_eq!(expected_numbers, numbers);
        assert!(!disconnected);

        // Send some more numbers and make sure we get them back.
        let expected_numbers = vec![9, 7, 2, 0, 0, 6];
        for n in &expected_numbers {
            tx.send(*n).unwrap();
        }

        let (numbers, disconnected) = read_all_from_channel(&mut rx);
        assert_eq!(expected_numbers, numbers);
        assert!(!disconnected);

        // Send just a few more, then disconnect the channel.
        tx.send(128).unwrap();
        drop(tx);
        let (numbers, disconnected) = read_all_from_channel(&mut rx);
        assert_eq!(vec![128], numbers);
        assert!(disconnected);

        // Also exercise the trivial case where the channel is disconnected
        // before we read anything.
        let (tx, mut rx) = mpsc::unbounded_channel();
        drop(tx);
        let (numbers, disconnected) = read_all_from_channel::<u32>(&mut rx);
        assert!(numbers.is_empty());
        assert!(disconnected);
    }

    pub struct BasicPlanTestCase {
        pub plan: Plan,
        pub to_recover: Vec<SagaId>,
        pub to_skip: Vec<SagaId>,
        pub to_mark_done: Vec<SagaId>,
        pub to_mark_maybe: Vec<SagaId>,
    }

    impl BasicPlanTestCase {
        pub fn new(log: &slog::Logger) -> BasicPlanTestCase {
            let to_recover = make_saga_ids(4);
            let to_skip = make_saga_ids(3);
            let to_mark_done = make_saga_ids(2);
            let to_mark_maybe = make_saga_ids(1);

            info!(log, "test setup";
                "to_recover" => ?to_recover,
                "to_skip" => ?to_skip,
                "to_mark_done" => ?to_mark_done,
                "to_mark_maybe" => ?to_mark_maybe,
            );

            let mut plan_builder = PlanBuilder::new(log);
            for saga_id in &to_recover {
                plan_builder
                    .saga_recovery_needed(*saga_id, make_fake_saga(*saga_id));
            }
            for saga_id in &to_skip {
                plan_builder.saga_recovery_not_needed(*saga_id);
            }
            for saga_id in &to_mark_done {
                plan_builder.saga_infer_done(*saga_id);
            }
            for saga_id in &to_mark_maybe {
                plan_builder.saga_maybe_done(*saga_id);
            }
            let plan = plan_builder.build();

            BasicPlanTestCase {
                plan,
                to_recover,
                to_skip,
                to_mark_done,
                to_mark_maybe,
            }
        }
    }

    #[test]
    fn test_plan_basic() {
        let logctx = test_setup_log("saga_recovery_plan_basic");

        // Trivial initial case
        let plan_builder = PlanBuilder::new(&logctx.log);
        let plan = plan_builder.build();
        assert_eq!(0, plan.sagas_needing_recovery().count());
        assert_eq!(0, plan.sagas_inferred_done().count());
        assert_eq!(0, plan.sagas_maybe_done().count());

        // Basic case
        let BasicPlanTestCase {
            plan,
            to_recover,
            to_skip: _,
            to_mark_done,
            to_mark_maybe,
        } = BasicPlanTestCase::new(&logctx.log);

        let found_to_recover =
            plan.sagas_needing_recovery().collect::<Vec<_>>();
        assert_eq!(to_recover.len(), found_to_recover.len());
        for (expected_saga_id, (found_saga_id, found_saga_record)) in
            to_recover.into_iter().zip(found_to_recover.into_iter())
        {
            assert_eq!(expected_saga_id, *found_saga_id);
            assert_eq!(expected_saga_id, found_saga_record.id.0);
            assert_eq!("dummy", found_saga_record.name);
        }
        assert_eq!(
            to_mark_done,
            plan.sagas_inferred_done().collect::<Vec<_>>(),
        );
        assert_eq!(to_mark_maybe, plan.sagas_maybe_done().collect::<Vec<_>>(),);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_execution_basic() {
        let logctx = test_setup_log("saga_recovery_execution_basic");

        // Trivial initial case
        let plan_builder = PlanBuilder::new(&logctx.log);
        let plan = plan_builder.build();
        let execution_builder = ExecutionBuilder::new();
        let execution = execution_builder.build();

        assert_eq!(0, execution.sagas_recovered_successfully().count());
        let last_pass = status::LastPassSuccess::new(&plan, &execution);
        assert_eq!(0, last_pass.nfound);
        assert_eq!(0, last_pass.nrecovered);
        assert_eq!(0, last_pass.nfailed);
        assert_eq!(0, last_pass.nskipped);
        assert_eq!(0, last_pass.nremoved);

        // Test a non-trivial ExecutionDone
        let BasicPlanTestCase {
            plan,
            mut to_recover,
            to_skip,
            to_mark_done,
            to_mark_maybe: _,
        } = BasicPlanTestCase::new(&logctx.log);
        let mut execution_builder = ExecutionBuilder::new();
        assert!(to_recover.len() >= 3, "someone changed the test case");

        // Start recovery backwards, just to make sure there's not some implicit
        // dependency on the order.  (We could shuffle, but then the test would
        // be non-deterministic.)
        for saga_id in to_recover.iter().rev() {
            execution_builder.saga_recovery_start(*saga_id, logctx.log.clone());
        }

        // "Finish" recovery, in yet a different order (for the same reason as
        // above).  We want to test the success and failure cases.
        //
        // Act like:
        // - recovery for the last saga failed
        // - recovery for the other sagas completes successfully
        to_recover.rotate_left(2);
        for (i, saga_id) in to_recover.iter().enumerate() {
            if i == to_recover.len() - 1 {
                execution_builder.saga_recovery_failure(
                    *saga_id,
                    &Error::internal_error("test error"),
                );
            } else {
                execution_builder.saga_recovery_success(*saga_id);
            }
        }

        let execution = execution_builder.build();
        assert_eq!(
            to_recover.len() - 1,
            execution.sagas_recovered_successfully().count()
        );
        let last_pass = status::LastPassSuccess::new(&plan, &execution);
        assert_eq!(to_recover.len() + to_skip.len(), last_pass.nfound);
        assert_eq!(to_recover.len() - 1, last_pass.nrecovered);
        assert_eq!(1, last_pass.nfailed);
        assert_eq!(to_skip.len(), last_pass.nskipped);
        assert_eq!(to_mark_done.len(), last_pass.nremoved);

        logctx.cleanup_successful();
    }
}
