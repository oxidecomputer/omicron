// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap see other XXXs -- particularly: updating global state (including
// debug state but also critical state) based on what happened during planning
// and execution
// XXX-dap consider breaking into a separate directory with its own submodules
// XXX-dap at the end, verify:
// - counters (maybe plumb these into Oximeter?)
// - task status reported by omdb
// - log entries
// XXX-dap TODO-coverage everything here
// XXX-dap omdb support
// XXX-dap TODO-doc everything here

//! Saga recovery
//!
//! ## Review of distributed sagas
//!
//! Nexus uses distributed sagas via [`steno`] to manage multi-step operations
//! that have their own unwinding or cleanup steps.  While executing sagas,
//! critical state is durably stored in the **saga log** such that after a
//! crash, the saga can be resumed while maintaining certain guarantees:
//!
//! - During normal execution, each **action** will be executed at least once.
//! - If an action B depends on action A, then once B has started, A will not
//!   run again.
//! - Once any action has failed, the saga is **unwound**, meaning that the undo
//!   actions are executed for any action that has successfully completed.
//! - The saga will not come to rest until one of these three things has
//!   happened:
//!   1. All actions complete successfully.  This is the normal case of saga
//!      completion.
//!   2. Any number of actions complete successfully, at least one action
//!      failed, and the undo actions complete successfully for any actions that
//!      *did* run.  This is the normal case of clean saga failure where
//!      intuitively the state of the world is unwound to match whatever it was
//!      before the saga ran.
//!   3. Any number of actions complete successfully, at least one action
//!      failed, and at least one undo action also failed.  This is a nebulous
//!      "stuck" state where the world may be partially changed by the saga.
//!
//! There's more to all this (see the Steno docs), but the important thing here
//! is that the persistent state is critical for ensuring these properties
//! across a Nexus crash.  The process of resuming in-progress sagas after a
//! crash is called **saga recovery**.  Fortunately, Steno handles the details
//! of those constraints.  All we have to do is provide Steno with the
//! persistent state of any sagas that it needs to resume.
//!
//!
//! ## Saga recovery and persistent state
//!
//! Everything needed to recover a saga is stored in:
//!
//! 1. a **saga** record, which is mostly immutable
//! 2. the **saga log**, an append-only description of exactly what happened
//!    during execution
//!
//! Responsibility for persisting this state is divided across Steno and Nexus:
//!
//! 1. Steno tells its consumer (Nexus) precisely what information needs to be
//!    stored and when.  It does this by invoking methods on the `SecStore`
//!    trait at key points in the saga's execution.  Steno does not care how
//!    this information is stored or where it is stored.
//!
//! 2. Nexus serializes the given state and stores it into CockroachDB using
//!    the `saga` and `saga_node_event` tables.
//!
//! After a crash, Nexus is then responsible for:
//!
//! 1. Identifying what sagas were in progress before the crash,
//! 2. Loading all the information about them from the database (namely, the
//!    `saga` record and the full saga log in the form of records from the
//!    `saga_node_event` table), and
//! 3. Providing all this information to Steno so that it can resume running the
//!    saga.
//!
//!
//! ## Saga recovery: not just at startup
//!
//! So far, this is fairly straightforward.  What makes it tricky is that there
//! are situations where we want to carry out saga recovery after Nexus has
//! already started and potentially recovered other sagas and started its own
//! sagas.  Specifically, when a Nexus instance gets **expunged** (removed
//! permanently), it may have outstanding sagas that need to be re-assigned to
//! another Nexus instance, which then needs to resume them.  To do this, we run
//! saga recovery in a Nexus background task so that it runs both periodically
//! and on-demand when activated.  (This approach is also useful for other
//! reasons, like retrying recovery for sagas whose recovery failed due to
//! transient errors.)
//!
//! Why does this make things tricky?  When Nexus goes to identify what sagas
//! it needs to recover, it lists sagas that are (1) assigned to it (as opposed
//! to a different Nexus) and (2) not yet finished.  But that could include
//! sagas in one of three groups:
//!
//! 1. Sagas from a previous Nexus [Unix process] lifetime that have not yet
//!    been recovered in this lifetime.  These **should** be recovered.
//! 2. Sagas from a previous Nexus [Unix process] lifetime that have already
//!    been recovered in this lifetime.  These **should not** be recovered.
//! 3. Sagas that were created in this Nexus lifetime.  These **should not** be
//!    recovered.
//!
//! There are a bunch of ways to attack this problem.  We do it by keeping track
//! in-memory of the set of sagas that might be running in the current process
//! and then ignoring those when we do recovery.  Even this is easier said than
//! done!  It's easy enough to insert new sagas into the set whenever a saga is
//! successfully recovered as well as any time a saga is created for the first
//! time (though that requires a structure that's modifiable from multiple
//! different contexts).  But to avoid this set growing unbounded, we should
//! remove entries when a saga finishes running.  When exactly can we do that?
//! We have to be careful of the intrinsic race between when the recovery
//! process queries the database to list candidate sagas for recovery (i.e.,
//! unfinished sagas assigned to this Nexus) and when it checks the set of sagas
//! that should be ignored.  Suppose a saga is running, the recovery process
//! finds it, then the saga finishes, it gets removed from the set, and then the
//! recovery process checks the set.  We'll think it wasn't running and start it
//! again -- very bad.  We can't remove anything from the set until we know that
//! the saga recovery task _doesn't_ have a stale list of candidate sagas to be
//! recovered.
//!
//! This constraint suggests the solution: the set will be owned and managed
//! entirely by the task that's doing saga recovery.  We'll use a channel to
//! trigger inserts when sagas are created elsewhere in Nexus.  What about
//! deletes?  The recovery process can actually figure out on its own when a
//! saga can be removed: if a saga was _not_ in the list of candidates to be
//! recovered, then that means it's finished, and that means it can be deleted
//! from the set.  Care must be taken to process things in the right order, but
//! in the end it's pretty simple: the recovery process first fetches the
//! candidate list of sagas, then processes all insertions that have come in
//! over the channel and adds them to the set, then compares the set against the
//! candidate list.  Sagas in the set and not in the list can be removed.  Sagas
//! in the list and not in the set must be recovered.  Sagas in both the set and
//! the list may or may not still be running, but they were running at some
//! point during this recovery pass and they can be safely ignored until the
//! next pass.

use crate::app::background::BackgroundTask;
use crate::app::sagas::ActionRegistry;
use crate::saga_interface::SagaContext;
use crate::Nexus;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use serde::Serialize;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;
use steno::SagaId;
use tokio::sync::mpsc;
use uuid::Uuid;

// These values are chosen to be large enough to likely cover the complete
// history of saga recoveries, successful and otherwise.  They just need to be
// finite so that this system doesn't use an unbounded amount of memory.
/// Maximum number of successful recoveries to keep track of for debugging
const N_SUCCESS_SAGA_HISTORY: usize = 128;
/// Maximum number of recent failures to keep track of for debugging
const N_FAILED_SAGA_HISTORY: usize = 128;

/// Background task that recovers sagas assigned to this Nexus
///
/// Normally, this task only does anything of note once, when Nexus starts up.
/// However, it runs periodically and can be activated explicitly for the rare
/// case when a saga has been re-assigned to this Nexus (e.g., because some
/// other Nexus has been expunged) and to handle retries for sagas whose
/// previous recovery failed.
pub struct SagaRecovery {
    datastore: Arc<DataStore>,
    /// Unique identifier for this Saga Execution Coordinator
    ///
    /// This always matches the Nexus id.
    sec_id: db::SecId,
    /// OpContext used for saga recovery
    saga_recovery_opctx: OpContext,
    nexus: Arc<Nexus>,
    sec_client: Arc<steno::SecClient>,
    registry: Arc<ActionRegistry>,

    // for keeping track of what sagas are currently outstanding
    // XXX-dap could be BTreeMap of some object saying created/resumed and when
    sagas_to_ignore: BTreeSet<steno::SagaId>,
    sagas_started_rx: mpsc::UnboundedReceiver<steno::SagaId>,
    remove_next: Vec<steno::SagaId>,

    // for status reporting
    recent_recoveries: DebuggingHistory<RecoverySuccess>,
    recent_failures: DebuggingHistory<RecoveryFailure>,
    last_pass: LastPass,
}

#[derive(Clone, Serialize)]
pub struct SagaRecoveryTaskStatus {
    recent_recoveries: DebuggingHistory<RecoverySuccess>,
    recent_failures: DebuggingHistory<RecoveryFailure>,
    last_pass: LastPass,
}

#[derive(Clone, Serialize)]
pub struct RecoveryFailure {
    time: DateTime<Utc>,
    saga_id: SagaId,
    message: String,
}

#[derive(Clone, Serialize)]
pub struct RecoverySuccess {
    time: DateTime<Utc>,
    saga_id: SagaId,
}

#[derive(Clone, Serialize)]
pub enum LastPass {
    NeverStarted,
    Failed { message: String },
    Success(LastPassSuccess),
}

#[derive(Clone, Serialize)]
pub struct LastPassSuccess {
    nfound: usize,
    nrecovered: usize,
    nfailed: usize,
    nskipped: usize,
    nremoved: usize,
}

impl SagaRecovery {
    pub fn new(
        datastore: Arc<DataStore>,
        sec_id: Uuid,
        saga_recovery_opctx: OpContext,
        nexus: Arc<Nexus>,
        sec: Arc<steno::SecClient>,
        registry: Arc<ActionRegistry>,
        sagas_started_rx: mpsc::UnboundedReceiver<steno::SagaId>,
    ) -> SagaRecovery {
        SagaRecovery {
            datastore,
            sec_id: db::SecId(sec_id),
            saga_recovery_opctx,
            nexus,
            sec_client: sec,
            registry,
            sagas_started_rx,
            sagas_to_ignore: BTreeSet::new(),
            remove_next: Vec::new(),
            recent_recoveries: DebuggingHistory::new(N_SUCCESS_SAGA_HISTORY),
            recent_failures: DebuggingHistory::new(N_FAILED_SAGA_HISTORY),
            last_pass: LastPass::NeverStarted,
        }
    }

    fn activate_finish(&mut self, last_pass: LastPass) -> serde_json::Value {
        self.last_pass = last_pass;

        serde_json::to_value(SagaRecoveryTaskStatus {
            recent_recoveries: self.recent_recoveries.clone(),
            recent_failures: self.recent_failures.clone(),
            last_pass: self.last_pass.clone(),
        })
        .unwrap()
    }

    async fn recovery_execute<'a>(
        &self,
        bgtask_log: &'a slog::Logger,
        plan: &'a SagaRecoveryPlan,
    ) -> SagaRecoveryDone<'a> {
        let mut builder = SagaRecoveryDoneBuilder::new(bgtask_log, plan);

        for (saga_id, saga) in &plan.needs_recovery {
            let saga_log = self.nexus.log.new(o!(
                "saga_name" => saga.name.clone(),
                "saga_id" => saga_id.to_string(),
            ));

            builder.saga_recovery_start(*saga_id, saga_log.clone());
            match self.recover_one_saga(bgtask_log, &saga_log, saga).await {
                Ok(completion_future) => {
                    builder.saga_recovery_success(*saga_id, completion_future);
                }
                Err(error) => {
                    // It's essential that we not bail out early just because we
                    // hit an error here.  We want to recover all the sagas that
                    // we can.
                    builder.saga_recovery_failure(*saga_id, &error);
                }
            }
        }

        builder.build()
    }

    async fn recover_one_saga(
        &self,
        bgtask_logger: &slog::Logger,
        saga_logger: &slog::Logger,
        saga: &nexus_db_model::Saga,
    ) -> Result<BoxFuture<'static, Result<(), Error>>, Error> {
        let datastore = &self.datastore;
        let saga_id: steno::SagaId = saga.id.into();

        let log_events = datastore
            .saga_fetch_log_batched(&self.saga_recovery_opctx, saga)
            .await
            .with_internal_context(|| format!("recovering saga {saga_id}"))?;
        trace!(bgtask_logger, "recovering saga: loaded log";
            "nevents" => log_events.len(),
            "saga_id" => %saga_id,
        );

        // The extra `Arc` is a little ridiculous.  The problem is that Steno
        // expects (in `sec_client.saga_resume()`) that the user-defined context
        // will be wrapped in an `Arc`.  But we already use `Arc<SagaContext>`
        // for our type.  Hence we need two Arcs.
        let saga_context = Arc::new(Arc::new(SagaContext::new(
            self.nexus.clone(),
            saga_logger.clone(),
        )));
        let saga_completion = self
            .sec_client
            .saga_resume(
                saga_id,
                saga_context,
                saga.saga_dag.clone(),
                self.registry.clone(),
                log_events,
            )
            .await
            .map_err(|error| {
                // TODO-robustness We want to differentiate between retryable and
                // not here
                Error::internal_error(&format!(
                    "failed to resume saga: {:#}",
                    error
                ))
            })?;

        trace!(&bgtask_logger, "recovering saga: starting the saga";
            "saga_id" => %saga_id
        );
        self.sec_client.saga_start(saga_id).await.map_err(|error| {
            Error::internal_error(&format!("failed to start saga: {:#}", error))
        })?;

        Ok(async {
            saga_completion.await.kind.map_err(|e| {
                Error::internal_error(&format!("Saga failure: {:?}", e))
            })?;
            Ok(())
        }
        .boxed())
    }

    // Update persistent state based on whatever happened.
    fn recovery_finish(
        &mut self,
        plan: &SagaRecoveryPlan,
        execution: SagaRecoveryDone,
    ) {
        for saga_id in &plan.inferred_done {
            assert!(self.sagas_to_ignore.remove(saga_id));
        }

        for success in execution.succeeded {
            assert!(self.sagas_to_ignore.insert(success.saga_id));
            self.recent_recoveries.append(success);
        }

        for failure in execution.failed {
            self.recent_failures.append(failure);
        }

        self.remove_next = plan.maybe_done.iter().copied().collect();
    }
}

impl BackgroundTask for SagaRecovery {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            let datastore = &self.datastore;

            // Fetch the list of not-yet-finished sagas that are assigned to
            // this Nexus instance.
            let result = list_sagas_in_progress(
                &self.saga_recovery_opctx,
                datastore,
                self.sec_id,
            )
            .await;

            // Process any newly-created sagas, adding them to our set of sagas
            // to ignore during recovery.  We never want to try to recover a
            // saga that was created within this Nexus's lifetime.
            //
            // We do this even if the previous step failed in order to avoid
            // letting the channel queue build up.  In practice, it shouldn't
            // really matter.
            //
            // But given that we're doing this, it's critical that we do it
            // *after* having fetched the candidate sagas from the database.
            // It's okay if one of these newly-created sagas doesn't show up in
            // the candidate list (because it hadn't actually started at the
            // point where we fetched the candidate list).  The reverse is not
            // okay: if we did this step before fetching candidates, and a saga
            // was immediately created and showed up in our candidate list, we'd
            // erroneously conclude that it needed to be recovered when in fact
            // it was already running.
            let disconnected = update_sagas_started(
                log,
                &mut self.sagas_to_ignore,
                &mut self.sagas_started_rx,
            );
            if disconnected {
                warn!(
                    log,
                    "sagas_started_rx disconnected (is Nexus shutting down?)"
                );
            }

            // If we failed to fetch the list of in-progress sagas from the
            // database, bail out now.  There's nothing more we can do.
            let db_sagas = match result {
                Ok(db_sagas) => db_sagas,
                Err(error) => {
                    return self.activate_finish(LastPass::Failed {
                        message: InlineErrorChain::new(&error).to_string(),
                    })
                }
            };

            let plan = SagaRecoveryPlan::new(
                log,
                &self.remove_next,
                &mut self.sagas_to_ignore,
                db_sagas,
            );
            let execution = self.recovery_execute(log, &plan).await;
            let last_pass = LastPass::Success(execution.to_last_pass_result());
            self.recovery_finish(&plan, execution);
            self.activate_finish(last_pass)
        }
        .boxed()
    }
}

/// Describes what should happen during a particular recovery pass
///
/// This is constructed by the saga recovery background task via
/// [`SagaRecoveryPlan::new()`].  That function uses
/// [`SagaRecoveryPlanBuilder`].  This all seems a little overboard for such an
/// internal structure but it helps separate concerns, particularly when it
/// comes to testing.
///
/// This structure is also much more detailed than it needs to be to support
/// better observability and testing.
struct SagaRecoveryPlan {
    /// sagas that need to be recovered
    needs_recovery: BTreeMap<steno::SagaId, nexus_db_model::Saga>,

    /// sagas that were found in the database to be in-progress, but that don't
    /// need to be recovered because they are either already running or have
    /// actually finished
    skipped_running: BTreeSet<steno::SagaId>,

    /// sagas that we infer have finished because they were missing from two
    /// consecutive database queries for in-progress sagas (with no intervening
    /// message indicating that they had been started)
    inferred_done: BTreeSet<steno::SagaId>,

    /// sagas that may be done, but we can't tell yet.  These are sagas where we
    /// previously had them running in this process and the database state now
    /// says that they're not running, but the database snapshot was potentially
    /// from before the time that the saga started, so we cannot tell yet
    /// whether the saga finished or just started.  We'll be able to tell during
    /// the next pass and if it's done at that point then these sagas will move
    /// to `inferred_done`.
    maybe_done: BTreeSet<steno::SagaId>,
}

impl<'a> SagaRecoveryPlan {
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
        previously_maybe_done: &[steno::SagaId],
        sagas_started: &mut BTreeSet<steno::SagaId>,
        mut running_sagas_found: BTreeMap<steno::SagaId, nexus_db_model::Saga>,
    ) -> SagaRecoveryPlan {
        let mut builder = SagaRecoveryPlanBuilder::new(log);

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
        for running_saga_id in sagas_started.iter() {
            match running_sagas_found.remove(running_saga_id) {
                None => {
                    // The saga is in the ignore set, but not the database list
                    // of running sagas.  It's possible that the saga has simply
                    // finished.  And if the saga is definitely not running any
                    // more, then we can remove it from the ignore set.  This is
                    // important to keep that set from growing without bound.
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
                    builder.saga_recovery_maybe_done(*running_saga_id)
                }

                Some(_found_saga) => {
                    // The saga is in the ignore set and the database list of
                    // running sagas.  It may have been created in the lifetime
                    // of this program or we may have recovered it previously,
                    // but either way, we don't have to do anything else with
                    // this one.
                    builder.saga_recovery_not_needed(
                        *running_saga_id,
                        "already running",
                    );
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
}

/// Internal helper used to construct `SagaRecoveryPlan`
struct SagaRecoveryPlanBuilder<'a> {
    log: &'a slog::Logger,
    needs_recovery: BTreeMap<steno::SagaId, nexus_db_model::Saga>,
    skipped_running: BTreeSet<steno::SagaId>,
    inferred_done: BTreeSet<steno::SagaId>,
    maybe_done: BTreeSet<steno::SagaId>,
}

impl<'a> SagaRecoveryPlanBuilder<'a> {
    /// Begin building a `SagaRecoveryPlan`
    pub fn new(log: &'a slog::Logger) -> SagaRecoveryPlanBuilder {
        SagaRecoveryPlanBuilder {
            log,
            needs_recovery: BTreeMap::new(),
            skipped_running: BTreeSet::new(),
            inferred_done: BTreeSet::new(),
            maybe_done: BTreeSet::new(),
        }
    }

    /// Turn this into a `SagaRecoveryPlan`
    pub fn build(self) -> SagaRecoveryPlan {
        SagaRecoveryPlan {
            needs_recovery: self.needs_recovery,
            skipped_running: self.skipped_running,
            inferred_done: self.inferred_done,
            maybe_done: self.maybe_done,
        }
    }

    /// Record that this saga appears to be done, based on it being missing from
    /// two different database queries for in-progress sagas with no intervening
    /// indication that a saga with this id was started in the meantime
    pub fn saga_infer_done(&mut self, saga_id: steno::SagaId) {
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
    pub fn saga_recovery_not_needed(
        &mut self,
        saga_id: steno::SagaId,
        reason: &'static str,
    ) {
        debug!(
            self.log,
            "found saga that can be ignored";
            "saga_id" => ?saga_id,
            "reason" => reason,
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
    pub fn saga_recovery_maybe_done(&mut self, saga_id: steno::SagaId) {
        debug!(
            self.log,
            "found saga that may be done (will be sure on the next pass)";
            "saga_id" => ?saga_id
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
        saga_id: steno::SagaId,
        saga: nexus_db_model::Saga,
    ) {
        info!(
            self.log,
            "found saga that needs to be recovered";
            "saga_id" => ?saga_id
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
struct SagaRecoveryDone<'a> {
    /// plan from which this recovery was carried out
    plan: &'a SagaRecoveryPlan,
    /// list of sagas that were successfully recovered
    succeeded: Vec<RecoverySuccess>,
    /// list of sagas that failed to be recovered
    failed: Vec<RecoveryFailure>,
    /// list of Futures to enable the consumer to wait for all recovered sagas
    /// to complete
    #[cfg(test)]
    completion_futures: Vec<BoxFuture<'static, Result<(), Error>>>,
}

impl<'a> SagaRecoveryDone<'a> {
    pub fn to_last_pass_result(&self) -> LastPassSuccess {
        let plan = self.plan;
        let nfound = plan.needs_recovery.len() + plan.skipped_running.len();
        LastPassSuccess {
            nfound,
            nrecovered: self.succeeded.len(),
            nfailed: self.failed.len(),
            nskipped: plan.skipped_running.len(),
            nremoved: plan.inferred_done.len(),
        }
    }

    #[cfg(test)]
    pub fn wait_for_recovered_sagas_to_finish(
        self,
    ) -> BoxFuture<'static, Result<(), Error>> {
        async {
            futures::future::try_join_all(self.completion_futures).await?;
            Ok(())
        }
        .boxed()
    }
}

struct SagaRecoveryDoneBuilder<'a> {
    log: &'a slog::Logger,
    plan: &'a SagaRecoveryPlan,
    in_progress: BTreeMap<steno::SagaId, slog::Logger>,
    succeeded: Vec<RecoverySuccess>,
    failed: Vec<RecoveryFailure>,
    #[cfg(test)]
    completion_futures: Vec<BoxFuture<'static, Result<(), Error>>>,
}

impl<'a> SagaRecoveryDoneBuilder<'a> {
    pub fn new(
        log: &'a slog::Logger,
        plan: &'a SagaRecoveryPlan,
    ) -> SagaRecoveryDoneBuilder<'a> {
        SagaRecoveryDoneBuilder {
            log,
            plan,
            in_progress: BTreeMap::new(),
            succeeded: Vec::new(),
            failed: Vec::new(),
            #[cfg(test)]
            completion_futures: Vec::new(),
        }
    }

    pub fn build(self) -> SagaRecoveryDone<'a> {
        assert!(
            self.in_progress.is_empty(),
            "attempted to build execution result while some recoveries are \
            still in progress"
        );
        SagaRecoveryDone {
            plan: self.plan,
            succeeded: self.succeeded,
            failed: self.failed,
            #[cfg(test)]
            completion_futures: self.completion_futures,
        }
    }

    /// Record that we've started recovering this saga
    pub fn saga_recovery_start(
        &mut self,
        saga_id: steno::SagaId,
        saga_logger: slog::Logger,
    ) {
        info!(&saga_logger, "recovering saga: start");
        assert!(self.in_progress.insert(saga_id, saga_logger).is_none());
    }

    /// Record that we've successfully recovered this saga
    pub fn saga_recovery_success(
        &mut self,
        saga_id: steno::SagaId,
        completion_future: BoxFuture<'static, Result<(), Error>>,
    ) {
        let saga_logger = self
            .in_progress
            .remove(&saga_id)
            .expect("recovered saga should have previously started");
        info!(saga_logger, "recovered saga");
        self.succeeded.push(RecoverySuccess { time: Utc::now(), saga_id });
        #[cfg(test)]
        self.completion_futures.push(completion_future);
    }

    /// Record that we failed to recover this saga
    pub fn saga_recovery_failure(
        &mut self,
        saga_id: steno::SagaId,
        error: &Error,
    ) {
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

// Helpers

/// List all in-progress sagas assigned to the given SEC
async fn list_sagas_in_progress(
    opctx: &OpContext,
    datastore: &DataStore,
    sec_id: db::SecId,
) -> Result<BTreeMap<steno::SagaId, nexus_db_model::saga_types::Saga>, Error> {
    let log = &opctx.log;
    debug!(log, "listing candidate sagas for recovery");
    let result = datastore
        .saga_list_recovery_candidates_batched(&opctx, &sec_id)
        .await
        .internal_context("listing in-progress sagas for saga recovery")
        .map(|list| {
            list.into_iter()
                .map(|saga| (saga.id.into(), saga))
                .collect::<BTreeMap<steno::SagaId, nexus_db_model::Saga>>()
        });
    match &result {
        Ok(list) => {
            info!(log, "listed in-progress sagas"; "count" => list.len());
        }
        Err(error) => {
            warn!(log, "failed to list in-progress sagas"; error);
        }
    };
    result
}

/// Update `set` with entries from `rx`
///
/// For more about this, see the comments above about how we keep track of sagas
/// running in the current process
fn update_sagas_started(
    log: &slog::Logger,
    set: &mut BTreeSet<steno::SagaId>,
    rx: &mut mpsc::UnboundedReceiver<steno::SagaId>,
) -> bool {
    let (new_sagas, disconnected) = read_all_from_channel(rx);
    if disconnected {
        warn!(log, "sagas_started_rx disconnected (is Nexus shutting down?)");
    }
    for saga_id in new_sagas {
        info!(log, "observed saga start"; "saga_id" => %saga_id);
        assert!(set.insert(saga_id));
    }
    disconnected
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

#[derive(Clone, Serialize)]
struct DebuggingHistory<T> {
    size: usize,
    ring: VecDeque<T>,
}

impl<T> DebuggingHistory<T> {
    fn new(size: usize) -> DebuggingHistory<T> {
        DebuggingHistory { size, ring: VecDeque::with_capacity(size) }
    }

    fn append(&mut self, t: T) {
        let len = self.ring.len();
        assert!(len <= self.size);
        if len == self.size {
            let _ = self.ring.pop_front();
        }
        self.ring.push_back(t);
    }
}
