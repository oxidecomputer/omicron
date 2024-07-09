// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap working here
// As I write this now, there's one compile error left: we're incorrectly trying
// to store a Future into a map that's specified to hold timestamps.  This is an
// artifact of combining two pre-existing hunks of code: one tracked all sagas
// recovered and stored timestamps; the other tracked the sagas recovered *in
// one pass* and stored the completion Futures.  The completion Futures are only
// used for the test suite.  So the answer here is probably to refactor some of
// this stuff to expose smaller pieces for use by the test suite.
//
// That's an important next step anyway.  I need to take a pass through
// `activate()` and all the functions that it calls to figure out how to
// decompose it into smaller pieces, particularly for testing, but also in hopes
// that it'll be more obviously correct.  Some ideas:
// - separate out planning from execution?  i.e., compute which sagas are
//   to be recovered/skipped/etc. with a helper that we can test in isolation.
//   This is a little tricky because recovery *is* one of those steps needed to
//   determine if something succeeded or failed.
// - create a StatusBuilder where we can report what we're doing?  This seems
//   kind of hard though
//
// XXX-dap at the end, verify:
// - counters (maybe plumb these into Oximeter?)
// - task status reported by omdb
// - log entries
// XXX-dap TODO-coverage everything here

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

// XXX-dap TODO-doc
// XXX-dap omdb
#[derive(Clone, Serialize)]
pub struct SagaRecoveryTaskStatus {
    recent_recoveries: DebuggingHistory<RecoverySuccess>,
    recent_failures: DebuggingHistory<RecoveryFailure>,
    last_pass: LastPass,
}

// XXX-dap TODO-doc
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

// XXX-dap TODO-doc
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

    async fn recovery_execute(
        &self,
        bgtask_log: &slog::Logger,
        plan: &SagaRecoveryPlan,
    ) -> SagaExecutionDone {
        let mut builder = SagaExecutionDoneBuilder::new(bgtask_log);

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
            // It's critical that we do this *after* having fetched the
            // candidate sagas from the database.  It's okay if one of these
            // newly-created sagas doesn't show up in the candidate list
            // (because it hadn't actually started at the point where we fetched
            // the candidate list).  By contrast, if we did this step before
            // fetching candidates, and a saga was immediately created and
            // showed up in our candidate list, we'd erroneously conclude that
            // it needed to be recovered.
            update_sagas_started(
                &mut self.sagas_to_ignore,
                &mut self.sagas_started_rx,
            );

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
            self.activate_finish(last_pass)
        }
        .boxed()
    }
}

struct SagaRecoveryPlan {
    needs_recovery: BTreeMap<steno::SagaId, nexus_db_model::Saga>,
    skipped_running: BTreeSet<steno::SagaId>,
    inferred_done: BTreeSet<steno::SagaId>,
    maybe_done: BTreeSet<steno::SagaId>,
}

impl<'a> SagaRecoveryPlan {
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

                Some(found_saga) => {
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

struct SagaRecoveryPlanBuilder<'a> {
    log: &'a slog::Logger,
    needs_recovery: BTreeMap<steno::SagaId, nexus_db_model::Saga>,
    skipped_running: BTreeSet<steno::SagaId>,
    inferred_done: BTreeSet<steno::SagaId>,
    maybe_done: BTreeSet<steno::SagaId>,
}

impl<'a> SagaRecoveryPlanBuilder<'a> {
    pub fn new(log: &'a slog::Logger) -> SagaRecoveryPlanBuilder {
        SagaRecoveryPlanBuilder {
            log,
            needs_recovery: BTreeMap::new(),
            skipped_running: BTreeSet::new(),
            inferred_done: BTreeSet::new(),
            maybe_done: BTreeSet::new(),
        }
    }

    pub fn build(self) -> SagaRecoveryPlan {
        SagaRecoveryPlan {
            needs_recovery: self.needs_recovery,
            skipped_running: self.skipped_running,
            inferred_done: self.inferred_done,
            maybe_done: self.maybe_done,
        }
    }

    pub fn saga_infer_done(&mut self, saga_id: steno::SagaId) {
        // XXX-dap log entry
        // XXX-dap panic if already present
        self.inferred_done.insert(saga_id);
    }
    pub fn saga_recovery_not_needed(
        &mut self,
        saga_id: steno::SagaId,
        reason: &'static str,
    ) {
        // XXX-dap log entry
        // XXX-dap panic if already present
        self.skipped_running.insert(saga_id);
    }

    pub fn saga_recovery_maybe_done(&mut self, saga_id: steno::SagaId) {
        // XXX-dap log entry
        // XXX-dap panic if already present
        self.maybe_done.insert(saga_id);
    }

    pub fn saga_recovery_needed(
        &mut self,
        saga_id: steno::SagaId,
        saga: nexus_db_model::Saga,
    ) {
        // XXX-dap log entry
        // XXX-dap panic if already present
        self.needs_recovery.insert(saga_id, saga);
    }
}

struct SagaExecutionDone {
    succeeded: Vec<RecoverySuccess>,
    failed: Vec<RecoveryFailure>,
    // XXX-dap only under cfg(test)
    completion_futures: Vec<BoxFuture<'static, Result<(), Error>>>,
}

impl SagaExecutionDone {
    pub fn to_last_pass_result(&self) -> LastPassSuccess {
        // XXX-dap
        todo!();
    }
}

struct SagaExecutionDoneBuilder<'a> {
    log: &'a slog::Logger,
    in_progress: BTreeMap<steno::SagaId, slog::Logger>,
    succeeded: Vec<RecoverySuccess>,
    failed: Vec<RecoveryFailure>,
    completion_futures: Vec<BoxFuture<'static, Result<(), Error>>>,
}

impl<'a> SagaExecutionDoneBuilder<'a> {
    pub fn new(log: &'a slog::Logger) -> SagaExecutionDoneBuilder<'a> {
        SagaExecutionDoneBuilder {
            log,
            in_progress: BTreeMap::new(),
            succeeded: Vec::new(),
            failed: Vec::new(),
            completion_futures: Vec::new(),
        }
    }

    pub fn build(self) -> SagaExecutionDone {
        assert!(
            self.in_progress.is_empty(),
            "attempted to build execution result while some recoveries are \
            still in progress"
        );
        SagaExecutionDone {
            succeeded: self.succeeded,
            failed: self.failed,
            completion_futures: self.completion_futures,
        }
    }

    pub fn saga_recovery_start(
        &mut self,
        saga_id: steno::SagaId,
        saga_logger: slog::Logger,
    ) {
        // XXX-dap log entry
        self.in_progress.insert(saga_id, saga_logger);
    }

    pub fn saga_recovery_success(
        &mut self,
        saga_id: steno::SagaId,
        completion_future: BoxFuture<'static, Result<(), Error>>,
    ) {
        // XXX-dap log entry
        self.in_progress.remove(&saga_id);
        self.succeeded.push(RecoverySuccess { time: Utc::now(), saga_id });
        self.completion_futures.push(completion_future);
    }

    pub fn saga_recovery_failure(
        &mut self,
        saga_id: steno::SagaId,
        error: &Error,
    ) {
        // XXX-dap log entry
        self.in_progress.remove(&saga_id);
        self.failed.push(RecoveryFailure {
            time: Utc::now(),
            saga_id,
            message: InlineErrorChain::new(error).to_string(),
        });
    }
}

// Helpers

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

fn update_sagas_started(
    set: &mut BTreeSet<steno::SagaId>,
    rx: &mut mpsc::UnboundedReceiver<steno::SagaId>,
) {
    let (new_sagas, disconnected) = read_all_from_channel(rx);
    // XXX-dap warn on disconnected
    for saga_id in new_sagas {
        // XXX-dap log message (reason: "received start message")
        // XXX-dap panic if needed
        set.insert(saga_id);
    }
}

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

// XXX-dap TODO-coverage
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
