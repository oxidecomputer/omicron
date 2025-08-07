// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
//! 1. Sagas from a previous Nexus lifetime (i.e., a different Unix process)
//!    that have not yet been recovered in this lifetime.  These **should** be
//!    recovered.
//! 2. Sagas from a previous Nexus lifetime (i.e., a different Unix process)
//!    that have already been recovered in this lifetime.  These **should not**
//!    be recovered.
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
//! saga can be removed: if a saga that was previously in the list of candidates
//! to be recovered and is now no longer in that list, then that means it's
//! finished, and that means it can be deleted from the set.  Care must be taken
//! to process things in the right order.  These details are mostly handled by
//! the separate [`nexus_saga_recovery`] crate.

use crate::Nexus;
use crate::app::background::BackgroundTask;
use crate::app::quiesce::SagaQuiesceHandle;
use crate::app::sagas::NexusSagaType;
use crate::saga_interface::SagaContext;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use std::collections::BTreeMap;
use std::sync::Arc;
use steno::SagaId;
use steno::SagaStateView;
use tokio::sync::mpsc;

/// Helpers used for saga recovery
pub struct SagaRecoveryHelpers<N: MakeSagaContext> {
    pub recovery_opctx: OpContext,
    pub maker: N,
    pub sec_client: Arc<steno::SecClient>,
    pub registry: Arc<steno::ActionRegistry<N::SagaType>>,
    pub sagas_started_rx: mpsc::UnboundedReceiver<SagaId>,
    pub quiesce: SagaQuiesceHandle,
}

/// Background task that recovers sagas assigned to this Nexus
///
/// Normally, this task only does anything of note once, when Nexus starts up.
/// But it runs periodically and can be activated explicitly for the rare case
/// when a saga has been re-assigned to this Nexus (e.g., because some other
/// Nexus has been expunged) and to handle retries for sagas whose previous
/// recovery failed.
pub struct SagaRecovery<N: MakeSagaContext> {
    datastore: Arc<DataStore>,
    /// Unique identifier for this Saga Execution Coordinator
    ///
    /// This always matches the Nexus id.
    sec_id: db::SecId,
    /// OpContext used for saga recovery
    saga_recovery_opctx: OpContext,
    /// Quiesce state
    quiesce: SagaQuiesceHandle,

    // state required to resume a saga
    /// handle to Steno, which actually resumes the saga
    sec_client: Arc<steno::SecClient>,
    /// generates the SagaContext for the saga
    maker: N,
    /// registry of actions that we need to provide to Steno
    registry: Arc<steno::ActionRegistry<N::SagaType>>,

    // state that we use during each recovery pass
    /// channel on which we listen for sagas being started elsewhere in Nexus
    sagas_started_rx: mpsc::UnboundedReceiver<SagaId>,
    /// recovery state persisted between passes
    rest_state: nexus_saga_recovery::RestState,

    /// status reporting
    status: nexus_saga_recovery::Report,
}

impl<N: MakeSagaContext> BackgroundTask for SagaRecovery<N> {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // We don't need the future that's returned by activate_internal().
            // That's only used by the test suite.
            let _ = self.activate_internal(opctx).await;
            serde_json::to_value(&self.status).unwrap()
        }
        .boxed()
    }
}

impl<N: MakeSagaContext> SagaRecovery<N> {
    pub fn new(
        datastore: Arc<DataStore>,
        sec_id: db::SecId,
        helpers: SagaRecoveryHelpers<N>,
    ) -> SagaRecovery<N> {
        SagaRecovery {
            datastore,
            sec_id,
            quiesce: helpers.quiesce,
            saga_recovery_opctx: helpers.recovery_opctx,
            maker: helpers.maker,
            sec_client: helpers.sec_client,
            registry: helpers.registry,
            sagas_started_rx: helpers.sagas_started_rx,
            rest_state: nexus_saga_recovery::RestState::new(),
            status: nexus_saga_recovery::Report::new(),
        }
    }

    /// Invoked for each activation of the background task
    ///
    /// This internal version exists solely to expose some information about
    /// what was recovered for testing.
    async fn activate_internal(
        &mut self,
        opctx: &OpContext,
    ) -> Option<(
        BoxFuture<'static, Result<(), Error>>,
        nexus_saga_recovery::LastPassSuccess,
    )> {
        let log = &opctx.log;
        let datastore = &self.datastore;

        // Record when saga recovery starts.  This must happen before we list
        // sagas in order to track which sagas we're guaranteed to see (or not)
        // from a concurrent re-assignment of sagas.  See `SagaQuiesceHandle`
        // for details.
        // XXX-dap could return something whose Drop invokes _done() so that if
        // we bail early, we don't have to remember to call it?
        self.quiesce.saga_recovery_start();

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
        let nstarted = self
            .rest_state
            .update_started_sagas(log, &mut self.sagas_started_rx);

        match result {
            Ok(db_sagas) => {
                let plan = nexus_saga_recovery::Plan::new(
                    log,
                    &self.rest_state,
                    db_sagas,
                );
                self.recovery_check_done(log, &plan).await;
                let (execution, future) =
                    self.recovery_execute(log, &plan).await;
                self.rest_state.update_after_pass(&plan, &execution);
                let last_pass_success =
                    nexus_saga_recovery::LastPassSuccess::new(
                        &plan, &execution,
                    );
                self.status.update_after_pass(&plan, execution, nstarted);
                self.quiesce.saga_recovery_done(true);
                Some((future, last_pass_success))
            }
            Err(error) => {
                self.status.update_after_failure(&error, nstarted);
                self.quiesce.saga_recovery_done(false);
                None
            }
        }
    }

    /// Check that for each saga that we inferred was done, Steno agrees
    ///
    /// This is not strictly necessary because this should always be true.  But
    /// if for some reason it's not, that would be a serious issue and we'd want
    /// to know that.
    async fn recovery_check_done(
        &mut self,
        log: &slog::Logger,
        plan: &nexus_saga_recovery::Plan,
    ) {
        for saga_id in plan.sagas_inferred_done() {
            match self.sec_client.saga_get(saga_id).await {
                Err(_) => {
                    self.status.ntotal_sec_errors_missing += 1;
                    error!(
                        log,
                        "SEC does not know about saga that we thought \
                        had finished";
                        "saga_id" => %saga_id
                    );
                }
                Ok(saga_state) => match saga_state.state {
                    SagaStateView::Done { .. } => (),
                    _ => {
                        self.status.ntotal_sec_errors_bad_state += 1;
                        error!(
                            log,
                            "we thought saga was done, but SEC reports a \
                            different state";
                            "saga_id" => %saga_id,
                            "sec_state" => ?saga_state.state
                        );
                    }
                },
            }
        }
    }

    /// Recovers the sagas described in `plan`
    async fn recovery_execute(
        &self,
        bgtask_log: &slog::Logger,
        plan: &nexus_saga_recovery::Plan,
    ) -> (nexus_saga_recovery::Execution, BoxFuture<'static, Result<(), Error>>)
    {
        let mut builder = nexus_saga_recovery::ExecutionBuilder::new();
        let mut completion_futures = Vec::new();

        // Load and resume all these sagas serially.  Too much parallelism here
        // could overload the database.  It wouldn't buy us much anyway to
        // parallelize this since these operations should generally be quick,
        // and there shouldn't be too many sagas outstanding, and Nexus has
        // already crashed so they've experienced a bit of latency already.
        for (saga_id, saga) in plan.sagas_needing_recovery() {
            let saga_log = self.maker.make_saga_log(*saga_id, &saga.name);
            builder.saga_recovery_start(*saga_id, saga_log.clone());
            match self.recover_one_saga(bgtask_log, &saga_log, saga).await {
                Ok(completion_future) => {
                    builder.saga_recovery_success(*saga_id);
                    completion_futures.push(completion_future);
                }
                Err(error) => {
                    // It's essential that we not bail out early just because we
                    // hit an error here.  We want to recover all the sagas that
                    // we can.
                    builder.saga_recovery_failure(*saga_id, &error);
                }
            }
        }

        let future = async {
            futures::future::try_join_all(completion_futures).await?;
            Ok(())
        }
        .boxed();
        (builder.build(), future)
    }

    async fn recover_one_saga(
        &self,
        bgtask_logger: &slog::Logger,
        saga_logger: &slog::Logger,
        saga: &nexus_db_model::Saga,
    ) -> Result<BoxFuture<'static, Result<(), Error>>, Error> {
        let datastore = &self.datastore;
        let saga_id: SagaId = saga.id.into();

        let log_events = datastore
            .saga_fetch_log_batched(&self.saga_recovery_opctx, saga.id)
            .await
            .with_internal_context(|| format!("recovering saga {saga_id}"))?;
        trace!(bgtask_logger, "recovering saga: loaded log";
            "nevents" => log_events.len(),
            "saga_id" => %saga_id,
        );

        let saga_context = self.maker.make_saga_context(saga_logger.clone());
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
        let saga_completion = self
            .quiesce
            .record_saga_recovery(saga_id, &steno::SagaName::new(&saga.name))
            .saga_completion_future(saga_completion);

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

/// List all in-progress sagas assigned to the given SEC
async fn list_sagas_in_progress(
    opctx: &OpContext,
    datastore: &DataStore,
    sec_id: db::SecId,
) -> Result<BTreeMap<SagaId, nexus_db_model::saga_types::Saga>, Error> {
    let log = &opctx.log;
    debug!(log, "listing candidate sagas for recovery");
    let result = datastore
        .saga_list_recovery_candidates_batched(&opctx, sec_id)
        .await
        .internal_context("listing in-progress sagas for saga recovery")
        .map(|list| {
            list.into_iter()
                .map(|saga| (saga.id.into(), saga))
                .collect::<BTreeMap<SagaId, nexus_db_model::Saga>>()
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

/// Encapsulates the tiny bit of behavior associated with constructing a new
/// saga context
///
/// This type exists so that the rest of the `SagaRecovery` task can avoid
/// knowing directly about Nexus, which in turn allows us to test it with sagas
/// that we control.
pub trait MakeSagaContext: Send + Sync {
    type SagaType: steno::SagaType;

    fn make_saga_context(
        &self,
        log: slog::Logger,
    ) -> Arc<<Self::SagaType as steno::SagaType>::ExecContextType>;

    fn make_saga_log(&self, id: SagaId, name: &str) -> slog::Logger;
}

impl MakeSagaContext for Arc<Nexus> {
    type SagaType = NexusSagaType;
    fn make_saga_context(&self, log: slog::Logger) -> Arc<Arc<SagaContext>> {
        // The extra `Arc` is a little ridiculous.  The problem is that Steno
        // expects (in `sec_client.saga_resume()`) that the user-defined context
        // will be wrapped in an `Arc`.  But we already use `Arc<SagaContext>`
        // for our type.  Hence we need two Arcs.
        Arc::new(Arc::new(SagaContext::new(self.clone(), log)))
    }

    fn make_saga_log(&self, id: SagaId, name: &str) -> slog::Logger {
        self.log.new(o!(
            "saga_name" => name.to_owned(),
            "saga_id" => id.to_string(),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nexus_auth::authn;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::pub_test_utils::TestDatabase;
    use nexus_db_queries::db::test_utils::UnpluggableCockroachDbSecStore;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::internal_api::views::LastResult;
    use omicron_test_utils::dev::{
        self,
        poll::{CondCheckError, wait_for_condition},
    };
    use pretty_assertions::assert_eq;
    use std::sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
    };
    use steno::{
        Action, ActionContext, ActionError, ActionRegistry, DagBuilder, Node,
        SagaDag, SagaId, SagaName, SagaResult, SagaType, SecClient,
        new_action_noop_undo,
    };
    use uuid::Uuid;
    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // The following is our "saga-under-test". It's a simple two-node operation
    // that tracks how many times it has been called, and provides a mechanism
    // for detaching storage to simulate power failure (and meaningfully
    // recover).

    #[derive(Debug)]
    struct TestContext {
        log: slog::Logger,

        // Storage, and instructions on whether or not to detach it
        // when executing the first saga action.
        storage: Arc<UnpluggableCockroachDbSecStore>,
        do_unplug: AtomicBool,

        // Tracks of how many times each node has been reached.
        n1_count: AtomicU32,
        n2_count: AtomicU32,
    }

    impl TestContext {
        fn new(
            log: &slog::Logger,
            storage: Arc<UnpluggableCockroachDbSecStore>,
        ) -> Self {
            TestContext {
                log: log.clone(),
                storage,
                do_unplug: AtomicBool::new(false),

                // Counters of how many times the nodes have been invoked.
                n1_count: AtomicU32::new(0),
                n2_count: AtomicU32::new(0),
            }
        }
    }

    #[derive(Debug)]
    struct TestOp;
    impl SagaType for TestOp {
        type ExecContextType = TestContext;
    }

    impl MakeSagaContext for Arc<TestContext> {
        type SagaType = TestOp;
        fn make_saga_context(&self, _log: slog::Logger) -> Arc<TestContext> {
            self.clone()
        }

        fn make_saga_log(&self, id: SagaId, name: &str) -> slog::Logger {
            self.log.new(o!(
                "saga_name" => name.to_owned(),
                "saga_id" => id.to_string(),
            ))
        }
    }

    static ACTION_N1: LazyLock<Arc<dyn Action<TestOp>>> =
        LazyLock::new(|| new_action_noop_undo("n1_action", node_one));
    static ACTION_N2: LazyLock<Arc<dyn Action<TestOp>>> =
        LazyLock::new(|| new_action_noop_undo("n2_action", node_two));

    fn registry_create() -> Arc<ActionRegistry<TestOp>> {
        let mut registry = ActionRegistry::new();
        registry.register(Arc::clone(&ACTION_N1));
        registry.register(Arc::clone(&ACTION_N2));
        Arc::new(registry)
    }

    fn saga_object_create() -> Arc<SagaDag> {
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(Node::action("n1_out", "NodeOne", ACTION_N1.as_ref()));
        builder.append(Node::action("n2_out", "NodeTwo", ACTION_N2.as_ref()));
        let dag = builder.build().unwrap();
        Arc::new(SagaDag::new(dag, serde_json::Value::Null))
    }

    async fn node_one(ctx: ActionContext<TestOp>) -> Result<i32, ActionError> {
        let uctx = ctx.user_data();
        uctx.n1_count.fetch_add(1, Ordering::SeqCst);
        info!(&uctx.log, "ACTION: node_one");
        // If "do_unplug" is true, we detach storage.
        //
        // This prevents the SEC from successfully recording that
        // this node completed, and acts like a crash.
        if uctx.do_unplug.load(Ordering::SeqCst) {
            info!(&uctx.log, "Unplugged storage");
            uctx.storage.set_unplug(true);
        }
        Ok(1)
    }

    async fn node_two(ctx: ActionContext<TestOp>) -> Result<i32, ActionError> {
        let uctx = ctx.user_data();
        uctx.n2_count.fetch_add(1, Ordering::SeqCst);
        info!(&uctx.log, "ACTION: node_two");
        Ok(2)
    }

    // Helper function for setting up storage, SEC, and a test context object.
    fn create_storage_sec_and_context(
        log: &slog::Logger,
        db_datastore: Arc<db::DataStore>,
        sec_id: db::SecId,
    ) -> (Arc<UnpluggableCockroachDbSecStore>, SecClient, Arc<TestContext>)
    {
        let storage = Arc::new(UnpluggableCockroachDbSecStore::new(
            sec_id,
            db_datastore,
            log.new(o!("component" => "SecStore")),
        ));
        let sec_client =
            steno::sec(log.new(o!("component" => "SEC")), storage.clone());
        let uctx = Arc::new(TestContext::new(&log, storage.clone()));
        (storage, sec_client, uctx)
    }

    // Helper function to run a basic saga that we can use to see which nodes
    // ran and how many times.
    async fn run_test_saga(
        uctx: &Arc<TestContext>,
        sec_client: &SecClient,
    ) -> (SagaId, SagaResult) {
        let saga_id = SagaId(Uuid::new_v4());
        let future = sec_client
            .saga_create(
                saga_id,
                uctx.clone(),
                saga_object_create(),
                registry_create(),
            )
            .await
            .unwrap();
        sec_client.saga_start(saga_id).await.unwrap();
        (saga_id, future.await)
    }

    // Tests the basic case: recovery of a saga that appears (from its log) to
    // be still running, and which is not currently running already.  In Nexus,
    // this corresponds to the basic case where a saga was created in a previous
    // Nexus lifetime and the current process knows nothing about it.
    #[tokio::test]
    async fn test_failure_during_saga_can_be_recovered() {
        // Test setup
        let logctx =
            dev::test_setup_log("test_failure_during_saga_can_be_recovered");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_raw_datastore(&log).await;
        let db_datastore = db.datastore();
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let (storage, sec_client, uctx) =
            create_storage_sec_and_context(&log, db_datastore.clone(), sec_id);
        let sec_log = log.new(o!("component" => "SEC"));
        let opctx = OpContext::for_tests(
            log.clone(),
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );
        let saga_recovery_opctx =
            opctx.child_with_authn(authn::Context::internal_saga_recovery());

        // In order to recover a partially-created saga, we need a partial log.
        // To create one, we'll run the saga normally, but configure it to
        // unplug the datastore partway through so that the later log entries
        // don't get written.  Note that the unplugged datastore completes
        // operations successfully so that the saga will appeaer to complete
        // successfully.
        uctx.do_unplug.store(true, Ordering::SeqCst);
        let (_, result) = run_test_saga(&uctx, &sec_client).await;
        let output = result.kind.unwrap();
        assert_eq!(output.lookup_node_output::<i32>("n1_out").unwrap(), 1);
        assert_eq!(output.lookup_node_output::<i32>("n2_out").unwrap(), 2);
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Simulate a crash by terminating the SEC and creating a new one using
        // the same storage system.
        //
        // Update uctx to prevent the storage system from detaching again.
        sec_client.shutdown().await;
        let sec_client = steno::sec(sec_log, storage.clone());
        uctx.storage.set_unplug(false);
        uctx.do_unplug.store(false, Ordering::SeqCst);

        // Use our background task to recover the saga.  Observe that it re-runs
        // operations and completes.
        let sec_client = Arc::new(sec_client);
        let (_, sagas_started_rx) = tokio::sync::mpsc::unbounded_channel();
        let quiesce = SagaQuiesceHandle::new(log.clone());
        let mut task = SagaRecovery::new(
            db_datastore.clone(),
            sec_id,
            SagaRecoveryHelpers {
                recovery_opctx: saga_recovery_opctx,
                maker: uctx.clone(),
                sec_client: sec_client.clone(),
                registry: registry_create(),
                sagas_started_rx,
                quiesce,
            },
        );

        let Some((completion_future, last_pass_success)) =
            task.activate_internal(&opctx).await
        else {
            panic!("saga recovery failed");
        };

        assert_eq!(last_pass_success.nrecovered, 1);
        assert_eq!(last_pass_success.nfailed, 0);
        assert_eq!(last_pass_success.nskipped, 0);

        // Wait for the recovered saga to complete and make sure it re-ran the
        // operations that we expected it to.
        completion_future
            .await
            .expect("recovered saga to complete successfully");
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 2);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 2);

        // Test cleanup
        drop(task);
        let sec_client = Arc::try_unwrap(sec_client).unwrap();
        sec_client.shutdown().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests that a saga that has finished (as reflected in the database state)
    // does not get recovered.
    #[tokio::test]
    async fn test_successful_saga_does_not_replay_during_recovery() {
        // Test setup
        let logctx = dev::test_setup_log(
            "test_successful_saga_does_not_replay_during_recovery",
        );
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_raw_datastore(&log).await;
        let db_datastore = db.datastore();
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let (storage, sec_client, uctx) =
            create_storage_sec_and_context(&log, db_datastore.clone(), sec_id);
        let sec_log = log.new(o!("component" => "SEC"));
        let opctx = OpContext::for_tests(
            log.clone(),
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );
        let saga_recovery_opctx =
            opctx.child_with_authn(authn::Context::internal_saga_recovery());

        // Create and start a saga, which we expect to complete successfully.
        let (_, result) = run_test_saga(&uctx, &sec_client).await;
        let output = result.kind.unwrap();
        assert_eq!(output.lookup_node_output::<i32>("n1_out").unwrap(), 1);
        assert_eq!(output.lookup_node_output::<i32>("n2_out").unwrap(), 2);
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Simulate a crash by terminating the SEC and creating a new one using
        // the same storage system.
        sec_client.shutdown().await;
        let sec_client = steno::sec(sec_log, storage.clone());

        // Go through recovery.  We should not find or recover this saga.
        let sec_client = Arc::new(sec_client);
        let (_, sagas_started_rx) = tokio::sync::mpsc::unbounded_channel();
        let quiesce = SagaQuiesceHandle::new(log.clone());
        let mut task = SagaRecovery::new(
            db_datastore.clone(),
            sec_id,
            SagaRecoveryHelpers {
                recovery_opctx: saga_recovery_opctx,
                maker: uctx.clone(),
                sec_client: sec_client.clone(),
                registry: registry_create(),
                sagas_started_rx,
                quiesce,
            },
        );

        let Some((_, last_pass_success)) = task.activate_internal(&opctx).await
        else {
            panic!("saga recovery failed");
        };

        assert_eq!(last_pass_success.nrecovered, 0);
        assert_eq!(last_pass_success.nfailed, 0);
        assert_eq!(last_pass_success.nskipped, 0);

        // The nodes should not have been replayed.
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Test cleanup
        drop(task);
        let sec_client = Arc::try_unwrap(sec_client).unwrap();
        sec_client.shutdown().await;
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Verify the plumbing that exists between regular saga creation and saga
    // recovery.
    #[nexus_test(server = crate::Server)]
    async fn test_nexus_recovery(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;

        // This is tricky to do.  We're trying to make sure the plumbing is
        // hooked up so that when a saga is created, the saga recovery task
        // learns about it.  The purpose of that plumbing is to ensure that we
        // don't try to recover a task that's already running.  It'd be ideal to
        // test that directly, but we can't easily control execution well enough
        // to ensure that the background task runs while the saga is still
        // running.  However, even if we miss it (i.e., the background task only
        // runs after the saga completes successfully), there's a side effect we
        // can look for: the task should report the completed saga as "maybe
        // done".  On the next activation, it should report that it's removed a
        // saga from its internal state (because it saw that it was done).

        // Wait for the task to run once.
        let driver = nexus.background_tasks_driver.get().unwrap();
        let task_name = driver
            .tasks()
            .find(|task_name| task_name.as_str() == "saga_recovery")
            .expect("expected background task called \"saga_recovery\"");
        let first_completed = wait_for_condition(
            || async {
                let status = driver.task_status(task_name);
                let LastResult::Completed(completed) = status.last else {
                    return Err(CondCheckError::<()>::NotYet);
                };
                Ok(completed)
            },
            &std::time::Duration::from_millis(250),
            &std::time::Duration::from_secs(15),
        )
        .await
        .unwrap();

        // Make sure that it didn't find anything to do.
        let status_raw = first_completed.details;
        let status: nexus_saga_recovery::Report =
            serde_json::from_value(status_raw).unwrap();
        let nexus_saga_recovery::LastPass::Success(last_pass_success) =
            status.last_pass
        else {
            panic!("wrong last pass variant");
        };
        assert_eq!(last_pass_success.nfound, 0);
        assert_eq!(last_pass_success.nrecovered, 0);
        assert_eq!(last_pass_success.nfailed, 0);
        assert_eq!(last_pass_success.nskipped, 0);

        // Now kick off a saga -- any saga will do.  We don't even care if it
        // works or not.  In practice, it will have finished by the time this
        // call completes.
        let _ = create_project(&cptestctx.external_client, "test").await;

        // Activate the background task.  Wait for one pass.
        nexus.background_tasks.task_saga_recovery.activate();
        let _ = wait_for_condition(
            || async {
                let status = driver.task_status(task_name);
                let LastResult::Completed(completed) = status.last else {
                    panic!("task had completed before; how has it not now?");
                };
                if completed.iteration <= first_completed.iteration {
                    return Err(CondCheckError::<()>::NotYet);
                }
                Ok(completed)
            },
            &std::time::Duration::from_millis(250),
            &std::time::Duration::from_secs(15),
        )
        .await
        .unwrap();

        // Activate it again.  This should be enough for it to report having
        // removed a saga from its state.
        nexus.background_tasks.task_saga_recovery.activate();
        let last_pass_success = wait_for_condition(
            || async {
                let status = driver.task_status(task_name);
                let LastResult::Completed(completed) = status.last else {
                    panic!("task had completed before; how has it not now?");
                };

                let status: nexus_saga_recovery::Report =
                    serde_json::from_value(completed.details).unwrap();
                let nexus_saga_recovery::LastPass::Success(last_pass_success) =
                    status.last_pass
                else {
                    panic!("wrong last pass variant");
                };
                if last_pass_success.nremoved > 0 {
                    return Ok(last_pass_success);
                }

                Err(CondCheckError::<()>::NotYet)
            },
            &std::time::Duration::from_millis(250),
            &std::time::Duration::from_secs(15),
        )
        .await
        .unwrap();

        assert!(last_pass_success.nremoved > 0);
    }
}
