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

use crate::context::OpContext;
use crate::db;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::pagination::{paginated, paginated_multicolumn, Paginator};
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::ExpressionMethods;
use diesel::SelectableHelper;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::TryFutureExt;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use steno::SagaId;

/// Describes the result of [`recover()`]
pub struct SagasRecovered {
    recovered: BTreeMap<SagaId, BoxFuture<'static, Result<(), Error>>>,
    skipped: BTreeSet<SagaId>,
    failed: BTreeMap<SagaId, Error>,
}

impl SagasRecovered {
    /// Iterate over the set of sagas that were successfully recovered
    pub fn iter_recovered(&self) -> impl Iterator<Item = SagaId> + '_ {
        self.recovered.keys().copied()
    }

    /// Iterate over the set of sagas that were found but skipped
    pub fn iter_skipped(&self) -> impl Iterator<Item = SagaId> + '_ {
        self.skipped.iter().copied()
    }

    /// Iterate over the set of sagas where recovery was attempted, but failed
    pub fn iter_failed(&self) -> impl Iterator<Item = (SagaId, &Error)> + '_ {
        self.failed.iter().map(|(id, error)| (*id, error))
    }

    /// Waits for all of the successfully recovered sagas to be completed
    /// successfully.  Returns the first error, if any.
    #[cfg(test)]
    async fn wait_for_recovered_sagas_to_finish(self) -> Result<(), Error> {
        let completion_futures = self.recovered.into_values();
        futures::future::try_join_all(completion_futures).await?;
        Ok(())
    }
}

/// Recover in-progress sagas (as after a crash or restart)
///
/// More specifically, this function queries the database to list all
/// uncompleted sagas that are assigned to SEC `sec_id` and for each one:
///
/// * invokes `skip(saga_id)` to see if this saga should be skipped altogether
/// * assuming it wasn't skipped, loads the saga DAG and log from `datastore`
/// * uses [`steno::SecClient::saga_resume`] to prepare to resume execution of
///   the saga using the persistent saga log
/// * resumes execution of the saga
///
/// The function completes successfully as long it successfully identifies the
/// sagas that need to be recovered and it returns once it has attempted to load
/// and resume all sagas that were found.  The returned value can be used to
/// inspect more about what happened.
pub async fn recover<T>(
    opctx: &OpContext,
    sec_id: db::SecId,
    sec_generation: db::SecGeneration,
    skip: &(dyn Fn(SagaId) -> bool + Send + Sync),
    make_context: &(dyn Fn(&slog::Logger) -> Arc<T::ExecContextType>
          + Send
          + Sync),
    datastore: &db::DataStore,
    sec_client: &steno::SecClient,
    registry: Arc<steno::ActionRegistry<T>>,
) -> Result<SagasRecovered, Error>
where
    T: steno::SagaType,
{
    info!(&opctx.log, "start saga recovery");

    let mut recovered = BTreeMap::new();
    let mut skipped = BTreeSet::new();
    let mut failed = BTreeMap::new();

    // We do not retry any database operations here because we're being invoked
    // by a background task that will be re-activated some time later and pick
    // up anything we missed.
    // TODO-monitoring we definitely want a way to raise a big red flag if
    // saga recovery is not completing.
    let found_sagas =
        list_unfinished_sagas(&opctx, datastore, &sec_id, &sec_generation)
            .await?;

    info!(&opctx.log, "listed sagas ({} total)", found_sagas.len());

    // Load and resume all sagas in serial.  Too much parallelism here could
    // overload the database.  It wouldn't buy us much anyway to parallelize
    // this since these operations should generally be quick, and there
    // shouldn't be too many sagas outstanding, and Nexus has already crashed so
    // they've experienced a bit of latency already.
    let nfound = found_sagas.len();
    for saga in found_sagas {
        // TODO-debugging want visibility into sagas that we cannot recover for
        // whatever reason
        let saga_id: steno::SagaId = saga.id.into();
        let saga_name = saga.name.clone();
        let saga_logger = opctx.log.new(o!(
            "saga_name" => saga_name,
            "saga_id" => saga_id.to_string()
        ));

        if skip(saga_id) {
            skipped.insert(saga_id);
            debug!(&saga_logger, "recovering saga: skipped (already done)");
            continue;
        }

        info!(&saga_logger, "recovering saga: start");
        match recover_saga(
            &opctx,
            &saga_logger,
            make_context,
            datastore,
            sec_client,
            Arc::clone(&registry),
            saga,
        )
        .await
        {
            Ok(completion_future) => {
                info!(&saga_logger, "recovered saga");
                recovered.insert(saga_id, completion_future.boxed());
            }
            Err(error) => {
                // It's essential that we not bail out early just because we hit
                // an error here.  We want to recover all the sagas that we can.
                error!(
                    &saga_logger,
                    "failed to recover saga";
                    &error,
                );
                failed.insert(saga_id, error);
            }
        }
    }

    assert_eq!(recovered.len() + skipped.len() + failed.len(), nfound);
    Ok(SagasRecovered { recovered, skipped, failed })
}

/// Recovers an individual saga
///
/// This function loads the saga log and uses `sec_client` to resume execution.
///
/// This function returns a future that completes when the resumed saga
/// has completed. The saga executor will attempt to execute the saga
/// regardless of this future - it is for notification purposes only,
/// and does not need to be polled.
async fn recover_saga<'a, T>(
    opctx: &'a OpContext,
    saga_logger: &slog::Logger,
    make_context: &(dyn Fn(&slog::Logger) -> Arc<T::ExecContextType>
          + Send
          + Sync),
    datastore: &'a db::DataStore,
    sec_client: &'a steno::SecClient,
    registry: Arc<steno::ActionRegistry<T>>,
    saga: db::saga_types::Saga,
) -> Result<
    impl core::future::Future<Output = Result<(), Error>> + 'static,
    Error,
>
where
    T: steno::SagaType,
{
    let saga_id: steno::SagaId = saga.id.into();
    let log_events = load_saga_log(&opctx, datastore, &saga).await?;
    trace!(&saga_logger, "recovering saga: loaded log");
    let uctx = make_context(&saga_logger);
    let saga_completion = sec_client
        .saga_resume(saga_id, uctx, saga.saga_dag, registry, log_events)
        .await
        .map_err(|error| {
            // TODO-robustness We want to differentiate between retryable and
            // not here
            Error::internal_error(&format!(
                "failed to resume saga: {:#}",
                error
            ))
        })?;
    trace!(&saga_logger, "recovering saga: starting the saga");
    sec_client.saga_start(saga_id).await.map_err(|error| {
        Error::internal_error(&format!("failed to start saga: {:#}", error))
    })?;

    Ok(async {
        saga_completion.await.kind.map_err(|e| {
            Error::internal_error(&format!("Saga failure: {:?}", e))
        })?;
        Ok(())
    })
}

/// Queries the database to load the full log for the specified saga
async fn load_saga_log(
    opctx: &OpContext,
    datastore: &db::DataStore,
    saga: &db::saga_types::Saga,
) -> Result<Vec<steno::SagaNodeEvent>, Error> {
    // XXX-dap
    datastore.saga_load_log_batched(opctx, saga).await
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::context::OpContext;
    use crate::db::test_utils::UnpluggableCockroachDbSecStore;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use rand::seq::SliceRandom;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use steno::{
        new_action_noop_undo, Action, ActionContext, ActionError,
        ActionRegistry, DagBuilder, Node, SagaDag, SagaId, SagaName,
        SagaResult, SagaType, SecClient,
    };
    use uuid::Uuid;

    // Returns a cockroach DB, as well as a "datastore" interface (which is the
    // one more frequently used by Nexus).
    //
    // The caller is responsible for calling "cleanup().await" on the returned
    // CockroachInstance - we would normally wrap this in a drop method, but it
    // is async.
    async fn new_db(
        log: &slog::Logger,
    ) -> (dev::db::CockroachInstance, Arc<db::DataStore>) {
        let db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(db::Pool::new(log, &cfg));
        let db_datastore = Arc::new(
            db::DataStore::new(&log, Arc::clone(&pool), None).await.unwrap(),
        );
        (db, db_datastore)
    }

    // The following is our "saga-under-test". It's a simple two-node operation
    // that tracks how many times it has been called, and provides a mechanism
    // for detaching storage, to simulate power failure (and meaningfully
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

    static ACTION_N1: Lazy<Arc<dyn Action<TestOp>>> =
        Lazy::new(|| new_action_noop_undo("n1_action", node_one));
    static ACTION_N2: Lazy<Arc<dyn Action<TestOp>>> =
        Lazy::new(|| new_action_noop_undo("n2_action", node_two));

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
        sec_generation: db::SecGeneration,
    ) -> (Arc<UnpluggableCockroachDbSecStore>, SecClient, Arc<TestContext>)
    {
        let storage = Arc::new(UnpluggableCockroachDbSecStore::new(
            sec_id,
            sec_generation,
            db_datastore,
            log.new(o!("component" => "SecStore")),
        ));
        let sec_client =
            steno::sec(log.new(o!("component" => "SEC")), storage.clone());
        let uctx = Arc::new(TestContext::new(&log, storage.clone()));
        (storage, sec_client, uctx)
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
        let (mut db, db_datastore) = new_db(&log).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        // SEC generation for the "current" lifetime.
        // This is the generation that will be used for newly created sagas.
        let sec_generation_now = db::SecGeneration::random();
        // SEC generation for the "next" lifetime.
        // We'll use this generation during saga recovery.  It must differ from
        // the one we use to create new sagas or else we'll skip those sagas
        // during recovery.
        let sec_generation_recovery = db::SecGeneration::random();
        assert_ne!(sec_generation_now, sec_generation_recovery);
        let (storage, sec_client, uctx) = create_storage_sec_and_context(
            &log,
            db_datastore.clone(),
            sec_id,
            sec_generation_now,
        );
        let sec_log = log.new(o!("component" => "SEC"));
        let opctx = OpContext::for_tests(
            log,
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        // Create and start a saga.
        //
        // Because "do_unplug" is set to true, we should detach storage within
        // the first node operation.
        //
        // We expect the saga will appear to complete successfully because the
        // simulated storage subsystem returns "OK" rather than an error.  But
        // the saga log that remains in the database will make it look like it
        // didn't finish.
        uctx.do_unplug.store(true, Ordering::SeqCst);
        let (saga_id, result) = run_test_saga(&uctx, &sec_client).await;
        let output = result.kind.unwrap();
        assert_eq!(output.lookup_node_output::<i32>("n1_out").unwrap(), 1);
        assert_eq!(output.lookup_node_output::<i32>("n2_out").unwrap(), 2);
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Now we "reboot", by terminating the SEC and creating a new one
        // using the same storage system.
        //
        // We update uctx to prevent the storage system from detaching again.
        sec_client.shutdown().await;
        let sec_client = steno::sec(sec_log, storage.clone());
        uctx.storage.set_unplug(false);
        uctx.do_unplug.store(false, Ordering::SeqCst);

        // Recover the saga, observing that it re-runs operations and completes.
        let sec_client = Arc::new(sec_client);
        let recovered = recover(
            &opctx,
            sec_id,
            sec_generation_recovery,
            &|_| false,
            &|_| uctx.clone(),
            &db_datastore,
            &sec_client,
            registry_create(),
        )
        .await
        .unwrap();

        assert_eq!([saga_id], *recovered.iter_recovered().collect::<Vec<_>>());
        assert_eq!(0, recovered.iter_failed().count());
        assert_eq!(0, recovered.iter_skipped().count());

        recovered.wait_for_recovered_sagas_to_finish().await.unwrap();
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 2);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 2);

        // Test cleanup
        let sec_client = Arc::try_unwrap(sec_client).unwrap();
        sec_client.shutdown().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
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

    // Tests that a saga that has finished (as reflected in the database state)
    // does not get recovered.
    #[tokio::test]
    async fn test_successful_saga_does_not_replay_during_recovery() {
        // Test setup
        let logctx = dev::test_setup_log(
            "test_successful_saga_does_not_replay_during_recovery",
        );
        let log = logctx.log.new(o!());
        let (mut db, db_datastore) = new_db(&log).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        // SEC generation for the "current" lifetime.
        // This is the generation that will be used for newly created sagas.
        let sec_generation_now = db::SecGeneration::random();
        // SEC generation for the "next" lifetime.
        // We'll use this generation during saga recovery.  It must differ from
        // the one we use to create new sagas or else we'll skip those sagas
        // during recovery.
        let sec_generation_recovery = db::SecGeneration::random();
        assert_ne!(sec_generation_now, sec_generation_recovery);
        let (storage, sec_client, uctx) = create_storage_sec_and_context(
            &log,
            db_datastore.clone(),
            sec_id,
            sec_generation_now,
        );
        let sec_log = log.new(o!("component" => "SEC"));
        let opctx = OpContext::for_tests(
            log,
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        // Create and start a saga, which we expect to complete successfully.
        let (_, result) = run_test_saga(&uctx, &sec_client).await;
        let output = result.kind.unwrap();
        assert_eq!(output.lookup_node_output::<i32>("n1_out").unwrap(), 1);
        assert_eq!(output.lookup_node_output::<i32>("n2_out").unwrap(), 2);
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Now we "reboot", by terminating the SEC and creating a new one
        // using the same storage system.
        sec_client.shutdown().await;
        let sec_client = steno::sec(sec_log, storage.clone());

        // Recover the saga.
        let sec_client = Arc::new(sec_client);
        let recovered = recover(
            &opctx,
            sec_id,
            sec_generation_recovery,
            &|_| false,
            &|_| uctx.clone(),
            &db_datastore,
            &sec_client,
            registry_create(),
        )
        .await
        .unwrap();
        // Recovery should not have even seen this saga.
        assert_eq!(recovered.iter_recovered().count(), 0);
        assert_eq!(recovered.iter_skipped().count(), 0);
        assert_eq!(recovered.iter_failed().count(), 0);

        // The nodes should not have been replayed.
        recovered.wait_for_recovered_sagas_to_finish().await.unwrap();
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Test cleanup
        let sec_client = Arc::try_unwrap(sec_client).unwrap();
        sec_client.shutdown().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Tests that we skip recovering sagas (1) from the current SEC generation
    // and (2) that we're told to skip.
    #[tokio::test]
    async fn test_recovery_skip() {
        // Test setup
        let logctx = dev::test_setup_log("test_recovery_skip");
        let log = logctx.log.new(o!());
        let (mut db, db_datastore) = new_db(&log).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        // SEC generation for the "current" lifetime.
        // This is the generation that will be used for newly created sagas.
        let sec_generation_now = db::SecGeneration::random();
        // SEC generation for the "next" lifetime.
        // We'll use this generation during saga recovery.  It must differ from
        // the one we use to create new sagas or else we'll skip those sagas
        // during recovery.
        let sec_generation_recovery = db::SecGeneration::random();
        assert_ne!(sec_generation_now, sec_generation_recovery);
        let (storage, sec_client, uctx) = create_storage_sec_and_context(
            &log,
            db_datastore.clone(),
            sec_id,
            sec_generation_now.clone(),
        );
        let sec_log = log.new(o!("component" => "SEC"));
        let opctx = OpContext::for_tests(
            log,
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        // Create and start a saga.
        //
        // See test_failure_during_saga_can_be_recovered().  We use the same
        // approach here to construct database state that makes it look like the
        // saga has not finished, even though the in-memory state will look like
        // it has.
        uctx.do_unplug.store(true, Ordering::SeqCst);
        let (saga_id, result) = run_test_saga(&uctx, &sec_client).await;
        let output = result.kind.unwrap();
        assert_eq!(output.lookup_node_output::<i32>("n1_out").unwrap(), 1);
        assert_eq!(output.lookup_node_output::<i32>("n2_out").unwrap(), 2);
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Just like in that test, we'll simulate a restart to see what happens
        // during recovery.  But this time, we'll explicitly skip this saga.
        sec_client.shutdown().await;
        let sec_client = steno::sec(sec_log, storage.clone());
        uctx.storage.set_unplug(false);
        uctx.do_unplug.store(false, Ordering::SeqCst);

        // First of all, carry out recovery using the same generation as the
        // SecStore.  This should not find our saga because recovery explicitly
        // skips sagas created with the same generation.
        let sec_client = Arc::new(sec_client);
        let recovered = recover(
            &opctx,
            sec_id,
            sec_generation_now,
            &|found_saga_id| found_saga_id == saga_id,
            &|_| uctx.clone(),
            &db_datastore,
            &sec_client,
            registry_create(),
        )
        .await
        .unwrap();
        assert_eq!(0, recovered.iter_recovered().count());
        assert_eq!(0, recovered.iter_failed().count());
        assert_eq!(0, recovered.iter_skipped().count());

        // Carry out recovery.
        let recovered = recover(
            &opctx,
            sec_id,
            sec_generation_recovery,
            &|found_saga_id| found_saga_id == saga_id,
            &|_| uctx.clone(),
            &db_datastore,
            &sec_client,
            registry_create(),
        )
        .await
        .unwrap();

        // We should report no sagas recovered, but one skipped.
        assert_eq!(0, recovered.iter_recovered().count());
        assert_eq!(0, recovered.iter_failed().count());
        assert_eq!([saga_id], *recovered.iter_skipped().collect::<Vec<_>>());

        // Be sure that nothing from that saga actually ran.
        recovered.wait_for_recovered_sagas_to_finish().await.unwrap();
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Test cleanup
        let sec_client = Arc::try_unwrap(sec_client).unwrap();
        sec_client.shutdown().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // XXX-dap TODO-coverage test the case of saga recovery error, with other
    // sagas present
}
