// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handles recovery of sagas

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
    let found_sagas = list_unfinished_sagas(&opctx, datastore, &sec_id).await?;

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

/// Queries the database to return a list of uncompleted sagas assigned to SEC
/// `sec_id`
// For now, we do the simplest thing: we fetch all the sagas that the
// caller's going to need before returning any of them.  This is easier to
// implement than, say, using a channel or some other stream.  In principle
// we're giving up some opportunity for parallelism.  The caller could be
// going off and fetching the saga log for the first sagas that we find
// while we're still listing later sagas.  Doing that properly would require
// concurrency limits to prevent overload or starvation of other database
// consumers.
async fn list_unfinished_sagas(
    opctx: &OpContext,
    datastore: &db::DataStore,
    sec_id: &db::SecId,
) -> Result<Vec<db::saga_types::Saga>, Error> {
    trace!(&opctx.log, "listing sagas");

    // Read all sagas in batches.
    //
    // Although we could read them all into memory simultaneously, this
    // risks blocking the DB for an unreasonable amount of time. Instead,
    // we paginate to avoid cutting off availability to the DB.
    let mut sagas = vec![];
    let mut paginator = Paginator::new(SQL_BATCH_SIZE);
    let conn = datastore.pool_connection_authorized(opctx).await?;
    while let Some(p) = paginator.next() {
        use db::schema::saga::dsl;

        let mut batch = paginated(dsl::saga, dsl::id, &p.current_pagparams())
            .filter(dsl::saga_state.ne(db::saga_types::SagaCachedState(
                steno::SagaCachedState::Done,
            )))
            .filter(dsl::current_sec.eq(*sec_id))
            .select(db::saga_types::Saga::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SagaDbg,
                        LookupType::ById(sec_id.0),
                    ),
                )
            })?;

        paginator = p.found_batch(&batch, &|row| row.id);
        sagas.append(&mut batch);
    }
    Ok(sagas)
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
    // Read all events in batches.
    //
    // Although we could read them all into memory simultaneously, this
    // risks blocking the DB for an unreasonable amount of time. Instead,
    // we paginate to avoid cutting off availability.
    let mut events = vec![];
    let mut paginator = Paginator::new(SQL_BATCH_SIZE);
    let conn = datastore.pool_connection_authorized(opctx).await?;
    while let Some(p) = paginator.next() {
        use db::schema::saga_node_event::dsl;
        let batch = paginated_multicolumn(
            dsl::saga_node_event,
            (dsl::node_id, dsl::event_type),
            &p.current_pagparams(),
        )
        .filter(dsl::saga_id.eq(saga.id))
        .select(db::saga_types::SagaNodeEvent::as_select())
        .load_async(&*conn)
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        .await?;
        paginator =
            p.found_batch(&batch, &|row| (row.node_id, row.event_type.clone()));

        let mut batch = batch
            .into_iter()
            .map(|event| steno::SagaNodeEvent::try_from(event))
            .collect::<Result<Vec<_>, Error>>()?;

        events.append(&mut batch);
    }
    Ok(events)
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
        let (storage, sec_client, uctx) =
            create_storage_sec_and_context(&log, db_datastore.clone(), sec_id);
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
        let (storage, sec_client, uctx) =
            create_storage_sec_and_context(&log, db_datastore.clone(), sec_id);
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

    // Tests that we skip recovering sagas that we're told to skip.
    #[tokio::test]
    async fn test_recovery_skip() {
        // Test setup
        let logctx = dev::test_setup_log("test_recovery_skip");
        let log = logctx.log.new(o!());
        let (mut db, db_datastore) = new_db(&log).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let (storage, sec_client, uctx) =
            create_storage_sec_and_context(&log, db_datastore.clone(), sec_id);
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

        // Carry out recovery.
        let sec_client = Arc::new(sec_client);
        let recovered = recover(
            &opctx,
            sec_id,
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

    #[tokio::test]
    async fn test_list_unfinished_sagas() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_unfinished_sagas");
        let log = logctx.log.new(o!());
        let (mut db, db_datastore) = new_db(&log).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let opctx = OpContext::for_tests(
            log,
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        // Create a couple batches of sagas.
        let new_running_db_saga = || {
            let params = steno::SagaCreateParams {
                id: steno::SagaId(Uuid::new_v4()),
                name: steno::SagaName::new("test saga"),
                dag: serde_json::value::Value::Null,
                state: steno::SagaCachedState::Running,
            };

            db::model::saga_types::Saga::new(sec_id, params)
        };
        let mut inserted_sagas = (0..SQL_BATCH_SIZE.get() * 2)
            .map(|_| new_running_db_saga())
            .collect::<Vec<_>>();

        // Shuffle these sagas into a random order to check that the pagination
        // order is working as intended on the read path, which we'll do later
        // in this test.
        inserted_sagas.shuffle(&mut rand::thread_rng());

        // Insert the batches of unfinished sagas into the database
        let conn = db_datastore
            .pool_connection_unauthorized()
            .await
            .expect("Failed to access db connection");
        diesel::insert_into(db::schema::saga::dsl::saga)
            .values(inserted_sagas.clone())
            .execute_async(&*conn)
            .await
            .expect("Failed to insert test setup data");

        // List them, expect to see them all in order by ID.
        let mut observed_sagas =
            list_unfinished_sagas(&opctx, &db_datastore, &sec_id)
                .await
                .expect("Failed to list unfinished sagas");
        inserted_sagas.sort_by_key(|a| a.id);

        // Timestamps can change slightly when we insert them.
        //
        // Sanitize them to make input/output equality checks easier.
        let sanitize_timestamps = |sagas: &mut Vec<db::saga_types::Saga>| {
            for saga in sagas {
                saga.time_created = chrono::DateTime::UNIX_EPOCH;
                saga.adopt_time = chrono::DateTime::UNIX_EPOCH;
            }
        };
        sanitize_timestamps(&mut observed_sagas);
        sanitize_timestamps(&mut inserted_sagas);

        assert_eq!(
            inserted_sagas, observed_sagas,
            "Observed sagas did not match inserted sagas"
        );

        // Test cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_list_unfinished_nodes() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_unfinished_nodes");
        let log = logctx.log.new(o!());
        let (mut db, db_datastore) = new_db(&log).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let opctx = OpContext::for_tests(
            log,
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );
        let saga_id = steno::SagaId(Uuid::new_v4());

        // Create a couple batches of saga events
        let new_db_saga_nodes =
            |node_id: u32, event_type: steno::SagaNodeEventType| {
                let event = steno::SagaNodeEvent {
                    saga_id,
                    node_id: steno::SagaNodeId::from(node_id),
                    event_type,
                };

                db::model::saga_types::SagaNodeEvent::new(event, sec_id)
            };
        let mut inserted_nodes = (0..SQL_BATCH_SIZE.get() * 2)
            .flat_map(|i| {
                // This isn't an exhaustive list of event types, but gives us a
                // few options to pick from. Since this is a pagination key,
                // it's important to include a variety here.
                use steno::SagaNodeEventType::*;
                [
                    new_db_saga_nodes(i, Started),
                    new_db_saga_nodes(i, UndoStarted),
                    new_db_saga_nodes(i, UndoFinished),
                ]
            })
            .collect::<Vec<_>>();

        // Shuffle these nodes into a random order to check that the pagination
        // order is working as intended on the read path, which we'll do later
        // in this test.
        inserted_nodes.shuffle(&mut rand::thread_rng());

        // Insert them into the database
        let conn = db_datastore
            .pool_connection_unauthorized()
            .await
            .expect("Failed to access db connection");
        diesel::insert_into(db::schema::saga_node_event::dsl::saga_node_event)
            .values(inserted_nodes.clone())
            .execute_async(&*conn)
            .await
            .expect("Failed to insert test setup data");

        // List them, expect to see them all in order by ID.
        //
        // Note that we need to make up a saga to see this, but the
        // part of it that actually matters is the ID.
        let params = steno::SagaCreateParams {
            id: saga_id,
            name: steno::SagaName::new("test saga"),
            dag: serde_json::value::Value::Null,
            state: steno::SagaCachedState::Running,
        };
        let saga = db::model::saga_types::Saga::new(sec_id, params);
        let observed_nodes = load_saga_log(&opctx, &db_datastore, &saga)
            .await
            .expect("Failed to list unfinished nodes");
        inserted_nodes.sort_by_key(|a| (a.node_id, a.event_type.clone()));

        let inserted_nodes = inserted_nodes
            .into_iter()
            .map(|node| steno::SagaNodeEvent::try_from(node))
            .collect::<Result<Vec<_>, _>>()
            .expect("Couldn't convert DB nodes to steno nodes");

        // The steno::SagaNodeEvent type doesn't implement PartialEq, so we need
        // to do this a little manually.
        assert_eq!(inserted_nodes.len(), observed_nodes.len());
        for i in 0..inserted_nodes.len() {
            assert_eq!(inserted_nodes[i].saga_id, observed_nodes[i].saga_id);
            assert_eq!(inserted_nodes[i].node_id, observed_nodes[i].node_id);
            assert_eq!(
                inserted_nodes[i].event_type.label(),
                observed_nodes[i].event_type.label()
            );
        }

        // Test cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_list_no_unfinished_nodes() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_no_unfinished_nodes");
        let log = logctx.log.new(o!());
        let (mut db, db_datastore) = new_db(&log).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let opctx = OpContext::for_tests(
            log,
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );
        let saga_id = steno::SagaId(Uuid::new_v4());

        let params = steno::SagaCreateParams {
            id: saga_id,
            name: steno::SagaName::new("test saga"),
            dag: serde_json::value::Value::Null,
            state: steno::SagaCachedState::Running,
        };
        let saga = db::model::saga_types::Saga::new(sec_id, params);

        // Test that this returns "no nodes" rather than throwing some "not
        // found" error.
        let observed_nodes = load_saga_log(&opctx, &db_datastore, &saga)
            .await
            .expect("Failed to list unfinished nodes");
        assert_eq!(observed_nodes.len(), 0);

        // Test cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // XXX-dap need to fix *and* test the case where we attempt to recover a
    // saga that was started in this program's lifetime
    // XXX-dap TODO-coverage test the case of saga recovery error, with other
    // sagas present
}
