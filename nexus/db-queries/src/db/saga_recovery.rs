// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handles recovery of sagas

use crate::context::OpContext;
use crate::db;
use futures::{future::BoxFuture, TryFutureExt};
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::backoff::retry_notify;
use omicron_common::backoff::retry_policy_internal_service;
use omicron_common::backoff::BackoffError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Result type of a [`RecoveryTask`].
pub type RecoveryResult = Result<CompletionTask, Error>;

/// A future which completes once sagas have been loaded and resumed.
/// Note that this does not necessarily mean the sagas have completed
/// execution.
///
/// Returns a Result of either:
/// - A [`CompletionTask`] to track the completion of the resumed sagas, or
/// - An [`Error`] encountered when attempting to load and resume sagas.
pub struct RecoveryTask(BoxFuture<'static, RecoveryResult>);

impl Future for RecoveryTask {
    type Output = RecoveryResult;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

/// Result type from a [`CompletionTask`].
pub type CompletionResult = Result<(), Error>;

/// A future which completes once loaded and resumed sagas have also completed.
pub struct CompletionTask(BoxFuture<'static, CompletionResult>);

impl Future for CompletionTask {
    type Output = CompletionResult;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

/// Starts an asynchronous task to recover sagas (as after a crash or restart)
///
/// More specifically, this task queries the database to list all uncompleted
/// sagas that are assigned to SEC `sec_id` and for each one:
///
/// * loads the saga DAG and log from `datastore`
/// * uses [`steno::SecClient::saga_resume`] to prepare to resume execution of
///   the saga using the persistent saga log
/// * resumes execution of each saga
///
/// The returned [`RecoveryTask`] completes once all sagas have been loaded
/// and resumed, and itself returns a [`CompletionTask`] which completes
/// when those resumed sagas have finished.
pub fn recover<T>(
    opctx: OpContext,
    sec_id: db::SecId,
    uctx: Arc<T::ExecContextType>,
    datastore: Arc<db::DataStore>,
    sec_client: Arc<steno::SecClient>,
    registry: Arc<steno::ActionRegistry<T>>,
) -> RecoveryTask
where
    T: steno::SagaType,
{
    let join_handle = tokio::spawn(async move {
        info!(&opctx.log, "start saga recovery");

        // We perform the initial list of sagas using a standard retry policy.
        // We treat all errors as transient because there's nothing we can do
        // about any of them except try forever.  As a result, we never expect
        // an error from the overall operation.
        // TODO-monitoring we definitely want a way to raise a big red flag if
        // saga recovery is not completing.
        // TODO-robustness It would be better to retry the individual database
        // operations within this operation than retrying the overall operation.
        // As this is written today, if the listing requires a bunch of pages
        // and the operation fails partway through, we'll re-fetch all the pages
        // we successfully fetched before.  If the database is overloaded and
        // only N% of requests are completing, the probability of this operation
        // succeeding decreases considerably as the number of separate queries
        // (pages) goes up.  We'd be much more likely to finish the overall
        // operation if we didn't throw away the results we did get each time.
        let found_sagas = retry_notify(
            retry_policy_internal_service(),
            || async {
                list_unfinished_sagas(&opctx, &datastore, &sec_id)
                    .await
                    .map_err(BackoffError::transient)
            },
            |error, duration| {
                warn!(
                    &opctx.log,
                    "failed to list sagas (will retry after {:?}): {:#}",
                    duration,
                    error
                )
            },
        )
        .await
        .unwrap();

        info!(&opctx.log, "listed sagas ({} total)", found_sagas.len());

        let recovery_futures = found_sagas.into_iter().map(|saga| async {
            // TODO-robustness We should put this into a retry loop.  We may
            // also want to take any failed sagas and put them at the end of the
            // queue.  It shouldn't really matter, in that the transient
            // failures here are likely to affect recovery of all sagas.
            // However, it's conceivable we misclassify a permanent failure as a
            // transient failure, or that a transient failure is more likely to
            // affect some sagas than others (e.g, data on a different node, or
            // it has a larger log that requires more queries).  To avoid one
            // bad saga ruining the rest, we should try to recover the rest
            // before we go back to one that's failed.
            // TODO-debugging want visibility into "abandoned" sagas
            let saga_id: steno::SagaId = saga.id.into();
            recover_saga(
                &opctx,
                Arc::clone(&uctx),
                &datastore,
                &sec_client,
                Arc::clone(&registry),
                saga,
            )
            .map_err(|error| {
                warn!(
                    &opctx.log,
                    "failed to recover saga {}: {:#}", saga_id, error
                );
                error
            })
            .await
        });

        let mut completion_futures = Vec::with_capacity(recovery_futures.len());
        // Loads and resumes all sagas in serial.
        for recovery_future in recovery_futures {
            let saga_complete_future = recovery_future.await?;
            completion_futures.push(saga_complete_future);
        }
        // Returns a future that awaits the completion of all resumed sagas.
        Ok(CompletionTask(Box::pin(async move {
            futures::future::try_join_all(completion_futures).await?;
            Ok(())
        })))
    });

    RecoveryTask(Box::pin(async move {
        // Unwraps join-related errors.
        join_handle.await.unwrap()
    }))
}

// Creates new page params for querying sagas.
fn new_page_params(
    marker: Option<&uuid::Uuid>,
) -> DataPageParams<'_, uuid::Uuid> {
    DataPageParams {
        marker,
        direction: dropshot::PaginationOrder::Ascending,
        limit: std::num::NonZeroU32::new(100).unwrap(),
    }
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
    let mut last_id = None;
    let mut sagas = vec![];
    loop {
        let pagparams = new_page_params(last_id.as_ref());
        let mut some_sagas =
            datastore.saga_list_unfinished_by_id(sec_id, &pagparams).await?;
        if some_sagas.is_empty() {
            break;
        }
        sagas.append(&mut some_sagas);
        last_id = Some(sagas.last().as_ref().unwrap().id.0 .0);
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
    uctx: Arc<T::ExecContextType>,
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
    let saga_name = saga.name.clone();
    trace!(opctx.log, "recovering saga: start";
        "saga_id" => saga_id.to_string(),
        "saga_name" => saga_name.clone(),
    );

    let log_events = load_saga_log(datastore, &saga).await?;
    trace!(
        opctx.log,
        "recovering saga: loaded log";
        "saga_id" => ?saga_id,
        "saga_name" => saga_name.clone()
    );
    let saga_completion = sec_client
        .saga_resume(
            saga_id,
            Arc::clone(&uctx),
            saga.saga_dag,
            registry,
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
    datastore: &db::DataStore,
    saga: &db::saga_types::Saga,
) -> Result<Vec<steno::SagaNodeEvent>, Error> {
    // Read all events in batches.
    //
    // Although we could read them all into memory simultaneously, this
    // risks blocking the DB for an unreasonable amount of time. Instead,
    // we paginate to avoid cutting off availability.
    let mut last_id = None;
    let mut events = vec![];
    loop {
        let pagparams = new_page_params(last_id.as_ref());
        let mut some_events =
            datastore.saga_node_event_list_by_id(saga.id, &pagparams).await?;
        if some_events.is_empty() {
            break;
        }
        events.append(&mut some_events);
        last_id = Some(events.last().as_ref().unwrap().saga_id.0);
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
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use steno::{
        new_action_noop_undo, Action, ActionContext, ActionError,
        ActionRegistry, DagBuilder, Node, SagaDag, SagaId, SagaName, SagaType,
        SecClient,
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
        let pool =
            Arc::new(db::Pool::new_qorb_single_host_blocking(&cfg).await);
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
        // We expect the saga will complete successfully, because the
        // storage subsystem returns "OK" rather than an error.
        uctx.do_unplug.store(true, Ordering::SeqCst);
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
        let result = future.await;
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
        recover(
            opctx,
            sec_id,
            uctx.clone(),
            db_datastore,
            sec_client.clone(),
            registry_create(),
        )
        .await // Await the loading and resuming of the sagas
        .unwrap()
        .await // Awaits the completion of the resumed sagas
        .unwrap();
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 2);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 2);

        // Test cleanup
        let sec_client = Arc::try_unwrap(sec_client).unwrap();
        sec_client.shutdown().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

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
        let result = future.await;
        let output = result.kind.unwrap();
        assert_eq!(output.lookup_node_output::<i32>("n1_out").unwrap(), 1);
        assert_eq!(output.lookup_node_output::<i32>("n2_out").unwrap(), 2);
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Now we "reboot", by terminating the SEC and creating a new one
        // using the same storage system.
        sec_client.shutdown().await;
        let sec_client = steno::sec(sec_log, storage.clone());

        // Recover the saga, observing that it does not replay the nodes.
        let sec_client = Arc::new(sec_client);
        recover(
            opctx,
            sec_id,
            uctx.clone(),
            db_datastore,
            sec_client.clone(),
            registry_create(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
        assert_eq!(uctx.n1_count.load(Ordering::SeqCst), 1);
        assert_eq!(uctx.n2_count.load(Ordering::SeqCst), 1);

        // Test cleanup
        let sec_client = Arc::try_unwrap(sec_client).unwrap();
        sec_client.shutdown().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
