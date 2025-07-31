// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus-level saga management and execution
//!
//! Steno provides its own interfaces for managing sagas.  The interface here is
//! a thin wrapper aimed at the mini framework we've built at the Nexus level
//! that makes it easier to define and manage sagas in a uniform way.
//!
//! The basic lifecycle at the Nexus level is:
//!
//! ```text
//!       input: saga type (impls [`NexusSaga`]),
//!              saga parameters (specific to the saga's type)
//!           |
//!           |  [`create_saga_dag()`]
//!           v
//!        SagaDag
//!           |
//!           |  [`SagaExecutor::saga_prepare()`]
//!           v
//!      RunnableSaga
//!           |
//!           |  [`RunnableSaga::start()`]
//!           v
//!      RunningSaga
//!           |
//!           |  [`RunningSaga::wait_until_stopped()`]
//!           v
//!      StoppedSaga
//! ```
//!
//! At the end, you can use [`StoppedSaga::into_omicron_result()`] to get at the
//! success output of the saga or convert any saga failure along the way to an
//! Omicron [`Error`].
//!
//! This interface allows a few different use cases:
//!
//! * A common case is that some code in Nexus wants to do all of this: create
//!   the saga DAG, run it, wait for it to finish, and get the result.
//!   [`SagaExecutor::saga_execute()`] does all this using these lower-level
//!   interfaces.
//! * An expected use case is that some code in Nexus wants to kick off a saga
//!   but not wait for it to finish.  In this case, they can just stop after
//!   calling [`RunnableSaga::start()`].  The saga will continue running; they
//!   just won't be able to directly wait for it to finish or get the result.
//! * Tests can use any of the lower-level pieces to examine intermediate state
//!   or inject errors.

use super::sagas::ACTION_REGISTRY;
use super::sagas::NexusSaga;
use crate::Nexus;
use crate::saga_interface::SagaContext;
use anyhow::Context;
use chrono::Utc;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use iddqd::IdOrdMap;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::internal_api::views::DemoSaga;
use nexus_types::internal_api::views::RunningSagaInfo;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResult;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use omicron_uuid_kinds::DemoSagaUuid;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::sync::OnceLock;
use steno::SagaDag;
use steno::SagaId;
use steno::SagaResult;
use steno::SagaResultOk;
use tokio::sync::mpsc;
use tokio::sync::watch;
use uuid::Uuid;

/// Given a particular kind of Nexus saga (the type parameter `N`) and
/// parameters for that saga, construct a [`SagaDag`] for it
pub(crate) fn create_saga_dag<N: NexusSaga>(
    params: N::Params,
) -> Result<SagaDag, Error> {
    N::prepare(&params)
}

/// Interface for kicking off sagas
///
/// See [`SagaExecutor`] for the implementation within Nexus.  Some tests use
/// alternate implementations that don't actually run the sagas.
pub(crate) trait StartSaga: Send + Sync {
    /// Create a new saga (of type `N` with parameters `params`), start it
    /// running, but do not wait for it to finish.
    ///
    /// This method returns the ID of the running saga.
    fn saga_start(&self, dag: SagaDag) -> BoxFuture<'_, Result<SagaId, Error>>;

    /// Create a new saga (of type `N` with parameters `params`), start it
    /// running, and return a future that can be used to wait for it to finish
    /// (along with the saga's ID).
    ///
    /// Callers who do not need to wait for the saga's completion should use
    /// `StartSaga::saga_start`, instead, as it avoids allocating the second
    /// `BoxFuture` for the completion future.
    fn saga_run(
        &self,
        dag: SagaDag,
    ) -> BoxFuture<'_, Result<(SagaId, SagaCompletionFuture), Error>>;
}

pub type SagaCompletionFuture = BoxFuture<'static, Result<(), Error>>;

impl StartSaga for SagaExecutor {
    fn saga_start(&self, dag: SagaDag) -> BoxFuture<'_, Result<SagaId, Error>> {
        async move {
            let runnable_saga = self.saga_prepare(dag).await?;
            // start() returns a future that can be used to wait for the saga to
            // complete.  We don't need that here.  (Cancelling this has no
            // effect on the running saga.)
            let running_saga = runnable_saga.start().await?;

            Ok(running_saga.id)
        }
        .boxed()
    }

    fn saga_run(
        &self,
        dag: SagaDag,
    ) -> BoxFuture<'_, Result<(SagaId, SagaCompletionFuture), Error>> {
        async move {
            let runnable_saga = self.saga_prepare(dag).await?;
            let running_saga = runnable_saga.start().await?;
            let id = running_saga.id;
            let completed = async move {
                running_saga
                    .wait_until_stopped()
                    .await
                    // Eat the saga's outputs, saga log, etc., and just return
                    // whether it succeeded or failed. This is necessary because
                    // some tests rely on a `NoopStartSaga` implementation that
                    // doesn't actually run sagas and therefore cannot produce a
                    // real saga log or outputs.
                    .into_omicron_result()
                    .map(|_| ())
            }
            .boxed();
            Ok((id, completed))
        }
        .boxed()
    }
}

/// Describes both the configuration (whether sagas are allowed to be executed)
/// and the state (how many sagas are running) for the purpose of quiescing
/// Nexus.
// Both configuration and state must be combined (under the same watch channel)
// to avoid races in detecting quiesce.  We want the quiescer to be able to say
// that if `sagas_allowed` is `Disallowed` and there are no sagas running, then
// sagas are quiesced.  But there's no way to guarantee that if these are stored
// in separate channels.
#[derive(Debug, Clone)]
struct Quiesce {
    sagas_allowed: SagasAllowed,
    sagas_running: IdOrdMap<RunningSagaInfo>,
}

/// Policy determining whether new sagas are allowed to be started
///
/// This is used by Nexus quiesce to disallow creation of new sagas when we're
/// trying to quiesce Nexus.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SagasAllowed {
    /// New sagas may be started (normal condition)
    Allowed,
    /// New sagas may not be started (happens during quiesce)
    Disallowed,
}

/// Handle to a self-contained subsystem for kicking off sagas
///
/// See the module-level documentation for details.
pub(crate) struct SagaExecutor {
    sec_client: Arc<steno::SecClient>,
    log: slog::Logger,
    nexus: OnceLock<Arc<Nexus>>,
    saga_create_tx: mpsc::UnboundedSender<steno::SagaId>,
    quiesce: watch::Sender<Quiesce>,
}

impl SagaExecutor {
    pub(crate) fn new(
        sec_client: Arc<steno::SecClient>,
        log: slog::Logger,
        saga_create_tx: mpsc::UnboundedSender<steno::SagaId>,
    ) -> SagaExecutor {
        let (quiesce, _) = watch::channel(Quiesce {
            sagas_allowed: SagasAllowed::Allowed,
            sagas_running: IdOrdMap::new(),
        });

        SagaExecutor {
            sec_client,
            log,
            nexus: OnceLock::new(),
            saga_create_tx,
            quiesce,
        }
    }

    /// Disallow new sagas from being started.
    ///
    /// This is currently a one-way trip.  Sagas cannot be un-quiesced.
    pub fn quiesce(&self) {
        // Log this before changing the config to make sure this message
        // appears before messages from code paths that saw this change.
        info!(&self.log, "starting saga quiesce");
        self.quiesce.send_modify(|q| {
            q.sagas_allowed = SagasAllowed::Disallowed;
        });
    }

    /// Wait for sagas to be quiesced
    pub async fn wait_for_quiesced(&self) {
        let mut rx = self.quiesce.subscribe();
        // unwrap(): this can only fail if the tx side is dropped, but that
        // can't happen because we have a reference to it via `self`.
        rx.wait_for(|q| {
            q.sagas_allowed == SagasAllowed::Disallowed
                && q.sagas_running.is_empty()
        })
        .await
        .unwrap();
    }

    /// Returns information about running sagas (involves a clone)
    pub fn sagas_running(&self) -> IdOrdMap<RunningSagaInfo> {
        self.quiesce.borrow().sagas_running.clone()
    }

    // This is a little gross.  We want to hang the SagaExecutor off of Nexus,
    // but we also need to refer to Nexus, which thus can't exist when
    // SagaExecutor is constructed.  So we have the caller hand it to us after
    // initialization.
    //
    // This isn't as statically verifiable as we'd normally like.  But it's only
    // one call site, it does fail cleanly if someone tries to use
    // `SagaExecutor` before this has been set, and the result is much cleaner
    // for all the other users of `SagaExecutor`.
    //
    // # Panics
    //
    // This function should be called exactly once in the lifetime of any
    // `SagaExecutor` object.  If it gets called more than once, concurrently or
    // not, it panics.
    pub(crate) fn set_nexus(&self, nexus: Arc<Nexus>) {
        self.nexus.set(nexus).unwrap_or_else(|_| {
            panic!("multiple initialization of SagaExecutor")
        })
    }

    fn nexus(&self) -> Result<&Arc<Nexus>, Error> {
        self.nexus
            .get()
            .ok_or_else(|| Error::unavail("saga are not available yet"))
    }

    // Low-level interface
    //
    // The low-level interface for running sagas starts with `saga_prepare()`
    // and then uses the `RunnableSaga`, `RunningSaga`, and `StoppedSaga` types
    // to drive execution forward.

    /// Given a DAG (which has generally been specifically created for a
    /// particular saga and includes the saga's parameters), prepare to start
    /// running the saga.  This does not actually start the saga running.
    ///
    /// ## Async cancellation
    ///
    /// The Future returned by this function is basically not cancellation-safe,
    /// in that if this Future is cancelled, one of a few things might be true:
    ///
    /// * Nothing has happened; it's as though this function was never called.
    /// * The saga has been created, but not started.  If this happens, the saga
    ///   will likely start running the next time saga recovery happens (e.g.,
    ///   the next time Nexus starts up) and then run to completion.
    ///
    /// It's not clear what the caller would _want_ if they cancelled this
    /// future, but whatever it is, clearly it's not guaranteed to be true.
    /// You're better off avoiding cancellation.  Fortunately, we currently
    /// execute sagas either from API calls and background tasks, neither of
    /// which can be cancelled.  **This function should not be used in a
    /// `tokio::select!` with a `timeout` or the like.**
    pub(crate) async fn saga_prepare(
        &self,
        dag: SagaDag,
    ) -> Result<RunnableSaga, Error> {
        let saga_id = SagaId(Uuid::new_v4());
        let saga_name = dag.saga_name();

        let allowed = self.quiesce.send_if_modified(|q| {
            if let SagasAllowed::Allowed = q.sagas_allowed {
                q.sagas_running
                    .insert_unique(RunningSagaInfo {
                        saga_id,
                        saga_name: saga_name.clone(),
                        time_started: Utc::now(),
                    })
                    .expect("unique saga id");
                true
            } else {
                false
            }
        });

        if !allowed {
            warn!(
                &self.log,
                "disallowing new saga (quiescing)";
                "saga_name" => saga_name.to_string(),
            );
            return Err(Error::unavail("new sagas are currently disallowed"));
        };

        // This handle will ensure that we remove the saga from the
        // `sagas_running` set if either the caller never runs the saga (i.e.,
        // they drop the RunnableSaga before starting it) or else when the saga
        // finishes.
        //
        // This must be instantiated after the successful insert above and
        // before any code path that would return early.
        let saga_ref =
            RunningSagaReference { saga_id, quiesce: self.quiesce.clone() };

        // Construct the context necessary to execute this saga.
        let nexus = self.nexus()?;
        let saga_logger = self.log.new(o!(
            "saga_name" => saga_name.to_string(),
            "saga_id" => saga_id.to_string()
        ));
        let saga_context = Arc::new(Arc::new(SagaContext::new(
            nexus.clone(),
            saga_logger.clone(),
        )));

        // Tell the recovery task about this.  It's critical that we send this
        // message before telling Steno about this saga.  It's not critical that
        // the task _receive_ this message synchronously.  See the comments in
        // the recovery task implementation for details.
        self.saga_create_tx.send(saga_id).map_err(
            |_: mpsc::error::SendError<SagaId>| {
                Error::internal_error(
                    "cannot create saga: recovery task not listening \
                     (is Nexus shutting down?)",
                )
            },
        )?;

        // Tell Steno about it.  This does not start it running yet.
        info!(saga_logger, "preparing saga");
        let saga_completion_future = self
            .sec_client
            .saga_create(
                saga_id,
                saga_context,
                Arc::new(dag),
                ACTION_REGISTRY.clone(),
            )
            .await
            .context("creating saga")
            .map_err(|error| {
                // TODO-error This could be a service unavailable error,
                // depending on the failure mode.  We need more information from
                // Steno.
                Error::internal_error(&format!("{:#}", error))
            })?;
        Ok(RunnableSaga {
            id: saga_id,
            saga_completion_future,
            log: saga_logger,
            sec_client: self.sec_client.clone(),
            saga_ref,
        })
    }

    // Convenience functions

    /// Create a new saga (of type `N` with parameters `params`), start it
    /// running, wait for it to finish, and report the result
    ///
    /// Note that this can take a long time and may not complete while parts of
    /// the system are not functioning.  Care should be taken when waiting on
    /// this in a latency-sensitive context.
    ///
    ///
    /// ## Async cancellation
    ///
    /// This function isn't really cancel-safe, in that if the Future returned
    /// by this function is cancelled, one of three things may be true:
    ///
    /// * Nothing has happened; it's as though this function was never called.
    /// * The saga has been created, but not started.  If this happens, the saga
    ///   will likely start running the next time saga recovery happens (e.g.,
    ///   the next time Nexus starts up) and then run to completion.
    /// * The saga has already been started and will eventually run to
    ///   completion (even though this Future has been cancelled).
    ///
    /// It's not clear what the caller would _want_ if they cancelled this
    /// future, but whatever it is, clearly it's not guaranteed to be true.
    /// You're better off avoiding cancellation.  Fortunately, we currently
    /// execute sagas either from API calls and background tasks, neither of
    /// which can be cancelled.  **This function should not be used in a
    /// `tokio::select!` with a `timeout` or the like.**
    ///
    /// Say you _do_ want to kick off a saga and wait only a little while before
    /// it completes.  In that case, you can use the lower-level interface to
    /// first create the saga (a process which still should not be cancelled,
    /// but would generally be quick) and then wait for it to finish.  The
    /// waiting part is cancellable.
    ///
    /// Note that none of this affects _crash safety_.  In terms of a crash: the
    /// crash will either happen before the saga has been created (in which
    /// case it's as though we didn't even call this function) or after (in
    /// which case the saga will run to completion).
    pub(crate) async fn saga_execute<N: NexusSaga>(
        &self,
        params: N::Params,
    ) -> Result<SagaResultOk, Error> {
        let dag = create_saga_dag::<N>(params)?;
        let runnable_saga = self.saga_prepare(dag).await?;
        let running_saga = runnable_saga.start().await?;
        let stopped_saga = running_saga.wait_until_stopped().await;
        stopped_saga.into_omicron_result()
    }
}

/// Encapsulates a saga to be run before we actually start running it
///
/// At this point, we've built the DAG, loaded it into the SEC, etc. but haven't
/// started it running.  This is a useful point to inject errors, inspect the
/// DAG, etc.
pub(crate) struct RunnableSaga {
    id: SagaId,
    saga_completion_future: BoxFuture<'static, SagaResult>,
    log: slog::Logger,
    sec_client: Arc<steno::SecClient>,
    saga_ref: RunningSagaReference,
}

impl RunnableSaga {
    pub(crate) fn id(&self) -> SagaId {
        self.id
    }

    /// Start this saga running.
    ///
    /// Once this completes, even if you drop the returned `RunningSaga`, the
    /// saga will still run to completion.
    pub(crate) async fn start(self) -> Result<RunningSaga, Error> {
        info!(self.log, "starting saga");
        self.sec_client
            .saga_start(self.id)
            .await
            .context("starting saga")
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;

        // When the saga finishes, we need to update our state to reflect that
        // it's no longer running.  We're provided a Future that we can use to
        // wait for the saga to finish.  We also provide an equivalent Future to
        // our consumer (in the `RunningSaga` that we return).  It'd be handy to
        // just hook into that one, but there's a hitch: our consumer is allowed
        // to drop that Future if they don't care when the saga finishes.  But
        // we do still care!  So we need to create our own task to poll the
        // completion future that we were given and pass along the result to the
        // Future that we provide our consumer.
        let saga_ref = self.saga_ref;
        let fut = self.saga_completion_future;

        // This is the task we spawn off to wait for the saga to finish and
        // update our state.  (The state update happens when `saga_ref` is
        // dropped.)
        let completion_watcher_task = tokio::spawn(async move {
            let rv = fut.await;
            drop(saga_ref);
            rv
        });

        // This is the future we'll provide to the consumer to be notified when
        // the saga finishes.
        let saga_completion_future = async move {
            match completion_watcher_task.await {
                Ok(rv) => rv,
                Err(error) => {
                    // This should be basically impossible.  A panic from the
                    // task would be a bug.  It's conceivable that it gets
                    // cancelled if we're in the middle of a shutdown of the
                    // tokio runtime itself.  That wouldn't really happen in the
                    // real Nexus but could happen as part of the test suite.
                    panic!(
                        "RunnableSaga: failed to wait for completion \
                         watcher task (is tokio runtime shutting down?): {}",
                        InlineErrorChain::new(&error),
                    );
                }
            }
        }
        .boxed();

        Ok(RunningSaga { id: self.id, saga_completion_future, log: self.log })
    }

    /// Start the saga running and wait for it to complete.
    ///
    /// This is a shorthand for `start().await?.wait_until_stopped().await`.
    // There is no reason this needs to be limited to tests, but it's only used
    // by the tests today.
    #[cfg(test)]
    pub(crate) async fn run_to_completion(self) -> Result<StoppedSaga, Error> {
        Ok(self.start().await?.wait_until_stopped().await)
    }
}

/// Describes a saga that's started running
pub(crate) struct RunningSaga {
    id: SagaId,
    saga_completion_future: BoxFuture<'static, SagaResult>,
    log: slog::Logger,
}

impl RunningSaga {
    /// Waits until the saga stops executing
    ///
    /// This function waits until the saga stops executing because one of the
    /// following three things happens:
    ///
    /// 1. The saga completes successfully
    ///    ([`nexus_types::internal_api::views::SagaState::Succeeded`]).
    /// 2. The saga fails and unwinding completes without errors
    ///    ([`nexus_types::internal_api::views::SagaState::Failed`]).
    /// 3. The saga fails and then an error is encountered during unwinding
    ///    ([`nexus_types::internal_api::views::SagaState::Stuck`]).
    ///
    /// Steno continues running the saga (and this function continues waiting)
    /// until one of those three things happens.  Once any of those things
    /// happens, the saga is no longer running and this function returns a
    /// `StoppedSaga` that you can use to inspect more precisely what happened.
    pub(crate) async fn wait_until_stopped(self) -> StoppedSaga {
        let result = self.saga_completion_future.await;
        info!(self.log, "saga finished"; "saga_result" => ?result);
        StoppedSaga { id: self.id, result, log: self.log }
    }
}

/// Describes a saga that's finished
pub(crate) struct StoppedSaga {
    id: SagaId,
    result: SagaResult,
    log: slog::Logger,
}

impl StoppedSaga {
    /// Fetches the raw Steno result for the saga's execution
    pub(crate) fn into_raw_result(self) -> SagaResult {
        self.result
    }

    /// Interprets the result of saga execution as a `Result` whose error type
    /// is `Error`.
    pub(crate) fn into_omicron_result(self) -> Result<SagaResultOk, Error> {
        self.result.kind.map_err(|saga_error| {
            let mut error = saga_error
                .error_source
                .convert::<Error>()
                .unwrap_or_else(|e| Error::internal_error(&e.to_string()))
                .internal_context(format!(
                    "saga ACTION error at node {:?}",
                    saga_error.error_node_name
                ));
            if let Some((undo_node, undo_error)) = saga_error.undo_failure {
                error = error.internal_context(format!(
                    "UNDO ACTION failed (node {:?}, error {:#}) after",
                    undo_node, undo_error
                ));

                // TODO this log message does not belong here because if the
                // caller isn't checking this then we won't log it.  We should
                // probably make Steno log this since there may be no place in
                // Nexus that's waiting for a given saga to finish.
                error!(self.log, "saga stuck";
                    "saga_id" => self.id.to_string(),
                    "error" => #%error,
                );
            }

            error
        })
    }
}

/// Handle to a running saga that's used to update our quiesce state when the
/// saga finishes
struct RunningSagaReference {
    saga_id: steno::SagaId,
    quiesce: watch::Sender<Quiesce>,
}

impl Drop for RunningSagaReference {
    fn drop(&mut self) {
        self.quiesce.send_modify(|q| {
            q.sagas_running
                .remove(&self.saga_id)
                .expect("saga was previously recorded running");
        });
    }
}

impl super::Nexus {
    /// Lists sagas currently managed by this Nexus instance
    pub(crate) async fn sagas_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<nexus_types::internal_api::views::Saga> {
        // The endpoint we're serving only supports `ScanById`, which only
        // supports an ascending scan.
        bail_unless!(
            pagparams.direction == dropshot::PaginationOrder::Ascending
        );
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let marker = pagparams.marker.map(|s| SagaId::from(*s));
        let saga_list = self
            .sagas
            .sec_client
            .saga_list(marker, pagparams.limit)
            .await
            .into_iter()
            .map(nexus_types::internal_api::views::Saga::from)
            .map(Ok);
        Ok(futures::stream::iter(saga_list).boxed())
    }

    /// Fetch information about a saga currently managed by this Nexus instance
    pub(crate) async fn saga_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> LookupResult<nexus_types::internal_api::views::Saga> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.sagas
            .sec_client
            .saga_get(SagaId::from(id))
            .await
            .map(nexus_types::internal_api::views::Saga::from)
            .map(Ok)
            .map_err(|_: ()| {
                Error::not_found_by_id(ResourceType::SagaDbg, &id)
            })?
    }

    /// For testing only: provides direct access to the underlying SecClient so
    /// that tests can inject errors
    #[cfg(test)]
    pub(crate) fn sec(&self) -> &steno::SecClient {
        &self.sagas.sec_client
    }

    pub(crate) async fn saga_demo_create(&self) -> Result<DemoSaga, Error> {
        use crate::app::sagas::demo;
        let demo_saga_id = DemoSagaUuid::new_v4();
        let saga_params = demo::Params { id: demo_saga_id };
        let saga_dag = create_saga_dag::<demo::SagaDemo>(saga_params)?;
        let runnable_saga = self.sagas.saga_prepare(saga_dag).await?;
        let saga_id = runnable_saga.id().0;
        // We don't need the handle that runnable_saga.start() returns because
        // we're not going to wait for the saga to finish here.
        let _ = runnable_saga.start().await?;

        let mut demo_sagas = self.demo_sagas()?;
        demo_sagas.preregister(demo_saga_id);

        Ok(DemoSaga { saga_id, demo_saga_id })
    }

    pub(crate) fn saga_demo_complete(
        &self,
        demo_saga_id: DemoSagaUuid,
    ) -> Result<(), Error> {
        let mut demo_sagas = self.demo_sagas()?;
        demo_sagas.complete(demo_saga_id)
    }
}
