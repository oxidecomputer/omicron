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
//!       input: saga type (impls [`NexusSaga`])
//!              parameters (specific to the saga's type)
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
//!
//! At the end, you can use [`StoppedSaga::to_omicron_result()`] to get at the
//! success output of the saga or convert any saga failure along the way to an
//! Omicron [`Error`].
//!
//! This interface allows a few different use cases:
//!
//! * A common case is that some code in Nexus wants to do all of this: create
//!   the saga DAG, run it, wait for it to finish, and get the result.
//!   [`Nexus::execute_saga()`] does all this using these lower-level
//!   interfaces.
//! * An expected use case is that some code in Nexus wants to kick off a saga
//!   but not wait for it to finish.  In this case, they can just stop after
//!   calling [`RunnableSaga::start()`].  The saga will continue running; they
//!   just won't be able to directly wait for it to finish or get the result.
//! * Tests can use any of the lower-level pieces to examine intermediate state.

use super::sagas::NexusSaga;
use super::sagas::SagaInitError;
use super::sagas::ACTION_REGISTRY;
use crate::saga_interface::SagaContext;
use crate::Nexus;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::StreamExt;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResult;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use std::sync::Arc;
use steno::DagBuilder;
use steno::SagaDag;
use steno::SagaId;
use steno::SagaName;
use steno::SagaResult;
use steno::SagaResultOk;
use uuid::Uuid;

/// Given a particular kind of Nexus saga (the type parameter `N`) and
/// parameters for that saga, construct a [`SagaDag`] for it
pub(crate) fn create_saga_dag<N: NexusSaga>(
    params: N::Params,
) -> Result<SagaDag, Error> {
    let builder = DagBuilder::new(SagaName::new(N::NAME));
    let dag = N::make_saga_dag(&params, builder)?;
    let params = serde_json::to_value(&params).map_err(|e| {
        SagaInitError::SerializeError(String::from("saga params"), e)
    })?;
    Ok(SagaDag::new(dag, params))
}

/// External handle to a self-contained subsystem for kicking off sagas
///
/// Note that Steno provides its own interface for kicking off sagas.  This one
/// is a thin wrapper around it.  This one exists to layer Nexus-specific
/// behavior on top of Steno's (e.g., error conversion).
pub struct SagaExecutor {
    sec_client: Arc<steno::SecClient>,
    log: slog::Logger,
}

impl SagaExecutor {
    pub fn new(
        sec_client: Arc<steno::SecClient>,
        log: slog::Logger,
    ) -> SagaExecutor {
        SagaExecutor { sec_client, log }
    }

    /// Given a DAG (which has generally been specifically created for a
    /// particular saga and includes the saga's parameters), prepare to start
    /// running the saga.  This does not actually start the saga running.
    pub(crate) async fn saga_prepare(
        &self,
        nexus: Arc<Nexus>,
        dag: SagaDag,
    ) -> Result<RunnableSaga, Error> {
        // Construct the context necessary to execute this saga.
        let saga_id = SagaId(Uuid::new_v4());
        let saga_logger = self.log.new(o!(
            "saga_name" => dag.saga_name().to_string(),
            "saga_id" => saga_id.to_string()
        ));
        let saga_context = Arc::new(Arc::new(SagaContext::new(
            nexus.clone(),
            saga_logger.clone(),
        )));

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
        })
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
}

impl RunnableSaga {
    #[cfg(test)]
    pub(crate) fn id(&self) -> SagaId {
        self.id
    }

    pub(crate) async fn start(self) -> Result<RunningSaga, Error> {
        info!(self.log, "starting saga");
        self.sec_client
            .saga_start(self.id)
            .await
            .context("starting saga")
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;

        Ok(RunningSaga {
            id: self.id,
            saga_completion_future: self.saga_completion_future,
            log: self.log,
        })
    }
}

/// Describes a saga that's been started running
pub(crate) struct RunningSaga {
    id: SagaId,
    saga_completion_future: BoxFuture<'static, SagaResult>,
    log: slog::Logger,
}

impl RunningSaga {
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
    #[cfg(test)]
    pub(crate) fn to_raw_result(self) -> SagaResult {
        self.result
    }

    pub(crate) fn to_omicron_result(self) -> Result<SagaResultOk, Error> {
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

                // XXX-dap this does not belong here because if the caller isn't
                // checking this then we won't log it.
                error!(self.log, "saga stuck";
                    "saga_id" => self.id.to_string(),
                    "error" => #%error,
                );
            }

            error
        })
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

    // XXX-dap convert callers?
    /// Legacy interface -- see [`SagaExecutor::saga_prepare()`]
    pub(crate) async fn create_runnable_saga(
        self: &Arc<Self>,
        dag: SagaDag,
    ) -> Result<RunnableSaga, Error> {
        self.sagas.saga_prepare(self.clone(), dag).await
    }

    // XXX-dap convert callers?
    /// Legacy interface -- see [`SagaExecutor::saga_start()`] and methods on
    /// `RunnableSaga` and `StoppedSaga`.
    pub(crate) async fn run_saga(
        &self,
        runnable_saga: RunnableSaga,
    ) -> Result<SagaResultOk, Error> {
        let running_saga = runnable_saga.start().await?;
        let done_saga = running_saga.wait_until_stopped().await;
        done_saga.to_omicron_result()
    }

    // XXX-dap convert callers?
    /// Starts the supplied `runnable_saga` and, if that succeeded, awaits its
    /// completion and returns the raw `SagaResult`.
    ///
    /// This is a test-only routine meant for use in tests that need to examine
    /// the details of a saga's final state (e.g., examining the exact point at
    /// which it failed). Non-test callers should use `run_saga` instead (it
    /// logs messages on error conditions and has a standard mechanism for
    /// converting saga errors to generic Omicron errors).
    #[cfg(test)]
    pub(crate) async fn run_saga_raw_result(
        &self,
        runnable_saga: RunnableSaga,
    ) -> Result<SagaResult, Error> {
        let running_saga = runnable_saga.start().await?;
        let done_saga = running_saga.wait_until_stopped().await;
        Ok(done_saga.to_raw_result())
    }

    // XXX-dap convert callers?
    /// Legacy interface: given a saga type and parameters, create a new saga
    /// and execute it.
    pub(crate) async fn execute_saga<N: NexusSaga>(
        self: &Arc<Self>,
        params: N::Params,
    ) -> Result<SagaResultOk, Error> {
        // Construct the DAG specific to this saga.
        let dag = create_saga_dag::<N>(params)?;

        // Register the saga with the saga executor.
        let runnable_saga = self.create_runnable_saga(dag).await?;

        // Actually run the saga to completion.
        // XXX-dap
        //
        // XXX: This may loop forever in case `SecStore::record_event` fails.
        // Ideally, `run_saga` wouldn't both start the saga and wait for it to
        // be finished -- instead, it would start off the saga, and then return
        // a notification channel that the caller could use to decide:
        //
        // - either to .await until completion
        // - or to stop waiting after a certain period, while still letting the
        //   saga run in the background.
        //
        // For more, see https://github.com/oxidecomputer/omicron/issues/5406
        // and the note in `sec_store.rs`'s `record_event`.
        self.run_saga(runnable_saga).await
    }

    /// For testing only: provides direct access to the underlying SecClient so
    /// that tests can inject errors
    #[cfg(test)]
    pub fn sec(&self) -> &steno::SecClient {
        &self.sagas.sec_client
    }
}
