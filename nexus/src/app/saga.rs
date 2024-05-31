// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga management and execution

use super::sagas::NexusSaga;
use super::sagas::SagaInitError;
use super::sagas::ACTION_REGISTRY;
use crate::saga_interface::SagaContext;
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
use slog::Logger;
use std::sync::Arc;
use steno::DagBuilder;
use steno::SagaDag;
use steno::SagaId;
use steno::SagaName;
use steno::SagaResult;
use steno::SagaResultOk;
use uuid::Uuid;

/// Encapsulates a saga to be run before we actually start running it
///
/// At this point, we've built the DAG, loaded it into the SEC, etc. but haven't
/// started it running.  This is a useful point to inject errors, inspect the
/// DAG, etc.
pub(crate) struct RunnableSaga {
    id: SagaId,
    fut: BoxFuture<'static, SagaResult>,
}

impl RunnableSaga {
    #[cfg(test)]
    pub(crate) fn id(&self) -> SagaId {
        self.id
    }
}

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

/// A mechanism for interacting with the saga execution coordinator (SEC)
/// running in this Nexus instance.
#[derive(Clone)]
pub struct SecClient {
    log: Logger,
    sec_client: Arc<steno::SecClient>,
}

impl SecClient {
    pub(crate) fn new(log: &Logger, sec_client: steno::SecClient) -> SecClient {
        let log = log.new(o!("component" => "SecClient"));
        let sec_client = Arc::new(sec_client);
        SecClient { log, sec_client }
    }
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
            .sec_client
            .saga_list(marker, pagparams.limit)
            .await
            .into_iter()
            .map(nexus_types::internal_api::views::Saga::from)
            .map(Ok);
        Ok(futures::stream::iter(saga_list).boxed())
    }

    pub(crate) async fn saga_get(
        &self,
        opctx: &OpContext,
        id: SagaId,
    ) -> LookupResult<nexus_types::internal_api::views::Saga> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.sec_client
            .saga_get(id)
            .await
            .map(nexus_types::internal_api::views::Saga::from)
            .map(Ok)
            .map_err(|_: ()| {
                Error::not_found_by_id(ResourceType::SagaDbg, &id.0)
            })?
    }

    pub(crate) async fn create_runnable_saga(
        &self,
        dag: SagaDag,
        saga_context: SagaContext,
    ) -> Result<RunnableSaga, Error> {
        // Construct the context necessary to execute this saga.
        let saga_id = SagaId(Uuid::new_v4());

        self.create_runnable_saga_with_id(dag, saga_id, saga_context).await
    }

    pub(crate) async fn create_runnable_saga_with_id(
        &self,
        dag: SagaDag,
        saga_id: SagaId,
        mut saga_context: SagaContext,
    ) -> Result<RunnableSaga, Error> {
        let saga_logger = self.log.new(o!(
            "saga_name" => dag.saga_name().to_string(),
            "saga_id" => saga_id.to_string()
        ));
        saga_context.set_logger(saga_logger);
        let future = self
            .sec_client
            .saga_create(
                saga_id,
                Arc::new(Arc::new(saga_context)),
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
        Ok(RunnableSaga { id: saga_id, fut: future })
    }

    pub(crate) async fn run_saga(
        &self,
        runnable_saga: RunnableSaga,
    ) -> Result<SagaResultOk, Error> {
        let log = &self.log;
        self.sec_client
            .saga_start(runnable_saga.id)
            .await
            .context("starting saga")
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;

        let result = runnable_saga.fut.await;
        result.kind.map_err(|saga_error| {
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

                error!(log, "saga stuck";
                    "saga_id" => runnable_saga.id.to_string(),
                    "error" => #%error,
                );
            }

            error
        })
    }

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
        self.sec_client
            .saga_start(runnable_saga.id)
            .await
            .context("starting saga")
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;

        Ok(runnable_saga.fut.await)
    }

    pub fn sec(&self) -> &Arc<steno::SecClient> {
        &self.sec_client
    }

    /// Given a saga type and parameters, create a new saga and execute it.
    pub(crate) async fn execute_saga<N: NexusSaga>(
        &self,
        params: N::Params,
        saga_context: SagaContext,
    ) -> Result<SagaResultOk, Error> {
        // Construct the DAG specific to this saga.
        let dag = create_saga_dag::<N>(params)?;

        // Register the saga with the saga executor.
        let runnable_saga =
            self.create_runnable_saga(dag, saga_context).await?;

        // Actually run the saga to completion.
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
}
