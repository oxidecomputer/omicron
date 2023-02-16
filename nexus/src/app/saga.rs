// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga management and execution

use super::sagas::NexusSaga;
use super::sagas::SagaInitError;
use super::sagas::ACTION_REGISTRY;
use crate::authz;
use crate::context::OpContext;
use crate::saga_interface::SagaContext;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::StreamExt;
use omicron_common::api::external;
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

/// Encapsulates a saga to be run before we actually start running it
///
/// At this point, we've built the DAG, loaded it into the SEC, etc. but haven't
/// started it running.  This is a useful point to inject errors, inspect the
/// DAG, etc.
pub struct RunnableSaga {
    id: SagaId,
    fut: BoxFuture<'static, SagaResult>,
}

impl RunnableSaga {
    pub fn id(&self) -> SagaId {
        self.id
    }
}

pub fn create_saga_dag<N: NexusSaga>(
    params: N::Params,
) -> Result<SagaDag, Error> {
    let builder = DagBuilder::new(SagaName::new(N::NAME));
    let dag = N::make_saga_dag(&params, builder)?;
    let params = serde_json::to_value(&params).map_err(|e| {
        SagaInitError::SerializeError(String::from("saga params"), e)
    })?;
    Ok(SagaDag::new(dag, params))
}

impl super::Nexus {
    pub async fn sagas_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<external::Saga> {
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
            .map(external::Saga::from)
            .map(Ok);
        Ok(futures::stream::iter(saga_list).boxed())
    }

    pub async fn saga_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> LookupResult<external::Saga> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.sec_client
            .saga_get(SagaId::from(id))
            .await
            .map(external::Saga::from)
            .map(Ok)
            .map_err(|_: ()| {
                Error::not_found_by_id(ResourceType::SagaDbg, &id)
            })?
    }

    pub async fn create_runnable_saga(
        self: &Arc<Self>,
        dag: SagaDag,
    ) -> Result<RunnableSaga, Error> {
        // Construct the context necessary to execute this saga.
        let saga_id = SagaId(Uuid::new_v4());

        self.create_runnable_saga_with_id(dag, saga_id).await
    }

    pub async fn create_runnable_saga_with_id(
        self: &Arc<Self>,
        dag: SagaDag,
        saga_id: SagaId,
    ) -> Result<RunnableSaga, Error> {
        let saga_logger = self.log.new(o!(
            "saga_name" => dag.saga_name().to_string(),
            "saga_id" => saga_id.to_string()
        ));
        let saga_context = Arc::new(Arc::new(SagaContext::new(
            self.clone(),
            saga_logger,
            Arc::clone(&self.authz),
        )));
        let future = self
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
        Ok(RunnableSaga { id: saga_id, fut: future })
    }

    pub async fn run_saga(
        &self,
        runnable_saga: RunnableSaga,
    ) -> Result<SagaResultOk, Error> {
        self.sec_client
            .saga_start(runnable_saga.id)
            .await
            .context("starting saga")
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;

        let result = runnable_saga.fut.await;
        result.kind.map_err(|saga_error| {
            saga_error
                .error_source
                .convert::<Error>()
                .unwrap_or_else(|e| Error::internal_error(&e.to_string()))
                .internal_context(format!(
                    "saga error at node {:?}",
                    saga_error.error_node_name
                ))
        })
    }

    pub fn sec(&self) -> &steno::SecClient {
        &self.sec_client
    }

    /// Given a saga type and parameters, create a new saga and execute it.
    pub(crate) async fn execute_saga<N: NexusSaga>(
        self: &Arc<Self>,
        params: N::Params,
    ) -> Result<SagaResultOk, Error> {
        // Construct the DAG specific to this saga.
        let dag = create_saga_dag::<N>(params)?;

        // Register the saga with the saga executor.
        let runnable_saga = self.create_runnable_saga(dag).await?;

        // Actually run the saga to completion.
        self.run_saga(runnable_saga).await
    }
}
