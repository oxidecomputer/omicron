// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga management and execution

use crate::authz;
use crate::context::OpContext;
use crate::saga_interface::SagaContext;
use anyhow::Context;
use futures::StreamExt;
use omicron_common::api::external;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResult;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use std::sync::Arc;
use steno::SagaDag;
use steno::SagaId;
use steno::SagaResultOk;
use steno::SagaType;
use uuid::Uuid;

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
            .saga_get(steno::SagaId::from(id))
            .await
            .map(external::Saga::from)
            .map(Ok)
            .map_err(|_: ()| {
                Error::not_found_by_id(ResourceType::SagaDbg, &id)
            })?
    }

    /// Given a saga template and parameters, create a new saga and execute it.
    pub(crate) async fn execute_saga<P, S>(
        self: &Arc<Self>,
        saga: SagaDag,
        saga_params: Arc<P>,
    ) -> Result<SagaResultOk, Error>
    where
        S: SagaType<ExecContextType = Arc<SagaContext>>,
        // TODO-cleanup The bound `P: Serialize` should not be necessary because
        // SagaParamsType must already impl Serialize.
        P: serde::Serialize,
    {
        todo!(); // XXX-dap
        //let saga_id = SagaId(Uuid::new_v4());
        //let saga_logger =
        //    self.log.new(o!("template_name" => template_name.to_owned()));
        //let saga_context = Arc::new(Arc::new(SagaContext::new(
        //    Arc::clone(self),
        //    saga_logger,
        //    Arc::clone(&self.authz),
        //)));
        //let future = self
        //    .sec_client
        //    .saga_create(
        //        saga_id,
        //        saga_context,
        //        saga_template,
        //        template_name.to_owned(),
        //        saga_params,
        //    )
        //    .await
        //    .context("creating saga")
        //    .map_err(|error| {
        //        // TODO-error This could be a service unavailable error,
        //        // depending on the failure mode.  We need more information from
        //        // Steno.
        //        Error::internal_error(&format!("{:#}", error))
        //    })?;

        //self.sec_client
        //    .saga_start(saga_id)
        //    .await
        //    .context("starting saga")
        //    .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;

        //let result = future.await;
        //result.kind.map_err(|saga_error| {
        //    saga_error.error_source.convert::<Error>().unwrap_or_else(|e| {
        //        // TODO-error more context would be useful
        //        Error::InternalError { internal_message: e.to_string() }
        //    })
        //})
    }
}
