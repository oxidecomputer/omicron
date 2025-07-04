// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{HttpError, HttpResponse, RequestContext};
use nexus_db_model::{AuditLogCompletion, AuditLogEntry, AuditLogEntryInit};
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::{
    CreateResult, DataPageParams, ListResultVec, UpdateResult,
};
use uuid::Uuid;

use crate::context::ApiContext;

impl super::Nexus {
    pub(crate) async fn audit_log_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
        start_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> ListResultVec<AuditLogEntry> {
        self.db_datastore
            .audit_log_list(opctx, pagparams, start_time, end_time)
            .await
    }

    pub(crate) async fn audit_log_entry_init(
        &self,
        opctx: &OpContext,
        // TODO: not sure we want the app layer to be aware of RequestContext.
        // might be better to extract the relevant fields at the call site. still
        // would want a helper to avoid duplication
        rqctx: &RequestContext<ApiContext>,
    ) -> CreateResult<AuditLogEntryInit> {
        let actor = opctx.authn.actor();
        let entry = AuditLogEntryInit::new(
            rqctx.request_id.clone(),
            rqctx.endpoint.operation_id.clone(),
            rqctx.request.uri().to_string(),
            rqctx.request.remote_addr().ip(),
            actor.map(|a| a.actor_id()),
            actor.and_then(|a| a.silo_id()),
            opctx.authn.scheme_used().map(|s| s.to_string()),
        );
        self.db_datastore.audit_log_entry_init(opctx, entry).await
    }

    // set duration and result on an existing entry
    pub(crate) async fn audit_log_entry_complete<R: HttpResponse>(
        &self,
        opctx: &OpContext,
        entry: &AuditLogEntryInit,
        result: &Result<R, HttpError>,
    ) -> UpdateResult<()> {
        let status_code = match result {
            Ok(response) => response.status_code(),
            Err(error) => error.status_code.as_status(),
        }
        .as_u16();
        let update = AuditLogCompletion::new(status_code);
        self.db_datastore.audit_log_entry_complete(opctx, &entry, update).await
    }
}
