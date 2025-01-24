// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::RequestContext;
use nexus_db_model::{AuditLogCompletion, AuditLogEntry, AuditLogEntryInit};
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::{
    CreateResult, DataPageParams, ListResultVec, UpdateResult,
};
use uuid::Uuid;

use crate::context::ApiContext;

impl super::Nexus {
    // Currently this pulls from CRDB only, but the idea is that we are
    // only storing recent entries in CRDB and moving the data in batches
    // to clickhouse in a job. In that case we would need to look at both
    // clickhouse and CRDB. We could potentially skip the CRDB part if we're
    // confident the range excludes CRDB data, but it's probably simpler to
    // always check CRDB.

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
            rqctx.request.remote_addr().ip().to_string(),
            actor.map(|a| a.actor_id()),
            actor.and_then(|a| a.silo_id()),
            opctx.authn.scheme_used().map(|s| s.to_string()),
        );
        self.db_datastore.audit_log_entry_init(opctx, entry).await
    }

    // set duration and result on an existing entry
    pub(crate) async fn audit_log_entry_complete(
        /* id, duration, result */
        &self,
        opctx: &OpContext,
        entry: &AuditLogEntryInit,
    ) -> UpdateResult<()> {
        let update = AuditLogCompletion::new();
        self.db_datastore.audit_log_entry_complete(opctx, &entry, update).await
    }
}
