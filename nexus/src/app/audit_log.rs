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

/// Truncate a str to at most `max` bytes, but make sure not to cut any chars
/// in half.
fn safe_truncate(s: &str, max: usize) -> &str {
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1; // back up until we hit a boundary
    }
    &s[..end]
}

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
        // This file is the only place we pass the entire request context into
        // a nexus app layer function. It seems fine, but if we wanted to avoid
        // that, we could instead give this function a million arguments and
        // extract the relevant fields at the call site.
        rqctx: &RequestContext<ApiContext>,
    ) -> CreateResult<AuditLogEntryInit> {
        let actor = opctx.authn.actor();
        let entry = AuditLogEntryInit::new(
            rqctx.request_id.clone(),
            rqctx.endpoint.operation_id.clone(),
            rqctx.request.uri().to_string(),
            rqctx.request.remote_addr().ip(),
            rqctx
                .request
                .headers()
                .get("User-Agent")
                .and_then(|value| value.to_str().ok())
                .map(|s| safe_truncate(s, 255).to_string()),
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
        let (status_code, error_code, error_message) = match result {
            Ok(response) => (response.status_code(), None, None),
            Err(error) => (
                error.status_code.as_status(),
                error.error_code.clone(),
                // For now we only log the external message. If we want to log
                // the internal message, we need to make sure that is ok to do.
                Some(error.external_message.clone()),
            ),
        };

        let update = AuditLogCompletion::new(
            status_code.as_u16(),
            error_code,
            error_message,
        );
        self.db_datastore.audit_log_entry_complete(opctx, &entry, update).await
    }
}
