// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{HttpError, HttpResponse, RequestContext};
use nexus_db_model::{
    AuditLogActor, AuditLogCompletion, AuditLogEntry, AuditLogEntryInit,
    AuditLogEntryInitParams,
};
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::{
    CreateResult, DataPageParams, ListResultVec, UpdateResult,
};
use omicron_common::backoff;
use std::time::Duration;
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

    /// Use for authenticated operations because we want to pull the actor from
    /// the opctx.
    pub(crate) async fn audit_log_entry_init(
        &self,
        opctx: &OpContext,
        // This file is the only place we pass the entire request context into
        // a nexus app layer function. It seems fine, but if we wanted to avoid
        // that, we could instead give this function a million arguments and
        // extract the relevant fields at the call site.
        rqctx: &RequestContext<ApiContext>,
    ) -> CreateResult<AuditLogEntryInit> {
        // for now, this conversion is pretty much 1-1
        let actor = match opctx.authn.actor() {
            Some(nexus_auth::authn::Actor::UserBuiltin { user_builtin_id }) => {
                AuditLogActor::UserBuiltin { user_builtin_id: *user_builtin_id }
            }
            Some(nexus_auth::authn::Actor::SiloUser {
                silo_user_id,
                silo_id,
            }) => AuditLogActor::SiloUser {
                silo_user_id: *silo_user_id,
                silo_id: *silo_id,
            },
            None => AuditLogActor::Unauthenticated,
        };

        self.audit_log_entry_init_inner(&opctx, actor, rqctx).await
    }

    /// For authenticated operations, we can pull the actor out of the opctx
    /// and have it be the actor we intend (the user). For unauthenticated
    /// requests like login attempts, the actor on the opctx is the built-in
    /// external-authenticator user, which would be misleading to consider the
    /// actor for the request. So for those operations we ignore the opctx.
    pub(crate) async fn audit_log_entry_init_unauthed(
        &self,
        opctx: &OpContext,
        rqctx: &RequestContext<ApiContext>,
    ) -> CreateResult<AuditLogEntryInit> {
        let actor = AuditLogActor::Unauthenticated;
        self.audit_log_entry_init_inner(&opctx, actor, rqctx).await
    }

    async fn audit_log_entry_init_inner(
        &self,
        opctx: &OpContext,
        actor: AuditLogActor,
        rqctx: &RequestContext<ApiContext>,
    ) -> CreateResult<AuditLogEntryInit> {
        // User agent is truncated for the DB because it can theoretically be
        // very long, but almost never contains useful info past the beginning.
        let user_agent = rqctx
            .request
            .headers()
            .get(http::header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .map(|s| safe_truncate(s, 255).to_string());

        let entry_params = AuditLogEntryInitParams {
            request_id: rqctx.request_id.clone(),
            operation_id: rqctx.endpoint.operation_id.clone(),
            request_uri: rqctx.request.uri().to_string(),
            source_ip: rqctx.request.remote_addr().ip(),
            user_agent,
            actor,
            access_method: opctx.authn.scheme_used().map(|s| s.to_string()),
        };
        self.db_datastore.audit_log_entry_init(opctx, entry_params.into()).await
    }

    /// Complete an existing audit log entry with result info like end time,
    /// HTTP status code, error message, etc. Note we retry write failures
    /// because we really want this to go through.
    pub(crate) async fn audit_log_entry_complete<R: HttpResponse>(
        &self,
        opctx: &OpContext,
        entry: &AuditLogEntryInit,
        result: &Result<R, HttpError>,
    ) -> UpdateResult<()> {
        let completion = match result {
            Ok(response) => AuditLogCompletion::Success {
                http_status_code: response.status_code().as_u16(),
            },
            Err(error) => AuditLogCompletion::Error {
                http_status_code: error.status_code.as_status().as_u16(),
                error_code: error.error_code.clone(),
                error_message: error.external_message.clone(),
            },
        };

        // Should retry at roughly 250ms, 750ms, 1750ms (plus however long the
        // tries take). We really want this write to go through.
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_multiplier(2.0)
            .with_initial_interval(Duration::from_millis(250))
            .with_max_elapsed_time(Some(Duration::from_secs(5)))
            .build();

        let mut count = 0;
        backoff::retry_notify(
            backoff,
            || async {
                self.db_datastore
                    .audit_log_entry_complete(opctx, &entry, completion.clone().into())
                    .await
                    .map_err(backoff::BackoffError::transient)
            },
            |error, delay| {
                let id = entry.id;
                count += 1;
                warn!(
                    self.log,
                    "failed attempt to complete audit log entry {id}. retry {count} in {delay:?}";
                    "error" => ?error
                );
            },
        )
        .await
        .map_err(|err| {
            error!(
                self.log,
                "failed all attempts to complete audit log entry {}", entry.id;
                "error" => ?err
            );
            err
        })
    }
}
