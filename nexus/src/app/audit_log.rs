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
fn safe_truncate(s: &str, max: usize) -> String {
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1; // back up until we hit a boundary
    }
    s[..end].to_string()
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
            Some(nexus_auth::authn::Actor::UserBuiltin { 
                user_builtin_id, user_name 
            }) => {
                AuditLogActor::UserBuiltin { 
                    user_builtin_id: *user_builtin_id,
                    user_name: user_name.to_string(),
                }
            }
            Some(nexus_auth::authn::Actor::SiloUser {
                silo_user_id,
                silo_id,
                silo_name,
            }) => AuditLogActor::SiloUser {
                silo_user_id: *silo_user_id,
                silo_id: *silo_id,
                silo_name: silo_name.to_string(),
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

    // A note on the handling of request URI: request.request.uri() is a
    // http::Uri, which contains the scheme and host only if they are in the
    // HTTP request line itself, i.e., only for HTTP/2 requests. So for HTTP/1.1
    // requests, all we'll have is a path. We are truncating it because it can
    // be arbitrarily long in theory, and we don't want to let people jam very
    // long strings into the DB.
    //
    // We could use the authority_for_request helper defined elsewhere to pull
    // the authority out of either the URI or the host header as appropriate
    // and log that in a dedicated column. In that case I think we would want
    // to log uri().path_and_query() instead of the full URI -- the only problem
    // is that path_and_query() returns an option, so we'd need to decide what
    // to fall back to, though in practice I don't think it's possible for it to
    // come back as `None` because every operation we audit log has a path.
    //
    // We should also consider redacting query strings or at least building in
    // some tooling to help us make sure we're not logging anything sensitive.

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
            .map(|s| safe_truncate(s, 256));

        let auth_method = match actor {
            // practically speaking, there is currently no operation that will
            // cause this method to be called with a built-in user
            AuditLogActor::UserBuiltin { .. }
            | AuditLogActor::SiloUser { .. } => {
                opctx.authn.scheme_used().map(|s| s.to_string())
            }
            // if we tried to pull it off the opctx this would be None anyway,
            // but it's better to be explicit
            AuditLogActor::Unauthenticated => None,
        };

        let entry_params = AuditLogEntryInitParams {
            request_id: rqctx.request_id.clone(),
            operation_id: rqctx.endpoint.operation_id.clone(),
            request_uri: safe_truncate(&rqctx.request.uri().to_string(), 512),
            source_ip: rqctx.request.remote_addr().ip(),
            user_agent,
            actor,
            auth_method,
        };
        self.db_datastore.audit_log_entry_init(opctx, entry_params.into()).await
    }

    /// Complete an existing audit log entry with result info like end time,
    /// HTTP status code, error message, etc. Note we retry write failures
    /// because we really want this to go through, but the caller should
    /// ignore error results because we do not want such a failure to fail the
    /// operation.
    pub(crate) async fn audit_log_entry_complete<R: HttpResponse>(
        &self,
        opctx: &OpContext,
        entry: &AuditLogEntryInit,
        result: &Result<R, HttpError>,
        resource_id: Option<String>,
    ) -> UpdateResult<()> {
        let completion = match result {
            Ok(response) => {
                AuditLogCompletion::Success {
                    http_status_code: response.status_code().as_u16(),
                    resource_id: resource_id,
                }
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
