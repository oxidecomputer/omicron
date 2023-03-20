// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared state used by API request handlers
use super::authn;
use super::authz;
use crate::authn::external::session_cookie::Session;
use crate::authn::ConsoleSessionWithSiloId;
use crate::authz::AuthorizedResource;
use crate::db::DataStore;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;
use uuid::Uuid;

/// Provides general facilities scoped to whatever operation Nexus is currently
/// doing
///
/// The idea is that whatever code path you're looking at in Nexus, it should
/// eventually have an OpContext that allows it to:
///
/// - log a message (with relevant operation-specific metadata)
/// - bump a counter (exported via Oximeter)
/// - emit tracing data
/// - do an authorization check
///
/// OpContexts are constructed when Nexus begins doing something.  This is often
/// when it starts handling an API request, but it could be when starting a
/// background operation or something else.
// Not all of these fields are used yet, but they may still prove useful for
// debugging.
#[allow(dead_code)]
pub struct OpContext {
    pub log: slog::Logger,
    pub authn: Arc<authn::Context>,

    authz: authz::Context,
    created_instant: Instant,
    created_walltime: SystemTime,
    metadata: BTreeMap<String, String>,
    kind: OpKind,
}

pub enum OpKind {
    /// Handling an external API request
    ExternalApiRequest,
    /// Handling an internal API request
    InternalApiRequest,
    /// Executing a saga activity
    Saga,
    /// Background operations in Nexus
    Background,
    /// Automated testing (unit tests and integration tests)
    Test,
}

impl OpContext {
    pub fn new<E>(
        log: &slog::Logger,
        auth_fn: impl FnOnce() -> Result<(Arc<authn::Context>, authz::Context), E>,
        metadata_loader: impl FnOnce(&mut BTreeMap<String, String>),
        kind: OpKind,
    ) -> Result<Self, E> {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let (authn, authz) = auth_fn()?;
        let (log, mut metadata) =
            OpContext::log_and_metadata_for_authn(log, &authn);
        metadata_loader(&mut metadata);

        Ok(OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata,
            kind,
        })
    }

    pub async fn new_async<E>(
        log: &slog::Logger,
        auth_fut: impl std::future::Future<
            Output = Result<(Arc<authn::Context>, authz::Context), E>,
        >,
        metadata_loader: impl FnOnce(&mut BTreeMap<String, String>),
        kind: OpKind,
    ) -> Result<Self, E> {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let (authn, authz) = auth_fut.await?;
        let (log, mut metadata) =
            OpContext::log_and_metadata_for_authn(log, &authn);
        metadata_loader(&mut metadata);

        Ok(OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata,
            kind,
        })
    }

    fn log_and_metadata_for_authn(
        log: &slog::Logger,
        authn: &authn::Context,
    ) -> (slog::Logger, BTreeMap<String, String>) {
        let mut metadata = BTreeMap::new();

        let log = if let Some(actor) = authn.actor() {
            let actor_id = actor.actor_id();
            metadata
                .insert(String::from("authenticated"), String::from("true"));
            metadata.insert(String::from("actor"), format!("{:?}", actor));

            log.new(
                o!("authenticated" => true, "actor_id" => actor_id.to_string()),
            )
        } else {
            metadata
                .insert(String::from("authenticated"), String::from("false"));
            log.new(o!("authenticated" => false))
        };

        (log, metadata)
    }

    pub fn load_request_metadata<T: Send + Sync + 'static>(
        rqctx: &dropshot::RequestContext<T>,
        metadata: &mut BTreeMap<String, String>,
    ) {
        let request = &rqctx.request;
        metadata.insert(String::from("request_id"), rqctx.request_id.clone());
        metadata
            .insert(String::from("http_method"), request.method().to_string());
        metadata.insert(String::from("http_uri"), request.uri().to_string());
    }

    /// Returns a context suitable for use in background operations in Nexus
    pub fn for_background(
        log: slog::Logger,
        authz: Arc<authz::Authz>,
        authn: authn::Context,
        datastore: Arc<DataStore>,
    ) -> OpContext {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let authn = Arc::new(authn);
        let authz = authz::Context::new(
            Arc::clone(&authn),
            Arc::clone(&authz),
            Arc::clone(&datastore),
        );
        OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata: BTreeMap::new(),
            kind: OpKind::Background,
        }
    }

    /// Returns a context suitable for automated tests where an OpContext is
    /// needed outside of a Dropshot context
    // Ideally this would only be exposed under `#[cfg(test)]`.  However, it's
    // used by integration tests (via `app::test_interfaces::TestInterfaces`) in
    // order to construct OpContexts that let them observe and muck with state
    // outside public interfaces.
    pub fn for_tests(
        log: slog::Logger,
        datastore: Arc<DataStore>,
    ) -> OpContext {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let authn = Arc::new(authn::Context::privileged_test_user());
        let authz = authz::Context::new(
            Arc::clone(&authn),
            Arc::new(authz::Authz::new(&log)),
            Arc::clone(&datastore),
        );
        OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata: BTreeMap::new(),
            kind: OpKind::Test,
        }
    }

    /// Check whether the actor performing this request is authorized for
    /// `action` on `resource`.
    pub async fn authorize<Resource>(
        &self,
        action: authz::Action,
        resource: &Resource,
    ) -> Result<(), Error>
    where
        Resource: AuthorizedResource + Debug + Clone,
    {
        // TODO-cleanup In an ideal world, Oso would consume &Action and
        // &Resource.  Instead, it consumes owned types.  As a result, they're
        // not available to us (even for logging) after we make the authorize()
        // call.  We work around this by cloning.
        trace!(self.log, "authorize begin";
            "actor" => ?self.authn.actor(),
            "action" => ?action,
            "resource" => ?*resource
        );
        let result = self.authz.authorize(self, action, resource.clone()).await;
        debug!(self.log, "authorize result";
            "actor" => ?self.authn.actor(),
            "action" => ?action,
            "resource" => ?*resource,
            "result" => ?result,
        );
        result
    }
}

impl Session for ConsoleSessionWithSiloId {
    fn silo_user_id(&self) -> Uuid {
        self.console_session.silo_user_id
    }
    fn silo_id(&self) -> Uuid {
        self.silo_id
    }
    fn time_last_used(&self) -> DateTime<Utc> {
        self.console_session.time_last_used
    }
    fn time_created(&self) -> DateTime<Utc> {
        self.console_session.time_created
    }
}

#[cfg(test)]
mod test {
    use super::OpContext;
    use crate::authn;
    use crate::authz;
    use authz::Action;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_background_context() {
        let logctx = dev::test_setup_log("test_background_context");
        let mut db = test_setup_database(&logctx.log).await;
        let (_, datastore) =
            crate::db::datastore::datastore_test(&logctx, &db).await;
        let opctx = OpContext::for_background(
            logctx.log.new(o!()),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::internal_unauthenticated(),
            datastore,
        );

        // This is partly a test of the authorization policy.  Today, background
        // contexts should have no privileges.  That's misleading because in
        // fact they do a bunch of privileged things, but we haven't yet added
        // privilege checks to those code paths.  Eventually we'll probably want
        // to define a particular internal user (maybe even a different one for
        // different background contexts) with specific privileges and test
        // those here.
        //
        // For now, we check what we currently expect, which is that this
        // context has no official privileges.
        let error = opctx
            .authorize(Action::Query, &authz::DATABASE)
            .await
            .expect_err("expected authorization error");
        assert!(matches!(error, Error::Unauthenticated { .. }));
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_test_context() {
        let logctx = dev::test_setup_log("test_background_context");
        let mut db = test_setup_database(&logctx.log).await;
        let (_, datastore) =
            crate::db::datastore::datastore_test(&logctx, &db).await;
        let opctx = OpContext::for_tests(logctx.log.new(o!()), datastore);

        // Like in test_background_context(), this is essentially a test of the
        // authorization policy.  The unit tests assume this user can do
        // basically everything.  We don't need to verify that -- the tests
        // themselves do that -- but it's useful to have a basic santiy test
        // that we can construct such a context it's authorized to do something.
        opctx
            .authorize(Action::Query, &authz::DATABASE)
            .await
            .expect("expected authorization to succeed");
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
