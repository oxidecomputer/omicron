// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared state used by API request handlers
use super::authn;
use super::authz;
use crate::authn::ConsoleSessionWithSiloId;
use crate::authn::external::session_cookie::Session;
use crate::authz::AuthorizedResource;
use crate::authz::RoleSet;
use crate::storage::Storage;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::ConsoleSessionUuid;
use slog::debug;
use slog::o;
use slog::trace;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

    pub(crate) fn datastore(&self) -> &Arc<dyn Storage> {
        self.authz.datastore()
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

    pub fn load_request_metadata<C: Send + Sync + 'static>(
        rqctx: &dropshot::RequestContext<C>,
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
        datastore: Arc<dyn Storage>,
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
        datastore: Arc<dyn Storage>,
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

    /// Creates a new `OpContext` with extra metadata (including log metadata)
    ///
    /// This is intended for cases where you want an OpContext that's
    /// functionally the same as one that you already have, but where you want
    /// to provide extra debugging information (in the form of key-value pairs)
    /// in both the OpContext itself and its logger.
    pub fn child(&self, new_metadata: BTreeMap<String, String>) -> Self {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let mut metadata = self.metadata.clone();
        let mut log = self.log.clone();

        for (k, v) in new_metadata {
            metadata.insert(k.clone(), v.clone());
            log = log.new(o!(k => v));
        }

        OpContext {
            log,
            authn: self.authn.clone(),
            authz: self.authz.clone(),
            created_instant,
            created_walltime,
            metadata,
            kind: self.kind,
        }
    }

    /// Creates a new `OpContext` just like the given one, but with a different
    /// identity.
    ///
    /// This is only intended for tests.
    pub fn child_with_authn(&self, authn: authn::Context) -> OpContext {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();

        OpContext {
            log: self.log.clone(),
            authn: Arc::new(authn),
            authz: self.authz.clone(),
            created_instant,
            created_walltime,
            metadata: self.metadata.clone(),
            kind: self.kind,
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

    pub async fn get_roles<Resource>(
        &self,
        resource: &Resource,
    ) -> Result<RoleSet, Error>
    where
        Resource: AuthorizedResource + Debug + Clone,
    {
        self.authz.get_roles(self, resource.clone()).await
    }

    /// Returns an error if we're currently in a context where expensive or
    /// complex operations should not be allowed
    ///
    /// This is intended for checking that we're not trying to perform expensive
    /// or complex (multi-step) operations from HTTP request handlers.
    /// Generally, expensive or complex operations should be broken up into
    /// multiple requests (e.g., pagination).  That's for a variety reasons:
    ///
    /// - DoS mitigation: requests that kick off an arbitrarily-large amount of
    ///   work can tie up server resources without requiring commensurate
    ///   resources from the client, which makes it very easy to attack the
    ///   system
    ///
    /// - monitoring: it's easier to reason about metrics for operations that
    ///   are roughly bounded in size (otherwise, things like "requests per
    ///   second" can become meaningless)
    ///
    /// - stability: very large database queries have outsize effects on the
    ///   rest of the system (potentially blocking other clients for extended
    ///   periods of time, or preventing the database from timely cleanup of
    ///   invalidated data, etc.)
    ///
    /// - throttling: separate requests gives us an opportunity to dynamically
    ///   throttle clients that are hitting the system hard
    ///
    /// - failure transparency: when one request kicks off a complex
    ///   (multi-step) operation, it's harder to communicate programmatically
    ///   why the request failed
    ///
    /// - retries: when failures happen during smaller operations, clients can
    ///   retry only the part that failed.  When failures happen during complex
    ///   (multi-step) operations, the client has to retry the whole thing.
    ///   This is much worse than it sounds: it means that for the request to
    ///   succeed, _all_ of the suboperations have to succeed.  During a period
    ///   of transient failures, that could be extremely unlikely.  With smaller
    ///   requests, clients can just retry each one until it succeeds without
    ///   having to retry the requests that already succeeded (whose failures
    ///   would trigger another attempt at the whole thing).
    ///
    /// - Simple request-response HTTP is not well-suited to long-running
    ///   operations.  There's no way to communicate progress or even that the
    ///   request is still going.  There's no good way to cancel such a request,
    ///   either.  Clients and proxies often don't expect long requests and
    ///   apply aggressive timeouts.  Depending on the HTTP version, a
    ///   long-running request can tie up the TCP connection.
    ///
    /// We shouldn't allow these in either internal or external API handlers,
    /// but we currently have some internal APIs for exercising some expensive
    /// blueprint operations and so we allow these cases here.
    pub fn check_complex_operations_allowed(&self) -> Result<(), Error> {
        let api_handler = match self.kind {
            OpKind::ExternalApiRequest => true,
            OpKind::InternalApiRequest
            | OpKind::Saga
            | OpKind::Background
            | OpKind::Test => false,
        };
        if api_handler {
            Err(Error::internal_error(
                "operation not allowed from API handlers",
            ))
        } else {
            Ok(())
        }
    }
}

impl Session for ConsoleSessionWithSiloId {
    fn id(&self) -> ConsoleSessionUuid {
        self.console_session.id()
    }

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
    use nexus_db_model::IdentityType;
    use nexus_db_model::RoleAssignment;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::ResourceType;
    use omicron_test_utils::dev;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use uuid::Uuid;

    struct FakeStorage {}

    impl FakeStorage {
        fn new() -> Arc<dyn crate::storage::Storage> {
            Arc::new(Self {})
        }
    }

    #[async_trait::async_trait]
    impl crate::storage::Storage for FakeStorage {
        async fn role_asgn_list_for(
            &self,
            _opctx: &OpContext,
            _identity_type: IdentityType,
            _identity_id: Uuid,
            _resource_type: ResourceType,
            _resource_id: Uuid,
        ) -> Result<Vec<RoleAssignment>, Error> {
            unimplemented!("This test is not expected to access the database");
        }
    }

    #[tokio::test]
    async fn test_background_context() {
        let logctx = dev::test_setup_log("test_background_context");

        let datastore = FakeStorage::new();
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
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_test_context() {
        let logctx = dev::test_setup_log("test_background_context");
        let datastore = FakeStorage::new();
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
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_child_context() {
        let logctx = dev::test_setup_log("test_child_context");
        let datastore = FakeStorage::new();
        let opctx = OpContext::for_background(
            logctx.log.new(o!()),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::internal_unauthenticated(),
            datastore,
        );

        let child_opctx = opctx.child(BTreeMap::from([
            (String::from("one"), String::from("two")),
            (String::from("three"), String::from("four")),
        ]));
        let grandchild_opctx = opctx.child(BTreeMap::from([
            (String::from("one"), String::from("seven")),
            (String::from("five"), String::from("six")),
        ]));

        // Verify they're the same "kind".
        assert_eq!(opctx.kind, child_opctx.kind);
        assert_eq!(opctx.kind, grandchild_opctx.kind);

        // Verify that both descendants have metadata from the root.
        for (k, v) in opctx.metadata.iter() {
            assert_eq!(v, &child_opctx.metadata[k]);
            assert_eq!(v, &grandchild_opctx.metadata[k]);
        }

        // The child opctx ought to have its own metadata and not any of its
        // child's metadata.
        assert_eq!(child_opctx.metadata["one"], "two");
        assert_eq!(child_opctx.metadata["three"], "four");
        assert!(!child_opctx.metadata.contains_key("five"));

        // The granchild opctx ought to have its own metadata, one key of which
        // overrides its parent's.
        assert_eq!(grandchild_opctx.metadata["one"], "seven");
        assert_eq!(grandchild_opctx.metadata["five"], "six");

        logctx.cleanup_successful();
    }
}
