// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Guts of the authorization subsystem

use super::actor::AnyActor;
use super::roles::RoleSet;
use crate::authn;
use crate::authz::oso_generic;
use crate::authz::Action;
use crate::authz::AuthzResource;
use crate::context::OpContext;
use crate::db::DataStore;
use omicron_common::api::external::Error;
use oso::Oso;
use std::sync::Arc;

/// Server-wide authorization context
pub struct Authz {
    oso: Oso,
}

impl Authz {
    /// Construct an authorization context
    ///
    /// # Panics
    ///
    /// This function panics if we could not load the compiled-in Polar
    /// configuration.  That should be impossible outside of development.
    pub fn new() -> Authz {
        let oso = oso_generic::make_omicron_oso().expect("initializing Oso");
        Authz { oso }
    }
}

/// Operation-specific authorization context
///
/// This is the primary external interface for the authorization subsystem,
/// through which Nexus at-large makes authorization checks.  This is almost
/// always done through [`OpContext::authorize()`].
pub struct Context {
    authn: Arc<authn::Context>,
    authz: Arc<Authz>,
    datastore: Arc<DataStore>,
}

impl Context {
    pub fn new(
        authn: Arc<authn::Context>,
        authz: Arc<Authz>,
        datastore: Arc<DataStore>,
    ) -> Context {
        Context { authn, authz, datastore }
    }

    /// Check whether the actor performing this request is authorized for
    /// `action` on `resource`.
    pub async fn authorize<Resource>(
        &self,
        opctx: &OpContext,
        action: Action,
        resource: Resource,
    ) -> Result<(), Error>
    where
        Resource: oso::ToPolar + AuthzResource,
    {
        // TODO-security For Action::Read (and any other "read" action),
        // this should return NotFound rather than Forbidden.  But we cannot
        // construct the appropriate NotFound here without more information:
        // the resource type and how it was looked up.  In practice, it's
        // quite possible that all such cases should really use query
        // filtering instead of an explicit is_allowed() check, in which
        // case we could safely assume Forbidden here.
        //
        // Alternatively, we could let the caller produce the appropriate
        // "NotFound", but it would add a lot of boilerplate to a lot of
        // callers if we didn't return api::external::Error here.
        let mut roles = RoleSet::new();
        resource
            .fetch_all_related_roles_for_user(
                opctx,
                &self.datastore,
                &self.authn,
                &mut roles,
            )
            .await?;
        debug!(opctx.log, "roles"; "roles" => ?roles);
        let actor = AnyActor::new(&self.authn, roles);
        let is_authn = self.authn.actor().is_some();
        match self.authz.oso.is_allowed(actor, action, resource) {
            Err(error) => Err(Error::internal_error(&format!(
                "failed to compute authorization: {:#}",
                error
            ))),
            // If the user did not authenticate successfully, this will become a
            // 401 rather than a 403.
            Ok(false) if !is_authn => Err(Error::Unauthenticated {
                internal_message: String::from(
                    "authorization failed for unauthenticated request",
                ),
            }),
            Ok(false) => Err(Error::Forbidden),
            Ok(true) => Ok(()),
        }
    }
}

#[cfg(test)]
mod test {
    /*
     * These are essentially unit tests for the policy itself.
     * TODO-coverage This is just a start.  But we need roles to do a more
     * comprehensive test.
     * TODO If this gets any more complicated, we could consider automatically
     * generating the test cases.  We could precreate a bunch of resources and
     * some users with different roles.  Then we could run through a table that
     * says exactly which users should be able to do what to each resource.
     */
    use crate::authn;
    use crate::authz::Action;
    use crate::authz::Authz;
    use crate::authz::Context;
    use crate::authz::DATABASE;
    use crate::authz::FLEET;
    use crate::db::DataStore;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    fn authz_context_for_actor(
        authn: authn::Context,
        datastore: Arc<DataStore>,
    ) -> Context {
        let authz = Authz::new();
        Context::new(Arc::new(authn), Arc::new(authz), datastore)
    }

    fn authz_context_noauth(datastore: Arc<DataStore>) -> Context {
        let authn = authn::Context::internal_unauthenticated();
        let authz = Authz::new();
        Context::new(Arc::new(authn), Arc::new(authz), datastore)
    }

    #[tokio::test]
    async fn test_database() {
        let logctx = dev::test_setup_log("test_database");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            crate::db::datastore::datastore_test(&logctx, &db).await;
        let authz_privileged = authz_context_for_actor(
            authn::Context::internal_test_user(),
            Arc::clone(&datastore),
        );
        authz_privileged
            .authorize(&opctx, Action::Query, DATABASE)
            .await
            .expect("expected privileged user to be able to query database");
        let error = authz_privileged
            .authorize(&opctx, Action::Modify, DATABASE)
            .await
            .expect_err(
                "expected privileged test user not to be able to modify \
                database",
            );
        assert!(matches!(
            error,
            omicron_common::api::external::Error::Forbidden
        ));
        let authz_nobody = authz_context_for_actor(
            authn::Context::test_context_for_actor(
                authn::USER_TEST_UNPRIVILEGED.id,
            ),
            Arc::clone(&datastore),
        );
        authz_nobody
            .authorize(&opctx, Action::Query, DATABASE)
            .await
            .expect("expected unprivileged user to be able to query database");
        let authz_noauth = authz_context_noauth(datastore);
        authz_noauth
            .authorize(&opctx, Action::Query, DATABASE)
            .await
            .expect_err(
            "expected unauthenticated user not to be able to query database",
        );
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_organization() {
        let logctx = dev::test_setup_log("test_organization");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            crate::db::datastore::datastore_test(&logctx, &db).await;

        let authz_privileged = authz_context_for_actor(
            authn::Context::internal_test_user(),
            Arc::clone(&datastore),
        );
        authz_privileged
            .authorize(&opctx, Action::CreateChild, FLEET)
            .await
            .expect(
                "expected privileged user to be able to create organization",
            );
        let authz_nobody = authz_context_for_actor(
            authn::Context::test_context_for_actor(
                authn::USER_TEST_UNPRIVILEGED.id,
            ),
            Arc::clone(&datastore),
        );
        authz_nobody
            .authorize(&opctx, Action::CreateChild, FLEET)
            .await
            .expect_err(
            "expected unprivileged user not to be able to create organization",
        );
        let authz_noauth = authz_context_noauth(datastore);
        authz_noauth
            .authorize(&opctx, Action::Query, DATABASE)
            .await
            .expect_err(
                "expected unauthenticated user not to be able \
            to create organization",
            );
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
