// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Guts of the authorization subsystem

use super::actor::AnyActor;
use super::roles::RoleSet;
use crate::authn;
use crate::authz::oso_generic;
use crate::authz::Action;
use crate::context::OpContext;
use crate::db::DataStore;
use futures::future::BoxFuture;
use omicron_common::api::external::Error;
use oso::Oso;
use oso::OsoError;
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

    // TODO-cleanup This should not be exposed outside the `authz` module.
    pub fn is_allowed<R>(
        &self,
        actor: &AnyActor,
        action: Action,
        resource: &R,
    ) -> Result<bool, OsoError>
    where
        R: oso::ToPolar + Clone,
    {
        self.oso.is_allowed(actor.clone(), action, resource.clone())
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
        Resource: AuthorizedResource + Clone,
    {
        let mut roles = RoleSet::new();
        resource
            .load_roles(opctx, &self.datastore, &self.authn, &mut roles)
            .await?;
        debug!(opctx.log, "roles"; "roles" => ?roles);
        let actor = AnyActor::new(&self.authn, roles);
        let is_authn = self.authn.actor().is_some();
        match self.authz.is_allowed(&actor, action, &resource) {
            Ok(true) => Ok(()),
            Err(error) => Err(Error::internal_error(&format!(
                "failed to compute authorization: {:#}",
                error
            ))),
            Ok(false) => {
                let error = if is_authn {
                    Error::Forbidden
                } else {
                    // If the user did not authenticate successfully, this will
                    // become a 401 rather than a 403.
                    Error::Unauthenticated {
                        internal_message: String::from(
                            "authorization failed for unauthenticated request",
                        ),
                    }
                };

                Err(resource.on_unauthorized(&self.authz, error, actor, action))
            }
        }
    }
}

pub trait AuthorizedResource: oso::ToPolar + Send + Sync + 'static {
    /// Find all roles for the user described in `authn` that might be used to
    /// make an authorization decision on `self` (a resource)
    ///
    /// You can imagine that this function would first find roles that are
    /// explicitly associated with this resource in the database.  Then it would
    /// also find roles associated with its parent, since, for example, an
    /// Organization Administrator can access things within Projects in the
    /// organization.  This process continues up the hierarchy.
    ///
    /// That's how this works for most resources.  There are other kinds of
    /// resources (like the Database itself) that aren't stored in the database
    /// and for which a different mechanism might be used.
    fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
        &'a self,
        opctx: &'b OpContext,
        datastore: &'c DataStore,
        authn: &'d authn::Context,
        roleset: &'e mut RoleSet,
    ) -> BoxFuture<'f, Result<(), Error>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        'd: 'f,
        'e: 'f;

    /// Invoked on authz failure to determine the final authz result
    ///
    /// This is used for some resources to check if the actor should be able to
    /// even see them and produce an appropriate error if not
    fn on_unauthorized(
        &self,
        authz: &Authz,
        error: Error,
        actor: AnyActor,
        action: Action,
    ) -> Error;
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
