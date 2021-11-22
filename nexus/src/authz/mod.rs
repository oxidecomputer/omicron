//! Authorization facilities

use crate::authn;
use omicron_common::api::external::Error;
use oso::Oso;
use std::sync::Arc;

mod oso_types;
pub use oso_types::Action;
pub use oso_types::Organization;
pub use oso_types::Project;
pub use oso_types::ProjectChild;
pub use oso_types::DATABASE;
pub use oso_types::FLEET;

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
        let oso = oso_types::make_omicron_oso().expect("initializing Oso");
        Authz { oso }
    }
}

/// Operation-specific authorization context
pub struct Context {
    authn: Arc<authn::Context>,
    authz: Arc<Authz>,
}

impl Context {
    pub fn new(authn: Arc<authn::Context>, authz: Arc<Authz>) -> Context {
        Context { authn, authz }
    }

    /// Check whether the actor performing this request is authorized for
    /// `action` on `resource`.
    pub fn authorize<Resource>(
        &self,
        action: Action,
        resource: Resource,
    ) -> Result<(), Error>
    where
        Resource: oso::ToPolar,
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
        let actor = oso_types::AnyActor::from(&*self.authn);
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
    use super::Action;
    use super::Authz;
    use super::Context;
    use super::DATABASE;
    use super::FLEET;
    use crate::authn::TEST_USER_UUID_PRIVILEGED;
    use crate::authn::TEST_USER_UUID_UNPRIVILEGED;
    use std::sync::Arc;

    fn authz_context_for_actor(actor_id_str: &str) -> Context {
        let actor_id = actor_id_str.parse().expect("bad actor uuid in test");
        let authn = crate::authn::Context::test_context_for_actor(actor_id);
        let authz = Authz::new();
        Context::new(Arc::new(authn), Arc::new(authz))
    }

    fn authz_context_noauth() -> Context {
        let authn = crate::authn::Context::internal_unauthenticated();
        let authz = Authz::new();
        Context::new(Arc::new(authn), Arc::new(authz))
    }

    #[test]
    fn test_database() {
        let authz_privileged =
            authz_context_for_actor(TEST_USER_UUID_PRIVILEGED);
        authz_privileged
            .authorize(Action::Query, DATABASE)
            .expect("expected privileged user to be able to query database");
        let authz_nobody = authz_context_for_actor(TEST_USER_UUID_UNPRIVILEGED);
        authz_nobody
            .authorize(Action::Query, DATABASE)
            .expect("expected unprivileged user to be able to query database");
        let authz_noauth = authz_context_noauth();
        authz_noauth.authorize(Action::Query, DATABASE).expect_err(
            "expected unauthenticated user not to be able to query database",
        );
    }

    #[test]
    fn test_organization() {
        let authz_privileged =
            authz_context_for_actor(TEST_USER_UUID_PRIVILEGED);
        authz_privileged.authorize(Action::CreateChild, FLEET).expect(
            "expected privileged user to be able to create organization",
        );
        let authz_nobody = authz_context_for_actor(TEST_USER_UUID_UNPRIVILEGED);
        authz_nobody.authorize(Action::CreateChild, FLEET).expect_err(
            "expected unprivileged user not to be able to create organization",
        );
        let authz_noauth = authz_context_noauth();
        authz_noauth.authorize(Action::Query, DATABASE).expect_err(
            "expected unauthenticated user not to be able \
            to create organization",
        );
    }
}
