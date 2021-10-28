//! Authorization facilities

use crate::authn;
use omicron_common::api::external::Error;
use oso::Oso;
use std::sync::Arc;

mod oso_types;
pub use oso_types::Action;
pub use oso_types::Organization;
pub use oso_types::DATABASE;

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
        let actor = oso_types::AnyActor::from(&*self.authn);
        match self.authz.oso.is_allowed(actor, action, resource) {
            Err(error) => Err(Error::internal_error(&format!(
                "failed to compute authorization: {:#}",
                error
            ))),
            // XXX In some cases, this should be NotFound.  These are cases
            // where "action" is a read permission.  But we cannot construct the
            // appropriate NotFound here without more information: the resource
            // type and how it was looked up.  In practice, maybe all of the
            // cases that should work this way ought to use query filtering
            // instead, so we can safely assume Forbidden here.  That remains to
            // be seen.
            Ok(false) => Err(Error::Forbidden),
            Ok(true) => Ok(()),
        }
    }
}
