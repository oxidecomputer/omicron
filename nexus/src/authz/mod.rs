//! Authorization facilities

use crate::authn;
use omicron_common::api::external::Error;
use oso::Oso;
use std::sync::Arc;

mod oso_types;
pub use oso_types::Action;
pub use oso_types::Organization;
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
    /// `action` on `resource`.  For "read" operations, use authorize_read()
    /// instead.
    pub fn authorize<Resource>(
        &self,
        action: Action,
        resource: Resource,
    ) -> Result<(), Error>
    where
        Resource: oso::ToPolar,
    {
        // XXX Add an assertion that action != "read"
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
        // callers if we didn't return ApiError here.
        let actor = oso_types::AnyActor::from(&*self.authn);
        match self.authz.oso.is_allowed(actor, action, resource) {
            Err(error) => Err(Error::internal_error(&format!(
                "failed to compute authorization: {:#}",
                error
            ))),
            Ok(false) => Err(Error::Forbidden),
            Ok(true) => Ok(()),
        }
    }
}
