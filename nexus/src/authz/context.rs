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
    pub fn new(log: &slog::Logger) -> Authz {
        let oso = oso_generic::make_omicron_oso(log).expect("initializing Oso");
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
                Err(if !is_authn {
                    // If we failed an authz check, and the user did not
                    // authenticate at all, we report a 401.
                    Error::Unauthenticated {
                        internal_message: String::from(
                            "authorization failed for unauthenticated request",
                        ),
                    }
                } else {
                    // Otherwise, we normally think of this as a 403
                    // "Forbidden".  However, the resource impl may choose to
                    // override that with a 404 to avoid leaking information
                    // about the resource existing.
                    resource.on_unauthorized(
                        &self.authz,
                        Error::Forbidden,
                        actor,
                        action,
                    )
                })
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
