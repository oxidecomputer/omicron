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
use omicron_common::bail_unless;
use oso::Oso;
use oso::OsoError;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Server-wide authorization context
pub struct Authz {
    oso: Oso,
    class_names: BTreeSet<String>,
}

impl Authz {
    /// Construct an authorization context
    ///
    /// # Panics
    ///
    /// This function panics if we could not load the compiled-in Polar
    /// configuration.  That should be impossible outside of development.
    pub fn new(log: &slog::Logger) -> Authz {
        let oso_init =
            oso_generic::make_omicron_oso(log).expect("initializing Oso");
        Authz { oso: oso_init.oso, class_names: oso_init.class_names }
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

    #[cfg(test)]
    pub fn into_class_names(self) -> BTreeSet<String> {
        self.class_names
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
        // If we're given a resource whose PolarClass was never registered with
        // Oso, then the call to `is_allowed()` below will always return false
        // (indicating that the actor does not have permissions).  That will
        // cause this function to return an authz failure error (401, 403, or
        // 404, depending on the context).  This is never what we intend.
        // What's likely happened is that somebody forgot to register the class
        // with Oso.  This failure mode is very hard to debug because the Rust
        // code generates a valid Polar snippet and there's a working PolarClass
        // impl -- it's just that neither was ever given to Oso.  Make this
        // failure mode more debuggable by reporting a 500 with a clear error
        // message.  After all, this is a bug.  (We could panic, since it's more
        // of a programmer error than an operational error.  But unlike most
        // programmer errors, the nature of the problem and the blast radius are
        // well understood, so we may as well avoid crashing.)
        let class_name = &resource.polar_class().name;
        bail_unless!(
            self.authz.class_names.contains(class_name),
            "attempted authz check on unregistered resource: {:?}",
            class_name
        );

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
    /// Silo Administrator can access things within Projects in the
    /// silo.  This process continues up the hierarchy.
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

    /// Returns the Polar class that implements this resource
    fn polar_class(&self) -> oso::Class;
}

#[cfg(test)]
mod test {
    use crate::authn;
    use crate::authz::Action;
    use crate::authz::Authz;
    use crate::authz::Context;
    use crate::db::DataStore;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    fn authz_context_for_actor(
        log: &slog::Logger,
        authn: authn::Context,
        datastore: Arc<DataStore>,
    ) -> Context {
        let authz = Authz::new(log);
        Context::new(Arc::new(authn), Arc::new(authz), datastore)
    }

    #[tokio::test]
    async fn test_unregistered_resource() {
        let logctx = dev::test_setup_log("test_unregistered_resource");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            crate::db::datastore::datastore_test(&logctx, &db).await;

        // Define a resource that we "forget" to register with Oso.
        use super::AuthorizedResource;
        use crate::authz::actor::AnyActor;
        use crate::authz::roles::RoleSet;
        use crate::context::OpContext;
        use omicron_common::api::external::Error;
        use oso::PolarClass;
        #[derive(Clone, PolarClass)]
        struct UnregisteredResource;
        impl AuthorizedResource for UnregisteredResource {
            fn load_roles<'a, 'b, 'c, 'd, 'e, 'f>(
                &'a self,
                _: &'b OpContext,
                _: &'c DataStore,
                _: &'d authn::Context,
                _: &'e mut RoleSet,
            ) -> futures::future::BoxFuture<'f, Result<(), Error>>
            where
                'a: 'f,
                'b: 'f,
                'c: 'f,
                'd: 'f,
                'e: 'f,
            {
                // authorize() shouldn't get far enough to call this.
                unimplemented!();
            }

            fn on_unauthorized(
                &self,
                _: &Authz,
                _: Error,
                _: AnyActor,
                _: Action,
            ) -> Error {
                // authorize() shouldn't get far enough to call this.
                unimplemented!();
            }

            fn polar_class(&self) -> oso::Class {
                Self::get_polar_class()
            }
        }

        // Make sure an authz check with this resource fails with a clear
        // message.
        let unregistered_resource = UnregisteredResource {};
        let authz_privileged = authz_context_for_actor(
            &logctx.log,
            authn::Context::privileged_test_user(),
            Arc::clone(&datastore),
        );
        let error = authz_privileged
            .authorize(&opctx, Action::Read, unregistered_resource)
            .await;
        println!("{:?}", error);
        assert!(matches!(error, Err(Error::InternalError {
            internal_message
        }) if internal_message == "attempted authz check \
            on unregistered resource: \"UnregisteredResource\""));

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
