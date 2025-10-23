// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authentication facilities
//!
//! In the limit, we'll want all operations in Nexus to have an associated
//! authentication/authorization context that describes who (or what) is doing
//! the operation and what privileges they have.
//!
//! This module includes generic, HTTP-agnostic facilities for representing who
//! or what is authenticated and why an authentication attempt failed.
//!
//! The [`external`] submodule provides an [`external::Authenticator`] interface
//! that will eventually authenticate requests using standard external
//! authentication mechanisms like HTTP signatures or OAuth.
//!
//! In the future, we can add other submodules for other kinds of
//! authentication.  For example, if we use macaroons for internal authn, we
//! could have a different `InternalHttpnAuthenticator` that validates the
//! macaroons.   Other operations may not be associated with HTTP requests at
//! all (like saga recovery, health checking, or fault response), but we may
//! still want them to carry information about what's authenticated and what
//! privileges it has.  These submodules might provide different mechanisms for
//! authentication, but they'd all produce the same [`Context`] struct.

pub mod external;
pub mod saga;
pub mod silos;

pub use nexus_db_fixed_data::silo_user::USER_TEST_PRIVILEGED;
pub use nexus_db_fixed_data::silo_user::USER_TEST_UNPRIVILEGED;
pub use nexus_db_fixed_data::user_builtin::USER_DB_INIT;
pub use nexus_db_fixed_data::user_builtin::USER_EXTERNAL_AUTHN;
pub use nexus_db_fixed_data::user_builtin::USER_INTERNAL_API;
pub use nexus_db_fixed_data::user_builtin::USER_INTERNAL_READ;
pub use nexus_db_fixed_data::user_builtin::USER_SAGA_RECOVERY;
pub use nexus_db_fixed_data::user_builtin::USER_SERVICE_BALANCER;

use crate::authz;
use newtype_derive::NewtypeDisplay;
use nexus_db_fixed_data::silo::DEFAULT_SILO;
use nexus_types::external_api::shared::FleetRole;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::identity::Asset;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::BuiltInUserUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloUserUuid;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use uuid::Uuid;

/// Describes how the actor performing the current operation is authenticated
///
/// This is HTTP-agnostic.  Subsystems in Nexus could create contexts for
/// purposes unrelated to HTTP (e.g., background jobs).
#[derive(Debug)]
pub struct Context {
    /// Describes whether the user is authenticated and provides more
    /// information that's specific to whether they're authenticated or not
    kind: Kind,

    /// List of authentication schemes tried
    ///
    /// If `kind` is `Kind::Unauthenticated`, then none of these schemes found
    /// any credentials to verify.  Otherwise, whether authentiation succeeded
    /// or failed, it was the last scheme in this list that was responsible for
    /// the final determination.
    schemes_tried: Vec<SchemeName>,
}

impl Context {
    /// Returns the authenticated actor, if any
    pub fn actor(&self) -> Option<&Actor> {
        self.actor_required().ok()
    }

    /// Returns the authenticated actor if present or an Unauthenticated error
    /// otherwise
    pub fn actor_required(
        &self,
    ) -> Result<&Actor, omicron_common::api::external::Error> {
        match &self.kind {
            Kind::Authenticated(Details { actor }, ..) => Ok(actor),
            Kind::Unauthenticated => {
                Err(omicron_common::api::external::Error::Unauthenticated {
                    internal_message: "Actor required".to_string(),
                })
            }
        }
    }

    /// Returns the current actor's Silo if they have one or an appropriate
    /// error otherwise
    ///
    /// This is intended for code paths that always expect a Silo to be present.
    /// Built-in users have no Silo, and this function will return an
    /// InternalError if the currently-authenticated user is built-in.  If you
    /// want to handle that case differently, see
    /// [`Context::silo_or_builtin()`].
    pub fn silo_required(
        &self,
    ) -> Result<authz::Silo, omicron_common::api::external::Error> {
        self.silo_or_builtin().and_then(|maybe_silo| {
            maybe_silo.ok_or_else(|| {
                omicron_common::api::external::Error::internal_error(
                    "needed Silo for a built-in user, but \
                        built-in users have no Silo",
                )
            })
        })
    }

    /// Determine whether the currently authenticated actor has a Silo or is a
    /// built-in user
    ///
    /// This function allows callers to distinguish these three cases:
    ///
    /// * there's an authenticated user with an associated Silo (most common)
    /// * there's an authenticated built-in user who has no associated Silo
    /// * there's no authenticated user (returned as an error)
    ///
    /// Built-in users have no Silo, and so they usually can't do anything that
    /// might use a Silo.  You usually want to use [`Context::silo_required()`]
    /// if you don't expect to be looking at a built-in user.
    ///
    /// Additionally, non-user Actors may also be associated with a Silo.
    pub fn silo_or_builtin(
        &self,
    ) -> Result<Option<authz::Silo>, omicron_common::api::external::Error> {
        self.actor_required().map(|actor| match actor {
            Actor::SiloUser { silo_id, .. } => Some(authz::Silo::new(
                authz::FLEET,
                *silo_id,
                LookupType::ById(*silo_id),
            )),
            Actor::UserBuiltin { .. } => None,
            Actor::Scim { silo_id } => Some(authz::Silo::new(
                authz::FLEET,
                *silo_id,
                LookupType::ById(*silo_id),
            )),
        })
    }

    /// Returns the `SiloAuthnPolicy` for the authenticated actor's Silo, if
    /// any.
    pub fn silo_authn_policy(&self) -> Option<&SiloAuthnPolicy> {
        match &self.kind {
            Kind::Unauthenticated => None,
            Kind::Authenticated(_, policy) => {
                // note: must be Some if `details.actor` is also Some, otherwise
                // you'll see a 500 in the impl of
                // `ApiResourceWithRoles::conferred_roles_by` for Fleet.
                policy.as_ref()
            }
        }
    }

    /// Returns the list of schemes tried, in order
    ///
    /// This should generally *not* be exposed to clients.
    pub fn schemes_tried(&self) -> &[SchemeName] {
        &self.schemes_tried
    }

    /// If the user is authenticated, return the last scheme in the list of
    /// schemes tried, which is the one that worked.
    pub fn scheme_used(&self) -> Option<&SchemeName> {
        match &self.kind {
            Kind::Authenticated(..) => self.schemes_tried().last(),
            Kind::Unauthenticated => None,
        }
    }

    /// Returns an unauthenticated context for use internally
    pub fn internal_unauthenticated() -> Context {
        Context { kind: Kind::Unauthenticated, schemes_tried: vec![] }
    }

    /// Returns an authenticated context for handling internal API contexts
    pub fn internal_api() -> Context {
        Context::context_for_builtin_user(USER_INTERNAL_API.id)
    }

    /// Returns an authenticated context for saga recovery
    pub fn internal_saga_recovery() -> Context {
        Context::context_for_builtin_user(USER_SAGA_RECOVERY.id)
    }

    /// Returns an authenticated context for use by internal resource allocation
    pub fn internal_read() -> Context {
        Context::context_for_builtin_user(USER_INTERNAL_READ.id)
    }

    /// Returns an authenticated context for use for authenticating external
    /// requests
    pub fn external_authn() -> Context {
        Context::context_for_builtin_user(USER_EXTERNAL_AUTHN.id)
    }

    /// Returns an authenticated context for Nexus-startup database
    /// initialization
    pub fn internal_db_init() -> Context {
        Context::context_for_builtin_user(USER_DB_INIT.id)
    }

    /// Returns an authenticated context for Nexus-driven service balancing.
    pub fn internal_service_balancer() -> Context {
        Context::context_for_builtin_user(USER_SERVICE_BALANCER.id)
    }

    fn context_for_builtin_user(user_builtin_id: BuiltInUserUuid) -> Context {
        Context {
            kind: Kind::Authenticated(
                Details { actor: Actor::UserBuiltin { user_builtin_id } },
                None,
            ),
            schemes_tried: Vec::new(),
        }
    }

    /// Returns an authenticated context for a special testing user
    // Ideally this would only be exposed under `#[cfg(test)]`, but it's used by
    // `OpContext::for_tests()`.
    pub fn privileged_test_user() -> Context {
        Context {
            kind: Kind::Authenticated(
                Details {
                    actor: Actor::SiloUser {
                        silo_user_id: USER_TEST_PRIVILEGED.id(),
                        silo_id: USER_TEST_PRIVILEGED.silo_id,
                    },
                },
                Some(SiloAuthnPolicy::try_from(&*DEFAULT_SILO).unwrap()),
            ),
            schemes_tried: Vec::new(),
        }
    }

    /// Returns an authenticated context for the special unprivileged user
    /// (for testing only)
    #[cfg(test)]
    pub fn unprivileged_test_user() -> Context {
        Context::for_test_user(
            USER_TEST_UNPRIVILEGED.id(),
            USER_TEST_UNPRIVILEGED.silo_id,
            SiloAuthnPolicy::try_from(&*DEFAULT_SILO).unwrap(),
        )
    }

    /// Returns an authenticated context for the specific Silo user. Not marked
    /// as #[cfg(test)] so that this is available in integration tests.
    pub fn for_test_user(
        silo_user_id: SiloUserUuid,
        silo_id: Uuid,
        silo_authn_policy: SiloAuthnPolicy,
    ) -> Context {
        Context {
            kind: Kind::Authenticated(
                Details { actor: Actor::SiloUser { silo_user_id, silo_id } },
                Some(silo_authn_policy),
            ),
            schemes_tried: Vec::new(),
        }
    }

    /// Returns an authenticated context for a Silo's SCIM Actor. Not marked as
    /// #[cfg(test)] so that this is available in integration tests.
    pub fn for_scim(silo_id: Uuid) -> Context {
        Context {
            kind: Kind::Authenticated(
                Details { actor: Actor::Scim { silo_id } },
                // This should never be non-empty, we don't want the SCIM user
                // to ever have associated roles.
                Some(SiloAuthnPolicy::new(BTreeMap::default())),
            ),
            schemes_tried: Vec::new(),
        }
    }
}

/// Authentication-related policy derived from a user's Silo
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SiloAuthnPolicy {
    /// Describes which fleet-level roles are automatically conferred by which
    /// silo-level roles.
    mapped_fleet_roles: BTreeMap<SiloRole, BTreeSet<FleetRole>>,

    /// When true, restricts networking actions to Silo Admins only
    restrict_network_actions: bool,
}

impl SiloAuthnPolicy {
    pub fn new(
        mapped_fleet_roles: BTreeMap<SiloRole, BTreeSet<FleetRole>>,
        restrict_network_actions: bool,
    ) -> SiloAuthnPolicy {
        SiloAuthnPolicy { mapped_fleet_roles, restrict_network_actions }
    }

    pub fn mapped_fleet_roles(
        &self,
    ) -> &BTreeMap<SiloRole, BTreeSet<FleetRole>> {
        &self.mapped_fleet_roles
    }

    pub fn restrict_network_actions(&self) -> bool {
        self.restrict_network_actions
    }
}

impl TryFrom<&nexus_db_model::Silo> for SiloAuthnPolicy {
    type Error = omicron_common::api::external::Error;

    fn try_from(
        value: &nexus_db_model::Silo,
    ) -> Result<Self, omicron_common::api::external::Error> {
        value.mapped_fleet_roles().map(|mapped_fleet_roles| SiloAuthnPolicy {
            mapped_fleet_roles,
            restrict_network_actions: value.restrict_network_actions,
        })
    }
}

#[cfg(test)]
mod test {
    use super::Context;
    use super::USER_DB_INIT;
    use super::USER_INTERNAL_API;
    use super::USER_INTERNAL_READ;
    use super::USER_SAGA_RECOVERY;
    use super::USER_SERVICE_BALANCER;
    use super::USER_TEST_PRIVILEGED;
    use super::USER_TEST_UNPRIVILEGED;
    use nexus_db_fixed_data::user_builtin::USER_EXTERNAL_AUTHN;
    use nexus_types::identity::Asset;

    #[test]
    fn test_internal_users() {
        // The context returned by "internal_unauthenticated()" ought to have no
        // associated actor.
        let authn = Context::internal_unauthenticated();
        assert!(authn.actor().is_none());

        // Validate the actor behind various test contexts.
        // The privileges are (or will be) verified in authz tests.
        let authn = Context::privileged_test_user();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.silo_user_id(), Some(USER_TEST_PRIVILEGED.id()));

        let authn = Context::unprivileged_test_user();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.silo_user_id(), Some(USER_TEST_UNPRIVILEGED.id()));

        let authn = Context::internal_read();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.built_in_user_id(), Some(USER_INTERNAL_READ.id));

        let authn = Context::external_authn();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.built_in_user_id(), Some(USER_EXTERNAL_AUTHN.id));

        let authn = Context::internal_db_init();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.built_in_user_id(), Some(USER_DB_INIT.id));

        let authn = Context::internal_service_balancer();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.built_in_user_id(), Some(USER_SERVICE_BALANCER.id));

        let authn = Context::internal_saga_recovery();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.built_in_user_id(), Some(USER_SAGA_RECOVERY.id));

        let authn = Context::internal_api();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.built_in_user_id(), Some(USER_INTERNAL_API.id));
    }
}

/// Describes whether the user is authenticated and provides more information
/// that's specific to whether they're authenticated (or not)
#[derive(Clone, Debug, Deserialize, Serialize)]
enum Kind {
    /// Client did not attempt to authenticate
    Unauthenticated,
    /// Client successfully authenticated
    Authenticated(Details, Option<SiloAuthnPolicy>),
}

/// Describes the actor that was authenticated
///
/// This could eventually include other information used during authorization,
/// like a remote IP, the time of authentication, etc.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Details {
    /// the actor performing the request
    actor: Actor,
}

/// Who is performing an operation
#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum Actor {
    UserBuiltin { user_builtin_id: BuiltInUserUuid },
    SiloUser { silo_user_id: SiloUserUuid, silo_id: Uuid },
    Scim { silo_id: Uuid },
}

impl Actor {
    pub fn silo_id(&self) -> Option<Uuid> {
        match self {
            Actor::UserBuiltin { .. } => None,
            Actor::SiloUser { silo_id, .. } => Some(*silo_id),
            Actor::Scim { silo_id } => Some(*silo_id),
        }
    }

    pub fn silo_user_id(&self) -> Option<SiloUserUuid> {
        match self {
            Actor::UserBuiltin { .. } => None,
            Actor::SiloUser { silo_user_id, .. } => Some(*silo_user_id),
            Actor::Scim { .. } => None,
        }
    }

    pub fn built_in_user_id(&self) -> Option<BuiltInUserUuid> {
        match self {
            Actor::UserBuiltin { user_builtin_id } => Some(*user_builtin_id),
            Actor::SiloUser { .. } => None,
            Actor::Scim { .. } => None,
        }
    }

    /// Return a generic UUID and db-model IdentityType for use with looking up
    /// role assignments, or None if a role assignment for this type of Actor is
    /// invalid.
    pub fn id_and_type_for_role_assignment(
        &self,
    ) -> Option<(Uuid, nexus_db_model::IdentityType)> {
        match &self {
            Actor::UserBuiltin { user_builtin_id } => Some((
                user_builtin_id.into_untyped_uuid(),
                nexus_db_model::IdentityType::UserBuiltin,
            )),
            Actor::SiloUser { silo_user_id, .. } => Some((
                silo_user_id.into_untyped_uuid(),
                nexus_db_model::IdentityType::SiloUser,
            )),
            // a role assignment for this Actor is invalid, they have a fixed
            // policy.
            Actor::Scim { .. } => None,
        }
    }
}

impl std::fmt::Debug for Actor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // This `Debug` impl is approximately the same as what we'd get by
        // deriving it.  We impl it by hand so that adding fields to `Actor`
        // doesn't result in them showing up in `Debug` output (e.g., log
        // messages) unless someone explicitly adds them here.
        //
        // Do NOT include sensitive fields (e.g., private key or a bearer
        // token) in this output!
        match self {
            Actor::UserBuiltin { user_builtin_id } => f
                .debug_struct("Actor::UserBuiltin")
                .field("user_builtin_id", &user_builtin_id)
                .finish_non_exhaustive(),
            Actor::SiloUser { silo_user_id, silo_id } => f
                .debug_struct("Actor::SiloUser")
                .field("silo_user_id", &silo_user_id)
                .field("silo_id", &silo_id)
                .finish_non_exhaustive(),
            Actor::Scim { silo_id } => f
                .debug_struct("Actor::Scim")
                .field("silo_id", &silo_id)
                .finish_non_exhaustive(),
        }
    }
}

/// A console session with the silo id of the authenticated user
#[derive(Clone, Debug)]
pub struct ConsoleSessionWithSiloId {
    pub console_session: nexus_db_model::ConsoleSession,
    pub silo_id: Uuid,
}

/// Label for a particular authentication scheme (used in log messages and
/// internal error messages)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchemeName(&'static str);
NewtypeDisplay! { () pub struct SchemeName(&'static str); }

/// Describes why authentication failed
///
/// This should usually *not* be exposed to end users because it can leak
/// information that makes it easier to exploit the system.  There are two
/// purposes for these codes:
///
/// 1. So that we have specific information in the logs (and maybe in the future
///    in user-visible diagnostic interfaces) for engineers or support to
///    diagnose the authentication failure after it's happened.
///
/// 2. To facilitate conversion to the appropriate [`dropshot::HttpError`] error
///    type.  This will generally have a lot less information to avoid leaking
///    information to attackers, but it's still useful to distinguish between
///    400 and 401/403, for example.
#[derive(Debug, thiserror::Error)]
#[error("authentication failed (tried schemes: {schemes_tried:?})")]
pub struct Error {
    /// list of authentication schemes that were tried
    schemes_tried: Vec<SchemeName>,
    /// why authentication failed
    #[source]
    reason: Reason,
}

#[derive(Debug, thiserror::Error)]
pub enum Reason {
    /// The authn credentials are syntactically invalid
    #[error("bad authentication credentials: {source:#}")]
    BadFormat {
        #[source]
        source: anyhow::Error,
    },

    /// We did not find the actor that was attempting to authenticate
    #[error("unknown actor {actor:?}")]
    UnknownActor { actor: String },

    /// The credentials were syntactically valid, but semantically invalid
    /// (e.g., a cryptographic signature did not match)
    #[error("bad credentials for actor {actor:?}: {source:#}")]
    BadCredentials {
        actor: Actor,
        #[source]
        source: anyhow::Error,
    },

    /// A user was authenticated, but we failed to load their Silo's
    /// authentication policy
    #[error("actor authenticated, but failed to load Silo authn policy")]
    LoadSiloAuthnPolicy {
        #[source]
        source: omicron_common::api::external::Error,
    },

    /// Operational error while attempting to authenticate
    #[error("unexpected error during authentication: {source:#}")]
    UnknownError {
        #[source]
        source: omicron_common::api::external::Error,
    },
}

impl From<Error> for dropshot::HttpError {
    fn from(authn_error: Error) -> Self {
        match authn_error.reason {
            // TODO-security Does this leak too much information, to say that
            // the header itself was malformed?  It doesn't feel like it, and as
            // a user it's _really_ helpful to know if you've just, like,
            // encoded it wrong.
            e @ Reason::BadFormat { .. } => {
                dropshot::HttpError::for_bad_request(None, format!("{:#}", e))
            }
            // The HTTP short summary of this status code is "Unauthorized", but
            // the code describes an authentication failure, not an
            // authorization one.  This applies to cases where the request was
            // missing credentials but needs them (which we can't know here) or
            // cases where the credentials were invalid.  See RFC 7235.
            // TODO-security Under what conditions should this be a 404
            // instead?
            // TODO Add a WWW-Authenticate header.  We probably want to provide
            // this on all requests, since different creds can always change the
            // behavior.
            e @ Reason::UnknownActor { .. }
            | e @ Reason::BadCredentials { .. } => dropshot::HttpError::from(
                omicron_common::api::external::Error::Unauthenticated {
                    internal_message: format!("{:#}", e),
                },
            ),
            Reason::UnknownError { source }
            | Reason::LoadSiloAuthnPolicy { source } => source.into(),
        }
    }
}
