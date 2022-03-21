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

pub use crate::db::fixed_data::user_builtin::USER_DB_INIT;
pub use crate::db::fixed_data::user_builtin::USER_INTERNAL_API;
pub use crate::db::fixed_data::user_builtin::USER_SAGA_RECOVERY;
pub use crate::db::fixed_data::user_builtin::USER_TEST_PRIVILEGED;
pub use crate::db::fixed_data::user_builtin::USER_TEST_UNPRIVILEGED;
use crate::db::model::ConsoleSession;

use serde::Deserialize;
use serde::Serialize;
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

    /// Returns the authenticated actor if present, Unauthenticated error otherwise
    // TODO this should maybe return omicron_common::api::external::Error
    pub fn actor_required(&self) -> Result<&Actor, dropshot::HttpError> {
        match &self.kind {
            Kind::Authenticated(Details { actor }) => Ok(actor),
            Kind::Unauthenticated => Err(dropshot::HttpError::from(
                omicron_common::api::external::Error::Unauthenticated {
                    internal_message: "Actor required".to_string(),
                },
            )),
        }
    }

    pub fn silo_required(
        &self,
    ) -> Result<Uuid, omicron_common::api::external::Error> {
        match self.actor_required() {
            Ok(actor) => Ok(actor.silo_id),
            Err(_) => {
                Err(omicron_common::api::external::Error::Unauthenticated {
                    internal_message: "Actor required".to_string(),
                })
            }
        }
    }

    /// Returns the list of schemes tried, in order
    ///
    /// This should generally *not* be exposed to clients.
    pub fn schemes_tried(&self) -> &[SchemeName] {
        &self.schemes_tried
    }

    /// Returns an unauthenticated context for use internally
    pub fn internal_unauthenticated() -> Context {
        Context { kind: Kind::Unauthenticated, schemes_tried: vec![] }
    }

    /// Returns an authenticated context for handling internal API contexts
    pub fn internal_api() -> Context {
        Context::context_for_actor(
            USER_INTERNAL_API.id,
            USER_INTERNAL_API.silo_id,
        )
    }

    /// Returns an authenticated context for saga recovery
    pub fn internal_saga_recovery() -> Context {
        Context::context_for_actor(
            USER_SAGA_RECOVERY.id,
            USER_SAGA_RECOVERY.silo_id,
        )
    }

    /// Returns an authenticated context for Nexus-startup database
    /// initialization
    pub fn internal_db_init() -> Context {
        Context::context_for_actor(USER_DB_INIT.id, USER_DB_INIT.silo_id)
    }

    fn context_for_actor(actor_id: Uuid, silo_id: Uuid) -> Context {
        Context {
            kind: Kind::Authenticated(Details {
                actor: Actor { id: actor_id, silo_id: silo_id },
            }),
            schemes_tried: Vec::new(),
        }
    }

    /// Returns an authenticated context for a special testing user
    pub fn internal_test_user() -> Context {
        Context::test_context_for_actor(USER_TEST_PRIVILEGED.id)
    }

    /// Returns an authenticated context for a specific user
    ///
    /// This is used for testing.
    pub fn test_context_for_actor(actor_id: Uuid) -> Context {
        Context::context_for_actor(
            actor_id,
            *crate::db::fixed_data::silo_builtin::SILO_ID,
        )
    }
}

#[cfg(test)]
mod test {
    use super::Context;
    use super::USER_DB_INIT;
    use super::USER_INTERNAL_API;
    use super::USER_SAGA_RECOVERY;
    use super::USER_TEST_PRIVILEGED;

    #[test]
    fn test_internal_users() {
        // The context returned by "internal_unauthenticated()" ought to have no
        // associated actor.
        let authn = Context::internal_unauthenticated();
        assert!(authn.actor().is_none());

        // Validate the actor behind various test contexts.
        // The privileges are (or will be) verified in authz tests.
        let authn = Context::internal_test_user();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.id, USER_TEST_PRIVILEGED.id);

        let authn = Context::internal_db_init();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.id, USER_DB_INIT.id);

        let authn = Context::internal_saga_recovery();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.id, USER_SAGA_RECOVERY.id);

        let authn = Context::internal_api();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.id, USER_INTERNAL_API.id);
    }
}

/// Describes whether the user is authenticated and provides more information
/// that's specific to whether they're authenticated (or not)
#[derive(Clone, Debug, Deserialize, Serialize)]
enum Kind {
    /// Client successfully authenticated
    Authenticated(Details),
    /// Client did not attempt to authenticate
    Unauthenticated,
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Actor {
    pub id: Uuid, // silo user id
    pub silo_id: Uuid,
}

/// A console session with the silo id of the authenticated user
#[derive(Clone, Debug)]
pub struct ConsoleSessionWithSiloId {
    pub console_session: ConsoleSession,
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
///
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
}

impl From<Error> for dropshot::HttpError {
    fn from(authn_error: Error) -> Self {
        match &authn_error.reason {
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
            Reason::UnknownActor { .. } | Reason::BadCredentials { .. } => {
                dropshot::HttpError::from(
                    omicron_common::api::external::Error::Unauthenticated {
                        internal_message: format!("{:#}", authn_error),
                    },
                )
            }
        }
    }
}
