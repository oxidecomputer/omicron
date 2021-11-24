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

use uuid::Uuid;

//
// Special built-in users
//
// Here's a proposed convention for choosing uuids that we hardcode into
// Omicron.
//
//   001de000-05e4-0000-0000-000000000000
//   ^^^^^^^^ ^^^^
//       +-----|----------------------------- prefix used for all reserved uuids
//             |                              (looks a bit like "oxide")
//             +----------------------------- says what kind of resource it is
//                                            ("05e4" looks a bit like "user")
//
// This way, the uuids stand out a bit.  It's not clear if this convention will
// be very useful, but it beats a random uuid.
//

/// User id reserved for a test user that's granted many privileges for the
/// purpose of running automated tests.
// "4007" looks a bit like "root".
pub const TEST_USER_UUID_PRIVILEGED: &str =
    "001de000-05e4-0000-0000-000000004007";

/// User id reserved for a test user that has no privileges.
// 60001 is the decimal uid for "nobody" on Helios.
pub const TEST_USER_UUID_UNPRIVILEGED: &str =
    "001de000-05e4-0000-0000-000000060001";

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
        match &self.kind {
            Kind::Unauthenticated => None,
            Kind::Authenticated(Details { actor }) => Some(actor),
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

    /// Returns an authenticated context for a special testing user
    // TODO-security This eventually needs to go.  But for now, this is used
    // in unit tests.
    #[cfg(test)]
    pub fn internal_test_user() -> Context {
        Context::test_context_for_actor(
            TEST_USER_UUID_PRIVILEGED.parse().unwrap(),
        )
    }

    /// Returns an authenticated context for a specific user
    ///
    /// This is used for unit testing the authorization rules.
    #[cfg(test)]
    pub fn test_context_for_actor(actor_id: Uuid) -> Context {
        Context {
            kind: Kind::Authenticated(Details { actor: Actor(actor_id) }),
            schemes_tried: Vec::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Context;
    use super::TEST_USER_UUID_PRIVILEGED;

    #[test]
    fn test_internal_users() {
        // The context returned by "internal_unauthenticated()" ought to have no
        // associated actor.
        let authn = Context::internal_unauthenticated();
        assert!(authn.actor().is_none());
        // The "internal_test_user()" context ought to refer to the predefined
        // test user.  This is used in a few places.
        let authn = Context::internal_test_user();
        let actor = authn.actor().unwrap();
        assert_eq!(actor.0.to_string(), TEST_USER_UUID_PRIVILEGED);
    }
}

/// Describes whether the user is authenticated and provides more information
/// that's specific to whether they're authenticated (or not)
#[derive(Debug)]
pub enum Kind {
    /// Client successfully authenticated
    Authenticated(Details),
    /// Client did not attempt to authenticate
    Unauthenticated,
}

/// Describes the actor that was authenticated
///
/// This could eventually include other information used during authorization,
/// like a remote IP, the time of authentication, etc.
#[derive(Debug)]
pub struct Details {
    /// the actor performing the request
    actor: Actor,
}

/// Who is performing an operation
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Actor(pub Uuid);

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
