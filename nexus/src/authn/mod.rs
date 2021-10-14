//!
//! Facilities related to external authentication
//! XXX Explain the generic vs. HTTP-specific stuff.
//!

pub mod http;
use uuid::Uuid;

/// Describes how the actor performing the current operation is authenticated
///
/// This is HTTP-agnostic.  Subsystems in Nexus could create contexts for
/// purposes unrelated to HTTP (e.g., background jobs).
#[derive(Debug)]
pub struct Context {
    /// Describes whether the user is authenticated and provides more
    /// information that's specific to whether they're authenticated or not
    pub kind: Kind,

    /// List of authentication modes tried
    ///
    /// If `kind` is `Unauthenticated(UnauthDetails::NotAttempted)`, then none
    /// of these modes found any credentials to verify.  Otherwise, whether
    /// authentiation succeeded or failed, it was the last mode in this list
    /// that was responsible for the final determination.
    pub modes_tried: Vec<String>,
}

/// Describes whether the user is authenticated and provides more information
/// that's specific to whether they're authenticated or not
#[derive(Debug)]
pub enum Kind {
    /// Client successfully authenticated
    Authenticated(AuthnDetails),
    /// Client did not attempt to authenticate
    Unauthenticated,
}

/// Describes how the client authenticated
// TODO Might this want to have a list of active roles?
#[derive(Debug)]
pub struct AuthnDetails {
    /// the actor performing the request
    pub actor: Actor,
}

/// Describes who is performing an operation
// TODO: This will probably wind up being an enum of: user | service
#[derive(Debug)]
pub struct Actor(pub Uuid);

pub use self::http::HTTP_HEADER_OXIDE_AUTHN_SPOOF;
pub use self::http::HttpAuthn;
