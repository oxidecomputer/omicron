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
pub enum Context {
    /// This actor is not authenticated at all.
    Unauthenticated,
    /// The actor is authenticated.  Details in [`Details`].
    Authenticated(Details),
}

/// Describes how the actor authenticated
// TODO Might this want to have a list of active roles?
#[derive(Debug)]
pub struct Details {
    /// the actor performing the request
    actor: Actor,
}

/// Describes who is performing an operation
// TODO: This will probably wind up being an enum of: user | service
#[derive(Debug)]
pub struct Actor(Uuid);
