pub mod authn;
pub mod authz;
pub mod context;
pub mod storage;

#[macro_use]
extern crate newtype_derive;

#[allow(unused_imports)]
#[macro_use]
extern crate slog;

#[usdt::provider(provider = "nexus")]
mod probes {
    /// Fires just before attempting to authenticate a request using the given
    /// scheme.
    fn authn__start(
        id: &usdt::UniqueId,
        scheme: &str,
        method: &str,
        uri: &str,
    ) {
    }

    /// Fires just after completing the authentication, with the result.
    fn authn__done(id: &usdt::UniqueId, result: &str) {}

    /// Fires just before attempting to authorize an action on a resource.
    fn authz__start(
        id: &usdt::UniqueId,
        actor: &str,
        action: &str,
        resource: &str,
    ) {
    }

    /// Fires just after completing an authorization check on a resource.
    fn authz__done(id: &usdt::UniqueId, result: &str) {}
}
