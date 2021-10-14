//! Authentication for requests to the external HTTP API
//! XXX mode -> scheme?

use crate::authn;
use crate::ServerContext;
use anyhow::anyhow;
use authn::Reason;
use dropshot::RequestContext;
use lazy_static::lazy_static;
use serde_with::DeserializeFromStr;
use serde_with::SerializeDisplay;
use std::collections::BTreeSet;
use std::sync::Arc;

mod spoof;
pub use spoof::HTTP_HEADER_OXIDE_AUTHN_SPOOF;

lazy_static! {
    #[rustfmt::skip]
    static ref EXTERNAL_AUTHN_MODES: Vec<(
        AuthnModeId,
        Arc<dyn HttpAuthnMode>
    )> = vec![(AuthnModeId::Spoof, Arc::new(spoof::HttpAuthnSpoof)),];
}

/// Authenticates incoming HTTP requests using modes intended for use by the
/// external API
///
/// (This will eventually support something like HTTP signatures and OAuth.  For
/// now, only a dummy mode is supported.)
pub struct Authenticator {
    all_modes: Vec<(AuthnModeId, Arc<dyn HttpAuthnMode>)>,
    allowed_modes: BTreeSet<AuthnModeId>,
}

impl Authenticator {
    /// Build a new authentiator that allows only the specified modes
    pub fn new<'a, I: IntoIterator<Item = &'a AuthnModeId>>(
        allowed_modes: I,
    ) -> Authenticator {
        Authenticator {
            all_modes: EXTERNAL_AUTHN_MODES.clone(),
            allowed_modes: allowed_modes.into_iter().map(|n| *n).collect(),
        }
    }

    /// Authenticate an incoming HTTP request
    //
    // TODO-security If the client is attempting to authenticate and fails for
    // whatever reason, we produce an error here.  It might be reasonable to
    // instead produce [`authn::Context::Unauthenticated`].  Does either of
    // these introduce a security problem?
    //
    // TODO-cleanup: Do we want to consume the whole RequestContext here so that
    // we can get to headers?  Or do we instead want to require that every
    // external API endpoint have an extractor that grabs the appropriate Authn
    // header(s)?  Is it customary to include the auth header(s) in the OpenAPI
    // spec for every endpoint?  Or do they go in some separate section of
    // headers that apply to many endpoints?
    //
    pub async fn authn_request(
        &self,
        rqctx: &RequestContext<Arc<ServerContext>>,
    ) -> Result<authn::Context, authn::Error> {
        let log = &rqctx.log;
        let request = rqctx.request.lock().await;

        let mut modes_tried = Vec::with_capacity(self.allowed_modes.len());
        for (mode_name, mode) in &self.all_modes {
            // XXX Extra quotes here
            let mode_name_str = serde_json::to_string(&mode_name).unwrap();
            if !self.allowed_modes.contains(mode_name) {
                trace!(
                    log,
                    "authn_http: skipping {:?} (not allowed by config)",
                    mode_name_str
                );
                continue;
            }

            trace!(log, "authn_http: trying {:?}", mode_name_str);
            modes_tried.push(mode_name_str);
            let result = mode.authn(rqctx, &request);
            match result {
                // TODO-security If the user explicitly failed one
                // authentication mode (i.e., a signature that didn't match, NOT
                // that they simply didn't try), should we try the others?
                ModeResult::Failed(reason) => {
                    return Err(authn::Error { reason, modes_tried })
                }
                ModeResult::Authenticated(details) => {
                    return Ok(authn::Context {
                        kind: authn::Kind::Authenticated(details),
                        modes_tried,
                    })
                }
                ModeResult::NotRequested => (),
            }
        }

        Ok(authn::Context { kind: authn::Kind::Unauthenticated, modes_tried })
    }
}

/// List of all supported external authn modes
///
/// Besides being useful in defining the configuration file, having a type that
/// describes all the supported modes makes it easier for us to record log
/// messages and other structured introspection describing which modes are
/// enabled, which modes were attempted for a request, and so on.
#[derive(
    Clone,
    Copy,
    Debug,
    DeserializeFromStr,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    SerializeDisplay,
)]
pub enum AuthnModeId {
    /// See [`HttpAuthnSpoof'].
    Spoof,
}

impl std::str::FromStr for AuthnModeId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "spoof" => Ok(AuthnModeId::Spoof),
            _ => Err(anyhow!("unsupported authn mode: {:?}", s)),
        }
    }
}

impl std::fmt::Display for AuthnModeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AuthnModeId::Spoof => "spoof",
        })
    }
}

/// Result returned by a particular authentication mode
#[derive(Debug)]
enum ModeResult {
    /// The client is not trying to use this authn mode
    NotRequested,
    /// The client successfully authenticated
    Authenticated(super::Details),
    /// The client tried and failed to authenticate
    Failed(Reason),
}

/// Implements a particular HTTP authentication mode
trait HttpAuthnMode: std::fmt::Debug + Send + Sync {
    /// Returns the (unique) name for this mode (for observability)
    fn name(&self) -> AuthnModeId;

    /// Locate credentials in the HTTP request and attempt to verify them
    fn authn(
        &self,
        rqctx: &RequestContext<Arc<ServerContext>>,
        request: &http::Request<hyper::Body>,
    ) -> ModeResult;
}

#[cfg(test)]
mod test {
    // XXX
}
