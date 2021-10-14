//! HTTP-specific external authentication

use crate::authn;
use crate::ServerContext;
use dropshot::RequestContext;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use thiserror::Error;

mod spoof;
pub use spoof::HTTP_HEADER_OXIDE_AUTHN_SPOOF;

lazy_static! {
    static ref HTTP_AUTHN_MODES: Vec<(HttpAuthnModeName, Arc<dyn HttpAuthnMode>)> =
        vec![(HttpAuthnModeName::Spoof, Arc::new(spoof::HttpAuthnSpoof)),];
}

// XXX
pub struct HttpAuthn {
    all_modes: Vec<(HttpAuthnModeName, Arc<dyn HttpAuthnMode>)>,
    allowed_modes: BTreeSet<HttpAuthnModeName>,
}

impl HttpAuthn {
    pub fn new<'a, I: IntoIterator<Item = &'a HttpAuthnModeName>>(
        modes: I,
    ) -> HttpAuthn {
        HttpAuthn {
            all_modes: HTTP_AUTHN_MODES.clone(),
            allowed_modes: modes.into_iter().map(|n| *n).collect(),
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
    ) -> Result<authn::Context, Error> {
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
                // TODO-security If the user explicitly failed one authentication
                // mode (i.e., a signature that didn't match, NOT that they simply
                // didn't try), should we try the others?
                ModeResult::Failed(kind) => {
                    return Err(Error { kind, modes_tried })
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

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum HttpAuthnModeName {
    Spoof,
}

/// Describes why HTTP authentication failed
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
#[derive(Debug, Error)]
#[error("HTTP authentication failed (tried modes: {modes_tried:?})")]
pub struct Error {
    /// list of authentication modes that were tried
    modes_tried: Vec<String>,
    /// why authentication failed
    #[source]
    kind: ErrorKind,
}

/* TODO These specific variants are pretty half-baked. */
#[derive(Debug, Error)]
pub enum ErrorKind {
    /// The authn header is syntactically invalid
    #[error("bad authentication header: {source:#}")]
    BadFormat {
        #[source]
        source: anyhow::Error,
    },

    /// We did not find the actor that was attempting to authenticate
    #[allow(dead_code)]
    #[error("unknown actor {actor:?}")]
    UnknownActor { actor: String },

    /// The credentials were invalid (e.g., the signature did not match)
    #[allow(dead_code)]
    #[error("bad credentials: {source:#}")]
    BadCredentials {
        #[source]
        source: anyhow::Error,
    },
}

// XXX need a test that we don't leak information
impl From<Error> for dropshot::HttpError {
    fn from(authn_error: Error) -> Self {
        match &authn_error.kind {
            // TODO-security Does this leak too much information, to say that
            // the header itself was malformed?  It doesn't feel like it, and as
            // a user it's _really_ helpful to know if you've just, like,
            // encoded it wrong.
            e @ ErrorKind::BadFormat { .. } => {
                dropshot::HttpError::for_bad_request(None, format!("{:#}", e))
            }
            // The HTTP short summary of this status code is "Unauthorized", but
            // the code describes an authentication failure, not an
            // authorization one.
            // TODO But is that the right status code to use here?
            // TODO-security Under what conditions should this be a 404
            // instead?
            // TODO Add a WWW-Authenticate header?
            ErrorKind::UnknownActor { .. }
            | ErrorKind::BadCredentials { .. } => dropshot::HttpError {
                status_code: http::StatusCode::UNAUTHORIZED,
                error_code: None,
                external_message: String::from("authentication failed"),
                internal_message: format!("{:#}", authn_error),
            },
        }
    }
}

/// Result returned by a particular authentication mode
// XXX document
#[derive(Debug)]
enum ModeResult {
    NotRequested,
    Authenticated(authn::AuthnDetails),
    Failed(ErrorKind),
}

/// Implements an HTTP authentication mode
///
/// The idea here is that we may support multiple different modes of
/// authentication.  These different modes may or may not use the same HTTP
/// headers.
trait HttpAuthnMode: std::fmt::Debug + Send + Sync {
    fn name(&self) -> HttpAuthnModeName;

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
