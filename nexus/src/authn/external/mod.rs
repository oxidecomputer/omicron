//! Authentication for requests to the external HTTP API

use crate::authn;
use anyhow::anyhow;
use authn::Reason;
use dropshot::RequestContext;
use serde_with::DeserializeFromStr;
use serde_with::SerializeDisplay;
use std::collections::BTreeSet;
use std::sync::Arc;

pub mod spoof;

/// Authenticates incoming HTTP requests using schemes intended for use by the
/// external API
///
/// (This will eventually support something like HTTP signatures and OAuth.  For
/// now, only a dummy scheme is supported.)
pub struct Authenticator<T> {
    all_schemes: Vec<(AuthnSchemeId, Arc<dyn HttpAuthnScheme<T>>)>,
    allowed_schemes: BTreeSet<AuthnSchemeId>,
}

impl<T> Authenticator<T>
where
    T: Send + Sync + 'static,
{
    /// Build a new authentiator that allows only the specified schemes
    pub fn new<'i1, 'i2, I1, I2>(
        all_schemes: I1,
        allowed_schemes: I2,
    ) -> Authenticator<T>
    where
        I1: IntoIterator<Item = &'i1 Arc<dyn HttpAuthnScheme<T>>>,
        I2: IntoIterator<Item = &'i2 AuthnSchemeId>,
    {
        Authenticator {
            all_schemes: all_schemes
                .into_iter()
                .cloned()
                .map(|s| (s.name(), s))
                .collect(),
            allowed_schemes: allowed_schemes.into_iter().map(|n| *n).collect(),
        }
    }

    /// Authenticate an incoming HTTP request
    // TODO-openapi: At some point, the authentication headers need to get into
    // the OpenAPI spec.  We probably don't want to have every endpoint function
    // accept them via an extractor, though.
    pub async fn authn_request(
        &self,
        rqctx: &RequestContext<T>,
    ) -> Result<authn::Context, authn::Error> {
        let log = &rqctx.log;
        let request = rqctx.request.lock().await;

        // Keep track of the schemes tried for debuggability.
        let mut schemes_tried = Vec::with_capacity(self.allowed_schemes.len());

        for (scheme_id, scheme_impl) in &self.all_schemes {
            // Rather than keeping a list of configured schemes and trying
            // those, we keep a list of all schemes and try only the ones that
            // have been configured.  This is a little more circuitous, but it
            // allows us to report an explicit log message when we're skipping a
            // scheme.  This way, people can (hopefully) more quickly debug a
            // case where some header seemed to be ignored.
            if !self.allowed_schemes.contains(scheme_id) {
                trace!(
                    log,
                    "authn: skipping scheme {:?} (not allowed by config)",
                    scheme_id
                );
                continue;
            }

            trace!(log, "authn: trying {:?}", scheme_id);
            schemes_tried.push(format!("{}", scheme_id));
            let result = scheme_impl.authn(rqctx, &request);
            match result {
                // TODO-security If the user explicitly failed one
                // authentication scheme (i.e., a signature that didn't match,
                // NOT that they simply didn't try), should we try the others
                // instead of returning the failure here?
                SchemeResult::Failed(reason) => {
                    return Err(authn::Error { reason, schemes_tried })
                }
                SchemeResult::Authenticated(details) => {
                    return Ok(authn::Context {
                        kind: authn::Kind::Authenticated(details),
                        schemes_tried,
                    })
                }
                SchemeResult::NotRequested => (),
            }
        }

        Ok(authn::Context { kind: authn::Kind::Unauthenticated, schemes_tried })
    }
}

// XXX Can we get rid of this?  Use a newtype around a String?
/// List of all supported external authn schemes
///
/// Besides being useful in defining the configuration file, having a type that
/// describes all the supported schemes makes it easier for us to record log
/// messages and other structured introspection describing which schemes are
/// enabled, which schemes were attempted for a request, and so on.
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
pub enum AuthnSchemeId {
    /// See [`HttpAuthnSpoof'].
    Spoof,
}

impl std::str::FromStr for AuthnSchemeId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "spoof" => Ok(AuthnSchemeId::Spoof),
            _ => Err(anyhow!("unsupported authn scheme: {:?}", s)),
        }
    }
}

impl std::fmt::Display for AuthnSchemeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AuthnSchemeId::Spoof => "spoof",
        })
    }
}

/// Implements a particular HTTP authentication scheme
pub trait HttpAuthnScheme<T>: std::fmt::Debug + Send + Sync
where
    T: Send + Sync + 'static,
{
    /// Returns the (unique) name for this scheme (for observability)
    fn name(&self) -> AuthnSchemeId;

    /// Locate credentials in the HTTP request and attempt to verify them
    fn authn(
        &self,
        rqctx: &RequestContext<T>,
        request: &http::Request<hyper::Body>,
    ) -> SchemeResult;
}

/// Result returned by each authentication scheme when trying to authenticate a
/// request
#[derive(Debug)]
pub enum SchemeResult {
    /// The client is not trying to use this authn scheme
    NotRequested,
    /// The client successfully authenticated
    Authenticated(super::Details),
    /// The client tried and failed to authenticate
    Failed(Reason),
}

#[cfg(test)]
mod test {
    // XXX What tests do we want here?
    // The end-to-end tests are covered by an integration test.  That makes sure
    // that we get the right information at the level of endpoint handlers,
    // based on whatever headers were provided in the request.  Here, we might
    // try to test the behavior of authn_request() itself, with a variety of
    // configurations.
}
