//! Authentication for requests to the external HTTP API

use crate::authn;
use authn::Reason;
use dropshot::RequestContext;

pub mod spoof;

/// Authenticates incoming HTTP requests using schemes intended for use by the
/// external API
///
/// (This will eventually support something like HTTP signatures and OAuth.  For
/// now, only a dummy scheme is supported.)
pub struct Authenticator<T> {
    allowed_schemes: Vec<Box<dyn HttpAuthnScheme<T>>>,
}

impl<T> Authenticator<T>
where
    T: Send + Sync + 'static,
{
    /// Build a new authentiator that allows only the specified schemes
    pub fn new(
        allowed_schemes: Vec<Box<dyn HttpAuthnScheme<T>>>,
    ) -> Authenticator<T> {
        Authenticator { allowed_schemes }
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

        // For debuggability, keep track of the schemes that we've tried.
        let mut schemes_tried = Vec::with_capacity(self.allowed_schemes.len());
        for scheme_impl in &self.allowed_schemes {
            let scheme_name = scheme_impl.name();
            trace!(log, "authn: trying {:?}", scheme_name);
            schemes_tried.push(scheme_name);
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

/// Implements a particular HTTP authentication scheme
pub trait HttpAuthnScheme<T>: std::fmt::Debug + Send + Sync + 'static
where
    T: Send + Sync + 'static,
{
    /// Returns the (unique) name for this scheme (for observability)
    fn name(&self) -> authn::SchemeName;

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

// TODO-coverage The end-to-end tests are covered by an integration test.  That
// makes sure that we get the right information at the level of endpoint
// handlers, based on whatever headers were provided in the request.  Here, we
// might try to test the behavior of authn_request() itself with a variety of
// configurations.
