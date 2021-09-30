//! HTTP-specific external authentication

use crate::authn;
use crate::ServerContext;
use dropshot::RequestContext;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

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
///    400 and 401, for example.
///
/* TODO These specific values are pretty half-baked. */
#[derive(Debug, Error)]
pub enum Error {
    /// The authn header is structurally invalid
    #[error("bad authentication header: {0}")]
    BadFormat(String),
    /// We did not find the actor that was attempting to authenticate
    #[error("unknown actor: {0:?}")]
    UnknownActor(String),
    /// The credentials were invalid (e.g., the signature did not match)
    #[error("bad credentials")]
    BadCredentials,
}

impl From<Error> for dropshot::HttpError {
    fn from(authn_error: Error) -> Self {
        let message = format!("{:#}", authn_error);
        match &authn_error {
            // TODO-security Does this leak too much information, to say that
            // the header itself was malformed?  It doesn't feel like it, and as
            // a user it's _really_ helpful to know if you've just, like,
            // encoded it wrong.
            Error::BadFormat(_) => {
                dropshot::HttpError::for_bad_request(None, message)
            }
            // The HTTP short summary of this status code is "Unauthorized", but
            // the code describes an authentication failure, not an
            // authorization one.
            // TODO But is that the right status code to use here?
            // TODO-security Under what conditions should this be a 404
            // instead?
            // TODO Add a WWW-Authenticate header?
            Error::UnknownActor(_) | Error::BadCredentials => {
                dropshot::HttpError {
                    status_code: http::StatusCode::UNAUTHORIZED,
                    error_code: None,
                    external_message: String::from("authentication failed"),
                    internal_message: message,
                }
            }
        }
    }
}

/// Authenticate an incoming HTTP request
//
// TODO-security If the client is attempting to authenticate and fails for
// whatever reason, we produce an error here.  It might be reasonable to
// instead produce [`authn::Context::Unauthenticated`].  Does either of
// these introduce a security problem?
//
// TODO-cleanup: Do we want to consume the whole RequestContext here so that we
// can get to headers?  Or do we instead want to require that every external API
// endpoint have an extractor that grabs the appropriate Authn header(s)?  Is it
// customary to include the auth header(s) in the OpenAPI spec for every
// endpoint?  Or do they go in some separate section of headers that apply to
// many endpoints?
//
pub async fn authn_http(
    rqctx: &RequestContext<Arc<ServerContext>>,
) -> Result<authn::Context, Error> {
    let apictx = rqctx.context();
    let request = rqctx.request.lock().await;
    let log = &rqctx.log;

    // XXX Should happen at server setup time, not now.
    let nexus = &apictx.nexus;
    let allowed =
        nexus.config_insecure().allow_any_request_to_spoof_authn_header;
    let auth_modes = vec![Box::new(HttpAuthnSpoof::new(allowed))];

    for mode in auth_modes {
        trace!(log, "authn_http: trying {:?}", mode);
        let result = mode.authn(&rqctx, &request)?;
        if let authn::Context::Authenticated(_) = &result {
            return Ok(result);
        }
    }

    return Ok(authn::Context::Unauthenticated);
}

/// Implements an authentication mode
///
/// The idea here is that we may support multiple different modes of
/// authentication.
trait HttpAuthnMode: std::fmt::Debug {
    /// Attempt to authenticate the current request
    ///
    /// If the request appears to be trying to use this mode, but fails
    /// authentication, that should be an error.  If the request is not using
    /// this mode, this should return an Unauthenticated context instead.
    // TODO-cleanup Would it be less confusing if this returned an enum of
    // Authenticated/ModeNotApplicable/Error?
    fn authn(
        &self,
        rqctx: &RequestContext<Arc<ServerContext>>,
        request: &http::Request<hyper::Body>,
    ) -> Result<authn::Context, Error>;
}

/// Implements a (test-only) authentication mode where the client simply
/// provides the required information in a custom header.
#[derive(Debug)]
struct HttpAuthnSpoof {
    /// whether the server is configured to allow this authn mode
    allowed: bool,
}

impl HttpAuthnSpoof {
    fn new(allowed: bool) -> Self {
        HttpAuthnSpoof {
            allowed,
        }
    }
}

const HTTP_HEADER_OXIDE_AUTHN_SPOOF: &str = "oxide-authn-spoof";

impl HttpAuthnMode for HttpAuthnSpoof {
    fn authn(
        &self,
        rqctx: &RequestContext<Arc<ServerContext>>,
        request: &http::Request<hyper::Body>,
    ) -> Result<authn::Context, Error> {
        let log = &rqctx.log;
        let headers = request.headers();
        let header_name = HTTP_HEADER_OXIDE_AUTHN_SPOOF;
        let spoof =
            extract_spoof_header(headers.get(header_name), self.allowed);
        match spoof {
            SpoofHeader::NotPresent => {
                trace!(log, "HttpAuthnSpoof: unauthenticated");
                Ok(authn::Context::Unauthenticated)
            }
            SpoofHeader::PresentNotAllowed => {
                trace!(
                    log,
                    "HttpAuthnSpoof: ignoring attempted spoof \
                (not allowed by configuration)"
                );
                trace!(log, "authn_http: unauthenticated");
                Ok(authn::Context::Unauthenticated)
            }
            SpoofHeader::PresentInvalid(error_message) => {
                trace!(log, "HttpAuthnSpoof: failed authentication");
                Err(Error::BadFormat(format!(
                    "header {:?}: {}",
                    header_name, error_message
                )))
            }
            SpoofHeader::PresentValid(found_uuid) => {
                let details = authn::Details { actor: authn::Actor(found_uuid) };
                trace!(
                    log,
                    "HttpAuthnSpoof: spoofed authentication";
                    "details" => ?details
                );
                Ok(authn::Context::Authenticated(details))
            }
        }
    }
}

enum SpoofHeader {
    NotPresent,
    PresentNotAllowed,
    PresentInvalid(String),
    PresentValid(Uuid),
}

fn extract_spoof_header(
    raw_value: Option<&http::HeaderValue>,
    allowed: bool,
) -> SpoofHeader {
    match (raw_value, allowed) {
        (None, _) => SpoofHeader::NotPresent,
        (Some(_), false) => SpoofHeader::PresentNotAllowed,
        (Some(raw_value), true) => {
            let r = raw_value
                .to_str()
                .map_err(|error_to_str| format!("{:#}", error_to_str))
                .and_then(|s: &str| {
                    Uuid::parse_str(s)
                        .map_err(|error_parse| format!("{:#}", error_parse))
                });
            match r {
                Ok(id) => SpoofHeader::PresentValid(id),
                Err(message) => SpoofHeader::PresentInvalid(message),
            }
        }
    }
}
