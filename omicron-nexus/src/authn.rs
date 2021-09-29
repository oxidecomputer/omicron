//!
//! Facilities related to external authentication
//!

use crate::ServerContext;
use dropshot::RequestContext;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

const HTTP_HEADER_OXIDE_AUTHN_SPOOF: &str = "oxide-authn-spoof";

/// Describes how the actor performing the current operation is authenticated
pub enum Context {
    /// This actor is not authenticated at all.
    Unauthenticated,
    /// The actor is authenticated.  Details in [`Details`].
    Authenticated(Details),
}

/// Describes who is performing an operation
// TODO: This will probably wind up being an enum of: user | service
#[derive(Debug)]
pub struct Actor(Uuid);

/// Describes how the actor authenticated
// TODO Might this want to have a list of active roles?
#[derive(Debug)]
pub struct Details {
    /// the actor performing the request
    actor: Actor,
}

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
) -> Result<Context, Error> {
    let apictx = rqctx.context();
    let request = rqctx.request.lock().await;
    let nexus = &apictx.nexus;
    let log = &rqctx.log;
    let headers = request.headers();
    let allowed = nexus.config_insecure.allow_any_request_to_spoof_authn_header;

    fn extract_spoof_header(
        log: &slog::Logger,
        headers: &http::HeaderMap<http::HeaderValue>,
        allowed: bool,
    ) -> Option<Result<Uuid, Error>> {
        match headers.get(HTTP_HEADER_OXIDE_AUTHN_SPOOF) {
            None => None,
            Some(_) if !allowed => {
                trace!(
                    log,
                    "authn_http: ignoring attempted spoof (not allowed by \
                    configuration)"
                );
                None
            }
            Some(raw_value) => Some(
                raw_value
                    .to_str()
                    .map_err(|error_to_str| {
                        Error::BadFormat(format!(
                            "header {:?}: {:#}",
                            HTTP_HEADER_OXIDE_AUTHN_SPOOF, error_to_str
                        ))
                    })
                    .and_then(|s: &str| {
                        Uuid::parse_str(s).map_err(|error_parse| {
                            Error::BadFormat(format!(
                                "header {:?}: {:#}",
                                HTTP_HEADER_OXIDE_AUTHN_SPOOF, error_parse
                            ))
                        })
                    }),
            ),
        }
    }

    match extract_spoof_header(log, headers, allowed) {
        None => {
            trace!(log, "authn_http: unauthenticated");
            Ok(Context::Unauthenticated)
        }
        Some(Err(error)) => {
            trace!(log, "authn_http: failed authentication");
            Err(error)
        }
        Some(Ok(found_uuid)) => {
            let details = Details { actor: Actor(found_uuid) };
            trace!(
                log,
                "authn_http: spoofed authentication";
                "details" => ?details
            );
            Ok(Context::Authenticated(details))
        }
    }
}
