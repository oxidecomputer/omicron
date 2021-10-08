//! HTTP-specific external authentication

use crate::authn;
use crate::ServerContext;
use dropshot::RequestContext;
use std::convert::TryFrom;
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
///    400 and 401/403, for example.
///
/* TODO These specific variants are pretty half-baked. */
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

    // XXX Should happen at server setup time, not now.  How would this work?
    // Is there like an HTTP-specific AuthnFactory?  Ugh...
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
        HttpAuthnSpoof { allowed }
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
        // Record a more specific message in the log for failures that would
        // otherwise be difficult to diagnose from the generic message.
        match spoof {
            SpoofHeader::
        }
        authn::Context::try_from(&spoof)
        //        match spoof {
        //            SpoofHeader::NotPresent => {
        //                trace!(log, "HttpAuthnSpoof: unauthenticated");
        //                Ok(authn::Context::Unauthenticated)
        //            }
        //            SpoofHeader::PresentNotAllowed => {
        //                trace!(
        //                    log,
        //                    "HttpAuthnSpoof: ignoring attempted spoof \
        //                (not allowed by configuration)"
        //                );
        //                trace!(log, "authn_http: unauthenticated");
        //                Ok(authn::Context::Unauthenticated)
        //            }
        //            SpoofHeader::PresentInvalid(error_message) => {
        //                trace!(log, "HttpAuthnSpoof: failed authentication");
        //                Err(Error::BadFormat(format!(
        //                    "header {:?}: {}",
        //                    header_name, error_message
        //                )))
        //            }
        //            SpoofHeader::PresentValid(found_uuid) => {
        //                let details =
        //                    authn::Details { actor: authn::Actor(found_uuid) };
        //                trace!(
        //                    log,
        //                    "HttpAuthnSpoof: spoofed authentication";
        //                    "details" => ?details
        //                );
        //                Ok(authn::Context::Authenticated(details))
        //            }
        //        }
    }
}

#[derive(Debug)]
enum SpoofHeader {
    NotPresent,
    PresentNotAllowed,
    PresentInvalid(String),
    PresentValid(Uuid),
}

impl TryFrom<&SpoofHeader> for authn::Context {
    type Error = Error;

    fn try_from(spoof: &SpoofHeader) -> Result<Self, Self::Error> {
        match spoof {
            SpoofHeader::NotPresent => Ok(authn::Context::Unauthenticated),
            SpoofHeader::PresentNotAllowed => {
                Ok(authn::Context::Unauthenticated)
            }
            SpoofHeader::PresentInvalid(error_message) => {
                Err(Error::BadFormat(format!(
                    "header {:?}: {}",
                    HTTP_HEADER_OXIDE_AUTHN_SPOOF, error_message
                )))
            }
            SpoofHeader::PresentValid(found_uuid) => {
                let details =
                    authn::Details { actor: authn::Actor(*found_uuid) };
                Ok(authn::Context::Authenticated(details))
            }
        }
    }
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

#[cfg(test)]
mod test {
    use super::extract_spoof_header;
    use super::SpoofHeader;
    use crate::authn;
    use std::convert::TryFrom;
    use uuid::Uuid;

    #[test]
    fn test_spoof_header_valid() {
        let test_uuid_str = "37b56e4f-8c60-453b-a37e-99be6efe8a89";
        let test_uuid = test_uuid_str.parse::<Uuid>().unwrap();
        let test_header = http::HeaderValue::from_str(test_uuid_str).unwrap();

        // Success case: the header is enabled by config and the client provided
        // a valid uuid in the header.
        let success_case = extract_spoof_header(Some(&test_header), true);
        assert!(
            matches!(success_case, SpoofHeader::PresentValid(u) if u == test_uuid)
        );

        // The client provided a valid uuid in the header, but the config does
        // not allow the header.
        let success_case = extract_spoof_header(Some(&test_header), false);
        assert!(matches!(success_case, SpoofHeader::PresentNotAllowed));

        // The client provided nothing (with header enabled and disabled)
        assert!(matches!(
            extract_spoof_header(None, true),
            SpoofHeader::NotPresent
        ));
        assert!(matches!(
            extract_spoof_header(None, false),
            SpoofHeader::NotPresent
        ));
    }

    #[test]
    fn test_spoof_header_bad_uuids() {
        // These inputs are all legal HTTP headers but not valid values for our
        // "oxide-authn-spoof" header.
        let bad_inputs: Vec<&[u8]> = vec![
            b"garbage in garbage can -- makes sense", // not a uuid
            b"foo\x80ar",                             // not UTF-8
            b"",                                      // empty value
        ];

        for input in &bad_inputs {
            // When the header is allowed, these values should all be rejected.
            let test_header = http::HeaderValue::from_bytes(input)
                .expect("test case header value was not a valid HTTP header");
            let result = extract_spoof_header(Some(&test_header), true);
            assert!(matches!(result, SpoofHeader::PresentInvalid(_)));

            // When the header is disallowed, the contents are not looked at.
            let result = extract_spoof_header(Some(&test_header), false);
            assert!(matches!(result, SpoofHeader::PresentNotAllowed));
        }
    }

    #[test]
    fn test_spoof_header_to_context() {
        use super::Error;
        use authn::Actor;
        use authn::Details;

        assert!(matches!(
            authn::Context::try_from(&SpoofHeader::NotPresent),
            Ok(authn::Context::Unauthenticated),
        ));
        assert!(matches!(
            authn::Context::try_from(&SpoofHeader::PresentNotAllowed),
            Ok(authn::Context::Unauthenticated),
        ));
        assert!(matches!(
            authn::Context::try_from(&SpoofHeader::PresentInvalid(
                String::from("dummy message")
            )),
            Err(Error::BadFormat(_)),
        ));
        let test_uuid = Uuid::new_v4();
        assert!(matches!(
            authn::Context::try_from(&SpoofHeader::PresentValid(test_uuid)),
            Ok(authn::Context::Authenticated(Details { actor: Actor(u)}))
                if u == test_uuid
        ));
    }
}
