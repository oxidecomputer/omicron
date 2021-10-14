use super::super::Details;
use super::AuthnSchemeId;
use super::HttpAuthnScheme;
use super::SchemeResult;
use super::Reason;
use crate::authn::Actor;
use crate::ServerContext;
use anyhow::Context;
use dropshot::RequestContext;
use std::sync::Arc;
use uuid::Uuid;

/// Header used for "spoof" authentication
pub const HTTP_HEADER_OXIDE_AUTHN_SPOOF: &str = "oxide-authn-spoof";

/// Implements a (test-only) authentication scheme where the client simply
/// provides the actor information in a custom header
/// ([`HTTP_HEADER_OXIDE_AUTHN_SPOOF`]) and we blindly trust it.  This is
/// (obviously) only used for testing.
#[derive(Debug)]
pub struct HttpAuthnSpoof;

impl HttpAuthnScheme for HttpAuthnSpoof {
    fn name(&self) -> AuthnSchemeId {
        AuthnSchemeId::Spoof
    }

    fn authn(
        &self,
        _rqctx: &RequestContext<Arc<ServerContext>>,
        request: &http::Request<hyper::Body>,
    ) -> SchemeResult {
        let headers = request.headers();
        authn_spoof(headers.get(HTTP_HEADER_OXIDE_AUTHN_SPOOF))
    }
}

fn authn_spoof(raw_value: Option<&http::HeaderValue>) -> SchemeResult {
    match raw_value {
        None => SchemeResult::NotRequested,
        Some(raw_value) => {
            let r = raw_value
                .to_str()
                .context("parsing header value as UTF-8")
                .and_then(|s: &str| {
                    Uuid::parse_str(s).context("parsing header value as UUID")
                });
            match r {
                Ok(id) => {
                    SchemeResult::Authenticated(Details { actor: Actor(id) })
                }
                Err(error) => {
                    SchemeResult::Failed(Reason::BadFormat { source: error })
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::super::Details;
    use super::super::SchemeResult;
    use super::authn_spoof;
    use crate::authn;
    use authn::Actor;
    use uuid::Uuid;

    #[test]
    fn test_spoof_header_valid() {
        let test_uuid_str = "37b56e4f-8c60-453b-a37e-99be6efe8a89";
        let test_uuid = test_uuid_str.parse::<Uuid>().unwrap();
        let test_header = http::HeaderValue::from_str(test_uuid_str).unwrap();

        // Success case: the client provided a valid uuid in the header.
        let success_case = authn_spoof(Some(&test_header));
        assert!(matches!(
            success_case,
            SchemeResult::Authenticated(
                Details { actor: Actor(i) }
            ) if i == test_uuid
        ));
    }

    #[test]
    fn test_spoof_header_missing() {
        // The client provided nothing (with header enabled and disabled)
        assert!(matches!(authn_spoof(None), SchemeResult::NotRequested));
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
            let test_header = http::HeaderValue::from_bytes(input)
                .expect("test case header value was not a valid HTTP header");
            let result = authn_spoof(Some(&test_header));
            if let SchemeResult::Failed(error) = result {
                assert!(format!("{:#}", error).starts_with(
                    "bad authentication header: parsing header value"
                ));
            } else {
                panic!(
                    "unexpected result from bad input {:?}: {:?}",
                    input, result
                );
            }
        }
    }
}
