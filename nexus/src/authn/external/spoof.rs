// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Custom, test-only authn scheme that trusts whatever the client says

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use crate::authn;
use crate::authn::Actor;
use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use lazy_static::lazy_static;
use uuid::Uuid;

/// Header used for "spoof" authentication
pub const HTTP_HEADER_OXIDE_AUTHN_SPOOF: &str = "oxide-authn-spoof";
/// Magic header value to produce a "no such actor" error
pub const SPOOF_RESERVED_BAD_ACTOR: &str = "Jack Donaghy";
/// Magic header value to produce a "bad credentials" error
pub const SPOOF_RESERVED_BAD_CREDS: &str =
    "this fake I.D., it is truly excellent";
pub const SPOOF_SCHEME_NAME: authn::SchemeName = authn::SchemeName("spoof");

lazy_static! {
    static ref SPOOF_RESERVED_BAD_CREDS_ACTOR: Actor =
        Actor("22222222-2222-2222-2222-222222222222".parse().unwrap());
}

/// Implements a (test-only) authentication scheme where the client simply
/// provides the actor information in a custom header
/// ([`HTTP_HEADER_OXIDE_AUTHN_SPOOF`]) and we always trust it.  This is useful
/// for testing the rest of the authn facilities since we can very easily and
/// precisely control its output.
#[derive(Debug)]
pub struct HttpAuthnSpoof;

#[async_trait]
impl<T> HttpAuthnScheme<T> for HttpAuthnSpoof
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        SPOOF_SCHEME_NAME
    }

    async fn authn(
        &self,
        _ctx: &T,
        _log: &slog::Logger,
        request: &http::Request<hyper::Body>,
    ) -> SchemeResult {
        let headers = request.headers();
        authn_spoof(headers.get(HTTP_HEADER_OXIDE_AUTHN_SPOOF))
    }
}

fn authn_spoof(raw_value: Option<&http::HeaderValue>) -> SchemeResult {
    if let Some(raw_value) = raw_value {
        match raw_value.to_str().context("parsing header value as UTF-8") {
            Err(source) => SchemeResult::Failed(Reason::BadFormat { source }),
            Ok(str_value) if str_value == SPOOF_RESERVED_BAD_ACTOR => {
                SchemeResult::Failed(Reason::UnknownActor {
                    actor: str_value.to_owned(),
                })
            }
            Ok(str_value) if str_value == SPOOF_RESERVED_BAD_CREDS => {
                SchemeResult::Failed(Reason::BadCredentials {
                    actor: *SPOOF_RESERVED_BAD_CREDS_ACTOR,
                    source: anyhow!("do not sell to the people on this list"),
                })
            }
            Ok(str_value) => match Uuid::parse_str(str_value)
                .context("parsing header value as UUID")
            {
                Ok(id) => {
                    SchemeResult::Authenticated(Details { actor: Actor(id) })
                }
                Err(source) => {
                    SchemeResult::Failed(Reason::BadFormat { source })
                }
            },
        }
    } else {
        SchemeResult::NotRequested
    }
}

#[cfg(test)]
mod test {
    use super::super::super::Details;
    use super::super::super::Reason;
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
        // The client provided no credentials.
        assert!(matches!(authn_spoof(None), SchemeResult::NotRequested));
    }

    #[test]
    fn test_spoof_reserved_values() {
        let bad_actor = super::SPOOF_RESERVED_BAD_ACTOR;
        let header = http::HeaderValue::from_static(bad_actor);
        assert!(matches!(
            authn_spoof(Some(&header)),
            SchemeResult::Failed(Reason::UnknownActor{ actor })
                if actor == bad_actor
        ));

        let bad_creds = super::SPOOF_RESERVED_BAD_CREDS;
        let header = http::HeaderValue::from_static(bad_creds);
        assert!(matches!(
            authn_spoof(Some(&header)),
            SchemeResult::Failed(Reason::BadCredentials{
                actor,
                source
            }) if actor == *super::SPOOF_RESERVED_BAD_CREDS_ACTOR &&
                    source.to_string() ==
                    "do not sell to the people on this list"
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
            let test_header = http::HeaderValue::from_bytes(input)
                .expect("test case header value was not a valid HTTP header");
            let result = authn_spoof(Some(&test_header));
            if let SchemeResult::Failed(error) = result {
                assert!(error.to_string().starts_with(
                    "bad authentication credentials: parsing header value"
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
