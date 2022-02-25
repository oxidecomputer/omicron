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
use headers::authorization::{Authorization, Bearer};
use headers::Header;
use headers::HeaderMapExt;
use lazy_static::lazy_static;
use uuid::Uuid;

/// Header used for "spoof" authentication
pub const HTTP_HEADER_OXIDE_AUTHN_SPOOF: &http::header::HeaderName =
    &http::header::AUTHORIZATION;
/// Magic header value to produce a "no such actor" error
const SPOOF_RESERVED_BAD_ACTOR: &str = "Jack-Donaghy";
/// Magic header value to produce a "bad credentials" error
const SPOOF_RESERVED_BAD_CREDS: &str = "this-fake-ID-it-is-truly-excellent";
pub const SPOOF_SCHEME_NAME: authn::SchemeName = authn::SchemeName("spoof");
const SPOOF_PREFIX: &str = "oxide-spoof-";

lazy_static! {
    static ref SPOOF_RESERVED_BAD_CREDS_ACTOR: Actor =
        Actor("22222222-2222-2222-2222-222222222222".parse().unwrap());
    pub static ref SPOOF_HEADER_BAD_ACTOR: http::header::HeaderValue =
        make_header_value_raw(SPOOF_RESERVED_BAD_ACTOR.as_bytes()).unwrap();
    pub static ref SPOOF_HEADER_BAD_CREDS: http::header::HeaderValue =
        make_header_value_raw(SPOOF_RESERVED_BAD_CREDS.as_bytes()).unwrap();
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
    let decode_result =
        <Authorization<Bearer> as Header>::decode(&mut raw_value.into_iter());
    let bearer = match decode_result {
        // This is `NotRequested` because there might be some other module that
        // can parse this Authorization header.
        Err(_) => return SchemeResult::NotRequested,
        Ok(bearer) => bearer,
    };

    let token = bearer.token();
    if !token.starts_with(SPOOF_PREFIX) {
        return SchemeResult::NotRequested;
    }

    let str_value = &token[SPOOF_PREFIX.len()..];

    if str_value == SPOOF_RESERVED_BAD_ACTOR {
        return SchemeResult::Failed(Reason::UnknownActor {
            actor: str_value.to_owned(),
        });
    }

    if str_value == SPOOF_RESERVED_BAD_CREDS {
        return SchemeResult::Failed(Reason::BadCredentials {
            actor: *SPOOF_RESERVED_BAD_CREDS_ACTOR,
            source: anyhow!("do not sell to the people on this list"),
        });
    }

    match Uuid::parse_str(str_value).context("parsing header value as UUID") {
        Ok(id) => SchemeResult::Authenticated(Details { actor: Actor(id) }),
        Err(source) => SchemeResult::Failed(Reason::BadFormat { source }),
    }
}

/// Returns a value of the `Authorization` header for this actor that will be
/// accepted using this scheme
pub fn make_header_value(id: Uuid) -> http::HeaderValue {
    let mut map = http::HeaderMap::new();
    let header =
        Authorization::bearer(&format!("{}{}", SPOOF_PREFIX, id)).unwrap();
    map.typed_insert(header);
    map.remove(&http::header::AUTHORIZATION).unwrap()
}

/// Returns a value of the `Authorization` header with `data` in the place where
/// the actor id goes
///
/// This is only intended for the test suite for cases where the input might not
/// be a valid Uuid and might not even be valid UTF-8.  Use `make_header_value`
/// if `data` is a valid UUID, as that function is safer and won't fail.
pub fn make_header_value_raw(
    data: &[u8],
) -> Result<http::HeaderValue, anyhow::Error> {
    let mut v: Vec<u8> = "Bearer oxide-spoof-".as_bytes().to_vec();
    v.extend(data);
    http::HeaderValue::from_bytes(&v).context("not a valid HTTP header value")
}

// XXX add test for make_header_value() and make_header_value_raw().

#[cfg(test)]
mod test {
    use super::super::super::Details;
    use super::super::super::Reason;
    use super::super::SchemeResult;
    use super::authn_spoof;
    use super::make_header_value;
    use super::make_header_value_raw;
    use crate::authn;
    use authn::Actor;
    use uuid::Uuid;

    #[test]
    fn test_spoof_header_valid() {
        let test_uuid_str = "37b56e4f-8c60-453b-a37e-99be6efe8a89";
        let test_uuid = test_uuid_str.parse::<Uuid>().unwrap();
        let test_header = make_header_value(test_uuid);

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
        let header = super::SPOOF_HEADER_BAD_ACTOR.clone();
        assert!(matches!(
            authn_spoof(Some(&header)),
            SchemeResult::Failed(Reason::UnknownActor{ actor })
                if actor == bad_actor
        ));

        let header = super::SPOOF_HEADER_BAD_CREDS.clone();
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
            b"garbage-in-garbage-can--makes-sense", // not a uuid
            b"",                                    // empty value
        ];

        for input in &bad_inputs {
            let test_header = make_header_value_raw(input).unwrap();
            let result = authn_spoof(Some(&test_header));
            if let SchemeResult::Failed(error) = result {
                assert!(error.to_string().starts_with(
                    "bad authentication credentials: parsing header value"
                ));
            } else {
                panic!(
                    "unexpected result from bad input {:?}: {:?}",
                    String::from_utf8_lossy(input),
                    result
                );
            }
        }

        // This case is not UTF-8.  It's a valid HTTP header.  The
        // implementation cannot easily distinguish:
        //
        //    Authorization: Bearer oxide-spoof-foo\x80ar
        //
        // which is clearly intended for "spoof" (but invalid input) from
        //
        //    Authorization: Bearer some-other-junk
        //
        // which might reasonably be interpreted by some other scheme.
        // It currently returns `NotRequested`.  The specific code isn't
        // important so much as that we don't crash or succeed.
        let test_header = make_header_value_raw(b"foo\x80ar").unwrap();
        assert!(matches!(
            authn_spoof(Some(&test_header)),
            SchemeResult::NotRequested
        ));
    }
}
