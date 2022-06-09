// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client tokens

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use super::SiloContext;
use crate::authn;
use anyhow::Context;
use async_trait::async_trait;
use headers::authorization::{Authorization, Bearer};
use headers::HeaderMapExt;

// This scheme is intended for clients such as the API, CLI, etc.
//
// For ease of integration into existing clients, we use RFC 6750 bearer tokens.
// This mechanism in turn uses HTTP's "Authorization" header.  In practice, it
// looks like this:
//
//     Authorization: Bearer oxide-token-d21e423290606003a183c2738a7b03212bcc9510"
//     ^^^^^^^^^^^^^  ^^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//     |              |      |           |
//     |              |      |           +--- specifies the token itself
//     |              |      +--------------- specifies this "token" mechanism
//     |              +---------------------- specifies RFC 6750 bearer tokens
//     +------------------------------------- standard HTTP authentication hdr
//
// (That's not a typo -- the "authorization" header is generally used to specify
// _authentication_ information.  Similarly, the "Unauthorized" HTTP response
// code usually describes an _authentication_ error.)

pub const TOKEN_SCHEME_NAME: authn::SchemeName = authn::SchemeName("token");

/// Prefix used on the bearer token to identify this scheme
// RFC 6750 expects bearer tokens to be opaque base64-encoded data.  In our
// case, the data we want to represent (this prefix, plus valid tokens) are
// subsets of the base64 character set, so we do not bother encoding them.
const TOKEN_PREFIX: &str = "oxide-token-";

/// Implements a bearer-token-based authentication scheme.
#[derive(Debug)]
pub struct HttpAuthnToken;

#[async_trait]
impl<T> HttpAuthnScheme<T> for HttpAuthnToken
where
    T: SiloContext + TokenContext + Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        TOKEN_SCHEME_NAME
    }

    async fn authn(
        &self,
        ctx: &T,
        _log: &slog::Logger,
        request: &http::Request<hyper::Body>,
    ) -> SchemeResult {
        let headers = request.headers();
        match parse_token(headers.typed_get().as_ref()) {
            Err(error) => SchemeResult::Failed(error),
            Ok(None) => SchemeResult::NotRequested,
            Ok(Some(token)) => match ctx.token_actor(token).await {
                Err(error) => SchemeResult::Failed(error),
                Ok(actor) => SchemeResult::Authenticated(Details { actor }),
            },
        }
    }
}

fn parse_token(
    raw_value: Option<&Authorization<Bearer>>,
) -> Result<Option<String>, Reason> {
    let token = match raw_value {
        None => return Ok(None),
        Some(bearer) => bearer.token(),
    };

    if !token.starts_with(TOKEN_PREFIX) {
        // This is some other kind of bearer token.  Maybe another scheme knows
        // how to deal with it.
        return Ok(None);
    }

    Ok(Some(token[TOKEN_PREFIX.len()..].to_string()))
}

/// Returns a value of the `Authorization` header for this actor that will be
/// accepted using this scheme
pub fn make_header_value(token: &str) -> Authorization<Bearer> {
    make_header_value_str(&token.to_string()).unwrap()
}

/// Returns a value of the `Authorization` header with `str` in the place where
/// the token goes
///
/// Unlike `make_header_value`, this is not guaranteed to work, as the
/// string may contain non-base64 characters.  Unlike `make_header_value_raw`,
/// this returns a typed value and so cannot be used to make various kinds of
/// invalid headers.
fn make_header_value_str(
    s: &str,
) -> Result<Authorization<Bearer>, anyhow::Error> {
    Authorization::bearer(&format!("{}{}", TOKEN_PREFIX, s))
        .context("not a valid HTTP header value")
}

/// A context that can look up a Silo user and client ID from a token.
#[async_trait]
pub trait TokenContext {
    async fn token_actor(&self, token: String) -> Result<authn::Actor, Reason>;
}

/*
/// Returns a value of the `Authorization` header with `data` in the place where
/// the token goes
///
/// This is only intended for the test suite for cases where the input might not
/// be a valid Uuid and might not even be valid UTF-8.  Use `make_header_value`
/// if `data` is a valid UUID, as that function is safer and won't fail.
pub fn make_header_value_raw(
    data: &[u8],
) -> Result<http::HeaderValue, anyhow::Error> {
    let mut v: Vec<u8> = "Bearer oxide-token-".as_bytes().to_vec();
    v.extend(data);
    http::HeaderValue::from_bytes(&v).context("not a valid HTTP header value")
}

#[cfg(test)]
mod test {
    use super::super::super::Reason;
    use super::authn_spoof_parse_id;
    use super::make_header_value;
    use super::make_header_value_raw;
    use super::make_header_value_str;
    use headers::authorization::Bearer;
    use headers::authorization::Credentials;
    use headers::Authorization;
    use headers::HeaderMapExt;
    use uuid::Uuid;

    #[test]
    fn test_make_header_value() {
        let test_uuid_str = "37b56e4f-8c60-453b-a37e-99be6efe8a89";
        let test_uuid = test_uuid_str.parse::<Uuid>().unwrap();
        let header_value = make_header_value(test_uuid);
        // Other formats are valid here (e.g., changes in case or spacing).
        // However, a black-box test that accounted for those would be at least
        // as complicated as `make_header_value()` itself and wouldn't be so
        // convincing in demonstrating that that function does what we think it
        // does.
        assert_eq!(
            header_value.0.encode().to_str().unwrap(),
            "Bearer oxide-spoof-37b56e4f-8c60-453b-a37e-99be6efe8a89"
        );
    }

    #[test]
    fn test_make_header_value_raw() {
        let test_uuid_str = "37b56e4f-8c60-453b-a37e-99be6efe8a89";
        let test_uuid = test_uuid_str.parse::<Uuid>().unwrap();
        assert_eq!(
            make_header_value_raw(test_uuid_str.as_bytes()).unwrap(),
            make_header_value(test_uuid).0.encode(),
        );

        assert_eq!(
            make_header_value_raw(b"who-put-the-bomp").unwrap(),
            "Bearer oxide-spoof-who-put-the-bomp"
        );

        // This one is not a valid bearer token because it's not base64
        assert_eq!(
            make_header_value_raw(b"not base64").unwrap(),
            "Bearer oxide-spoof-not base64"
        );

        // Perhaps surprisingly, header values do not need to be valid UTF-8
        let non_utf8 = make_header_value_raw(b"not-\x80-UTF8").unwrap();
        assert_eq!(non_utf8, b"Bearer oxide-spoof-not-\x80-UTF8".as_ref(),);
        non_utf8.to_str().unwrap_err();

        // Valid UTF-8, but not a valid HTTP header
        make_header_value_raw(b"not valid \x08 HTTP header").unwrap_err();
    }

    #[test]
    fn test_spoof_header_valid() {
        let test_uuid_str = "37b56e4f-8c60-453b-a37e-99be6efe8a89";
        let test_uuid = test_uuid_str.parse::<Uuid>().unwrap();
        let test_header = make_header_value(test_uuid);

        // Success case: the client provided a valid uuid in the header.
        let success_case = authn_spoof_parse_id(Some(&test_header));
        match success_case {
            Ok(Some(actor_id)) => {
                assert_eq!(actor_id, test_uuid);
            }
            _ => {
                assert!(false);
            }
        };
    }

    #[test]
    fn test_spoof_header_missing() {
        // The client provided no credentials.
        assert!(matches!(authn_spoof_parse_id(None), Ok(None)));
    }

    #[test]
    fn test_spoof_reserved_values() {
        let bad_actor = super::SPOOF_RESERVED_BAD_ACTOR;
        let header = super::SPOOF_HEADER_BAD_ACTOR.clone();
        assert!(matches!(
            authn_spoof_parse_id(Some(&header)),
            Err(Reason::UnknownActor{ actor })
                if actor == bad_actor
        ));

        let header = super::SPOOF_HEADER_BAD_CREDS.clone();
        assert!(matches!(
            authn_spoof_parse_id(Some(&header)),
            Err(Reason::BadCredentials{
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
        let bad_inputs: Vec<&str> = vec![
            "garbage-in-garbage-can--makes-sense", // not a uuid
            "",                                    // empty value
        ];

        for input in &bad_inputs {
            let test_header = make_header_value_str(input).unwrap();
            let result = authn_spoof_parse_id(Some(&test_header));
            if let Err(error) = result {
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
        let mut map = http::HeaderMap::new();
        map.insert(&http::header::AUTHORIZATION, test_header);
        let typed_header = map.typed_get::<Authorization<Bearer>>();
        assert!(typed_header.is_none());
        assert!(matches!(
            authn_spoof_parse_id(typed_header.as_ref()),
            Ok(None)
        ));
    }
}
*/
