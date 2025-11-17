// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client tokens

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use super::SiloUserSilo;
use crate::authn::{self, AuthMethod};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use headers::HeaderMapExt;
use headers::authorization::{Authorization, Bearer};

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
    T: SiloUserSilo + TokenContext + Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        TOKEN_SCHEME_NAME
    }

    async fn authn(
        &self,
        ctx: &T,
        _log: &slog::Logger,
        request: &dropshot::RequestInfo,
    ) -> SchemeResult {
        let headers = request.headers();
        match parse_token(headers.typed_get().as_ref()) {
            Err(error) => SchemeResult::Failed(error),
            Ok(None) => SchemeResult::NotRequested,
            Ok(Some(token)) => match ctx.token_actor(token).await {
                Err(error) => SchemeResult::Failed(error),
                Ok((actor, expiration)) => {
                    SchemeResult::Authenticated(Details {
                        actor,
                        auth_method: AuthMethod::DeviceToken { expiration },
                    })
                }
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

/// A context that can look up a Silo user and client ID from a token.
#[async_trait]
pub trait TokenContext {
    /// Returns the actor authenticated by the token and the token's expiration time (if any).
    async fn token_actor(
        &self,
        token: String,
    ) -> Result<(authn::Actor, Option<DateTime<Utc>>), Reason>;
}

#[cfg(test)]
mod test {
    use super::{TOKEN_PREFIX, parse_token};
    use anyhow::Context;
    use headers::Authorization;
    use headers::HeaderMapExt;
    use headers::authorization::Bearer;
    use headers::authorization::Credentials;

    /// Returns a value of the `Authorization` header for this actor that will be
    /// accepted using this scheme.
    fn make_header_value(token: &str) -> Authorization<Bearer> {
        make_header_value_str(token).unwrap()
    }

    /// Returns a value of the `Authorization` header with `str` in the place where
    /// the token goes.
    fn make_header_value_str(
        s: &str,
    ) -> Result<Authorization<Bearer>, anyhow::Error> {
        Authorization::bearer(&format!("{}{}", TOKEN_PREFIX, s))
            .context("not a valid HTTP header value")
    }

    /// Returns a value of the `Authorization` header with `data` in the place where
    /// the token goes. This is only intended for the test suite for cases where the
    /// input might not be valid UTF-8.
    pub fn make_header_value_raw(
        data: &[u8],
    ) -> Result<http::HeaderValue, anyhow::Error> {
        let mut v: Vec<u8> =
            format!("Bearer {}", TOKEN_PREFIX).as_bytes().to_vec();
        v.extend(data);
        http::HeaderValue::from_bytes(&v)
            .context("not a valid HTTP header value")
    }

    #[test]
    fn test_make_header_value() {
        let test_token = "AAAAAAATOKEN";
        let header_value = make_header_value(test_token);
        assert_eq!(
            header_value.0.encode().to_str().unwrap(),
            "Bearer oxide-token-AAAAAAATOKEN"
        );
    }

    #[test]
    fn test_token_header_valid() {
        let test_token = "AAAAAAANOTHERTOKEN";
        let test_header = make_header_value(test_token);

        let parsed_token = parse_token(Some(&test_header));
        match parsed_token {
            Ok(Some(token)) => assert_eq!(token, test_token),
            _ => assert!(false),
        };
    }

    #[test]
    fn test_token_header_missing() {
        // The client provided no credentials.
        assert!(matches!(parse_token(None), Ok(None)));
    }

    #[test]
    fn test_token_header_bad_utf8() {
        // This case is not UTF-8.  It's a valid HTTP header.  The
        // implementation cannot easily distinguish:
        //
        //    Authorization: Bearer oxide-token-foo\x80ar
        //
        // which is clearly intended for "token" (but invalid input) from
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
        assert!(matches!(parse_token(typed_header.as_ref()), Ok(None)));
    }
}
