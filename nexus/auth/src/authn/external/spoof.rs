// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Custom, test-only authn scheme that trusts whatever the client says

use std::sync::LazyLock;

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use super::SiloUserSilo;
use crate::authn::Actor;
use crate::authn::{self, AuthMethod};
use anyhow::Context;
use anyhow::anyhow;
use async_trait::async_trait;
use headers::HeaderMapExt;
use headers::authorization::{Authorization, Bearer};
use omicron_uuid_kinds::SiloUserUuid;
use slog::debug;
use std::str::FromStr;

// This scheme is intended for demos, development, and testing until we have a
// more automatable identity provider that can be used for those purposes.
//
// For ease of integration into existing clients, we use RFC 6750 bearer tokens.
// This mechanism in turn uses HTTP's "Authorization" header.  In practice, it
// looks like this:
//
//     Authorization: Bearer oxide-spoof-001de000-05e4-4000-8000-000000060001
//     ^^^^^^^^^^^^^  ^^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//     |              |      |           |
//     |              |      |           +--- specifies the actor id
//     |              |      +--------------- specifies this "spoof" mechanism
//     |              +---------------------- specifies RFC 6750 bearer tokens
//     +------------------------------------- standard HTTP authentication hdr
//
// (That's not a typo -- the "authorization" header is generally used to specify
// _authentication_ information.  Similarly, the "Unauthorized" HTTP response
// code usually describes an _authentication_ error.)
//
// This mechanism trusts (without verification) that the client is whoever
// they say they are.  That's true of any bearer token, but this one is
// particularly dangerous because the tokens are long-lived and not secret.

pub const SPOOF_SCHEME_NAME: authn::SchemeName = authn::SchemeName("spoof");

/// Magic value to produce a "no such actor" error
const SPOOF_RESERVED_BAD_ACTOR: &str = "Jack-Donaghy";
/// Magic value to produce a "bad credentials" error
const SPOOF_RESERVED_BAD_CREDS: &str = "this-fake-ID-it-is-truly-excellent";
/// Prefix used on the bearer token to identify this scheme
// RFC 6750 expects bearer tokens to be opaque base64-encoded data.  In our
// case, the data we want to represent (this prefix, plus valid uuids) are
// subsets of the base64 character set, so we do not bother encoding them.
const SPOOF_PREFIX: &str = "oxide-spoof-";

/// Actor (id) used for the special "bad credentials" error
static SPOOF_RESERVED_BAD_CREDS_ACTOR: LazyLock<Actor> =
    LazyLock::new(|| Actor::UserBuiltin {
        user_builtin_id: "22222222-2222-2222-2222-222222222222"
            .parse()
            .unwrap(),
    });

/// Complete HTTP header value to trigger the "bad actor" error
pub static SPOOF_HEADER_BAD_ACTOR: LazyLock<Authorization<Bearer>> =
    LazyLock::new(|| make_header_value_str(SPOOF_RESERVED_BAD_ACTOR).unwrap());

/// Complete HTTP header value to trigger the "bad creds" error
pub static SPOOF_HEADER_BAD_CREDS: LazyLock<Authorization<Bearer>> =
    LazyLock::new(|| make_header_value_str(SPOOF_RESERVED_BAD_CREDS).unwrap());

/// Implements a (test-only) authentication scheme where the client simply
/// provides the actor information in a custom bearer token and we always trust
/// it.  This is useful for testing the rest of the authn facilities since we
/// can very easily and precisely control its output.
#[derive(Debug)]
pub struct HttpAuthnSpoof;

#[async_trait]
impl<T> HttpAuthnScheme<T> for HttpAuthnSpoof
where
    T: SiloUserSilo + Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        SPOOF_SCHEME_NAME
    }

    async fn authn(
        &self,
        ctx: &T,
        log: &slog::Logger,
        request: &dropshot::RequestInfo,
    ) -> SchemeResult {
        let headers = request.headers();
        match authn_spoof_parse_id(headers.typed_get().as_ref()) {
            Err(error) => SchemeResult::Failed(error),
            Ok(None) => SchemeResult::NotRequested,
            Ok(Some(silo_user_id)) => {
                debug!(
                    log,
                    "looking up silo for user";
                    "silo_user_id" => silo_user_id.to_string()
                );
                match ctx.silo_user_silo(silo_user_id).await {
                    Err(error) => SchemeResult::Failed(error),
                    Ok(silo_id) => {
                        let actor = Actor::SiloUser { silo_id, silo_user_id };
                        SchemeResult::Authenticated(Details {
                            actor,
                            auth_method: AuthMethod::ConsoleSession,
                        })
                    }
                }
            }
        }
    }
}

fn authn_spoof_parse_id(
    raw_value: Option<&Authorization<Bearer>>,
) -> Result<Option<SiloUserUuid>, Reason> {
    let token = match raw_value {
        None => return Ok(None),
        Some(bearer) => bearer.token(),
    };

    if !token.starts_with(SPOOF_PREFIX) {
        // This is some other kind of bearer token.  Maybe another scheme knows
        // how to deal with it.
        return Ok(None);
    }

    let str_value = &token[SPOOF_PREFIX.len()..];
    if str_value == SPOOF_RESERVED_BAD_ACTOR {
        return Err(Reason::UnknownActor { actor: str_value.to_owned() });
    }

    if str_value == SPOOF_RESERVED_BAD_CREDS {
        return Err(Reason::BadCredentials {
            actor: *SPOOF_RESERVED_BAD_CREDS_ACTOR,
            source: anyhow!("do not sell to the people on this list"),
        });
    }

    SiloUserUuid::from_str(str_value)
        .context("parsing header value as UUID")
        .map(|silo_user_id| Some(silo_user_id))
        .map_err(|source| Reason::BadFormat { source })
}

/// Returns a value of the `Authorization` header for this actor that will be
/// accepted using this scheme
pub fn make_header_value<T: std::fmt::Display>(id: T) -> Authorization<Bearer> {
    make_header_value_str(&id.to_string()).unwrap()
}

/// Returns a value of the `Authorization` header with `str` in the place where
/// the actor id goes
///
/// Unlike `make_header_value`, this can is not guaranteed to work, as the
/// string may contain non-base64 characters.  Unlike `make_header_value_raw`,
/// this returns a typed value and so cannot be used to make various kinds of
/// invalid headers.
fn make_header_value_str(
    s: &str,
) -> Result<Authorization<Bearer>, anyhow::Error> {
    Authorization::bearer(&format!("{}{}", SPOOF_PREFIX, s))
        .context("not a valid HTTP header value")
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

#[cfg(test)]
mod test {
    use super::super::super::Reason;
    use super::authn_spoof_parse_id;
    use super::make_header_value;
    use super::make_header_value_raw;
    use super::make_header_value_str;
    use headers::Authorization;
    use headers::HeaderMapExt;
    use headers::authorization::Bearer;
    use headers::authorization::Credentials;
    use omicron_uuid_kinds::SiloUserUuid;
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
        let test_uuid = test_uuid_str.parse::<SiloUserUuid>().unwrap();
        let test_header = make_header_value(test_uuid);

        // Success case: the client provided a valid uuid in the header.
        let success_case = authn_spoof_parse_id(Some(&test_header));
        match success_case {
            Ok(Some(silo_user_id)) => {
                assert_eq!(silo_user_id, test_uuid);
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
