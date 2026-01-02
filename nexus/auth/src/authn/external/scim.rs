// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! SCIM-only bearer tokens

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use crate::authn;
use async_trait::async_trait;
use headers::HeaderMapExt;
use headers::authorization::{Authorization, Bearer};

// This scheme is intended only for SCIM provisioning clients.
//
// For ease of integration into existing clients, we use RFC 6750 bearer tokens.
// This mechanism in turn uses HTTP's "Authorization" header.  In practice, it
// looks like this:
//
//     Authorization: Bearer oxide-scim-01c90c58085fed6a230d137b9b9b5e7501d0a523
//     ^^^^^^^^^^^^^  ^^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//     |              |      |           |
//     |              |      |           +--- specifies the token itself
//     |              |      +--------------- specifies this "token" mechanism
//     |              +---------------------- specifies RFC 6750 bearer tokens
//     +------------------------------------- standard HTTP authentication hdr
//
// (That's not a typo -- the "authorization" header is generally used to specify
// _authentication_ information.  Similarly, the "Unauthorized" HTTP response
// code usually describes an _authentication_ error.)

pub const SCIM_TOKEN_SCHEME_NAME: authn::SchemeName =
    authn::SchemeName("scim_token");

/// Prefix used on the bearer token to identify this scheme
// RFC 6750 expects bearer tokens to be opaque base64-encoded data. In our case,
// the data we want to represent (this prefix, plus valid tokens) are subsets of
// the base64 character set, so we do not bother encoding them.
const TOKEN_PREFIX: &str = "oxide-scim-";

/// Implements a SCIM provisioning client specific bearer-token-based
/// authentication scheme.
#[derive(Debug)]
pub struct HttpAuthnScimToken;

#[async_trait]
impl<T> HttpAuthnScheme<T> for HttpAuthnScimToken
where
    T: ScimTokenContext + Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        SCIM_TOKEN_SCHEME_NAME
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
            Ok(Some(token)) => match ctx.scim_token_actor(token).await {
                Err(error) => SchemeResult::Failed(error),
                Ok(actor) => SchemeResult::Authenticated(Details {
                    actor,
                    device_token_expiration: None,
                }),
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

/// A context that can look up a Actor::Scim from a token.
#[async_trait]
pub trait ScimTokenContext {
    async fn scim_token_actor(
        &self,
        token: String,
    ) -> Result<authn::Actor, Reason>;
}
