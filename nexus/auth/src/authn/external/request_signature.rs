// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::Details;
use super::HttpAuthnScheme;
use super::Reason;
use super::SchemeResult;
use crate::authn;
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oxide_http_signatures::Components;
use oxide_http_signatures::VerifyingKey;
use uuid::Uuid;

pub const REQUEST_SIGNATURE_SCHEME_NAME: authn::SchemeName =
    authn::SchemeName::RequestSignature;

const MAX_SIGNATURE_AGE_SECONDS: i64 = 60;

#[derive(Debug)]
pub struct HttpAuthnRequestSignature;

#[async_trait]
pub trait RequestSignatureContext {
    async fn service_account_key_for_authn(
        &self,
        key_id: Uuid,
    ) -> Result<(authn::Actor, VerifyingKey, Option<DateTime<Utc>>), Reason>;
}

fn header<'a>(
    request: &'a dropshot::RequestInfo,
    name: &str,
) -> Result<Option<&'a str>, Reason> {
    request
        .headers()
        .get(name)
        .map(|v| {
            v.to_str().map_err(|_| Reason::BadFormat {
                source: anyhow!("{name} header is not ASCII"),
            })
        })
        .transpose()
}

fn required<'a>(
    request: &'a dropshot::RequestInfo,
    name: &str,
) -> Result<&'a str, Reason> {
    header(request, name)?.ok_or_else(|| Reason::BadFormat {
        source: anyhow!("signed request is missing the {name} header"),
    })
}

fn bad_format(error: oxide_http_signatures::Error) -> Reason {
    Reason::BadFormat { source: anyhow!(error) }
}

async fn authenticate<T: RequestSignatureContext>(
    ctx: &T,
    request: &dropshot::RequestInfo,
    signature_input: &str,
) -> Result<Details, Reason> {
    let signature = required(request, oxide_http_signatures::SIGNATURE_HEADER)?;
    let content_digest =
        required(request, oxide_http_signatures::CONTENT_DIGEST_HEADER)?;
    let parsed = oxide_http_signatures::parse(signature_input, signature)
        .map_err(bad_format)?;
    let key_id: Uuid = parsed
        .key_id
        .parse()
        .map_err(|_| Reason::UnknownActor { actor: parsed.key_id.clone() })?;

    let (actor, verifying_key, time_expires) =
        ctx.service_account_key_for_authn(key_id).await?;

    parsed
        .check_window(Utc::now().timestamp(), MAX_SIGNATURE_AGE_SECONDS)
        .map_err(|e| Reason::BadCredentials { actor, source: anyhow!(e) })?;

    let authority = match request.uri().authority() {
        Some(authority) => authority.as_str(),
        None => required(request, http::header::HOST.as_str())?,
    };
    let components = Components {
        method: request.method().as_str(),
        authority,
        path: request.uri().path(),
        query: request.uri().query(),
        content_digest,
    };
    parsed
        .verify(&verifying_key, &components)
        .map_err(|e| Reason::BadCredentials { actor, source: anyhow!(e) })?;

    Ok(Details {
        actor,
        device_token_expiration: time_expires,
        credential_id: Some(key_id),
    })
}

#[async_trait]
impl<T> HttpAuthnScheme<T> for HttpAuthnRequestSignature
where
    T: RequestSignatureContext + Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        REQUEST_SIGNATURE_SCHEME_NAME
    }

    async fn authn(
        &self,
        ctx: &T,
        _log: &slog::Logger,
        request: &dropshot::RequestInfo,
    ) -> SchemeResult {
        let signature_input = match header(
            request,
            oxide_http_signatures::SIGNATURE_INPUT_HEADER,
        ) {
            Ok(Some(value)) => value,
            Ok(None) => return SchemeResult::NotRequested,
            Err(reason) => return SchemeResult::Failed(reason),
        };
        match authenticate(ctx, request, signature_input).await {
            Ok(details) => SchemeResult::Authenticated(details),
            Err(reason) => SchemeResult::Failed(reason),
        }
    }
}
