// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{HttpAuthnScheme, Reason, SchemeResult};
use crate::authn;
use async_trait::async_trait;
use headers::HeaderMapExt;
use headers::authorization::{Authorization, Bearer};

#[derive(Debug)]
pub struct HttpAuthnServiceAccountToken;

#[async_trait]
pub trait ServiceAccountTokenContext {
    async fn authenticate_service_account_token(
        &self,
        token: String,
    ) -> Result<authn::Details, Reason>;
}

#[async_trait]
impl<T> HttpAuthnScheme<T> for HttpAuthnServiceAccountToken
where
    T: ServiceAccountTokenContext + Send + Sync + 'static,
{
    fn name(&self) -> authn::SchemeName {
        authn::SchemeName::ServiceAccountToken
    }

    async fn authn(
        &self,
        ctx: &T,
        _log: &slog::Logger,
        request: &dropshot::RequestInfo,
    ) -> SchemeResult {
        let Some(bearer) =
            request.headers().typed_get::<Authorization<Bearer>>()
        else {
            return SchemeResult::NotRequested;
        };
        let Some(token) = bearer.token().strip_prefix("oxide-service-account-")
        else {
            return SchemeResult::NotRequested;
        };
        match ctx.authenticate_service_account_token(token.to_owned()).await {
            Ok(details) => SchemeResult::Authenticated(details),
            Err(error) => SchemeResult::Failed(error),
        }
    }
}
