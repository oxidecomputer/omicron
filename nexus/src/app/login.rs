// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{HttpError, HttpResponseFound, http_response_found};
use nexus_auth::context::OpContext;
use nexus_db_model::{ConsoleSession, Name};
use nexus_db_queries::authn::silos::IdentityProviderType;
use nexus_types::external_api::{params::RelativeUri, shared::RelayState};

impl super::Nexus {
    pub(crate) async fn login_saml_redirect(
        &self,
        opctx: &OpContext,
        silo_name: &Name,
        provider_name: &Name,
        redirect_uri: Option<RelativeUri>,
    ) -> Result<HttpResponseFound, HttpError> {
        let (.., identity_provider) = self
            .datastore()
            .identity_provider_lookup(&opctx, silo_name, provider_name)
            .await?;

        match identity_provider {
            IdentityProviderType::Saml(saml_identity_provider) => {
                // Relay state is sent to the IDP, to be sent back to the SP
                // after a successful login.
                let relay_state =
                    RelayState { redirect_uri }.to_encoded().map_err(|e| {
                        HttpError::for_internal_error(format!(
                            "encoding relay state failed: {}",
                            e
                        ))
                    })?;

                let sign_in_url = saml_identity_provider
                    .sign_in_url(Some(relay_state))
                    .map_err(|e| {
                        HttpError::for_internal_error(e.to_string())
                    })?;

                http_response_found(sign_in_url)
            }
        }
    }

    pub(crate) async fn login_saml(
        &self,
        opctx: &OpContext,
        body_bytes: dropshot::UntypedBody,
        silo_name: &Name,
        provider_name: &Name,
    ) -> Result<(ConsoleSession, String), HttpError> {
        let (authz_silo, db_silo, identity_provider) = self
            .datastore()
            .identity_provider_lookup(&opctx, silo_name, provider_name)
            .await?;
        let (authenticated_subject, relay_state_string) =
            match identity_provider {
                IdentityProviderType::Saml(saml_identity_provider) => {
                    let body_bytes = body_bytes.as_str()?;
                    saml_identity_provider.authenticated_subject(
                        &body_bytes,
                        self.samael_max_issue_delay(),
                    )?
                }
            };
        let relay_state =
            relay_state_string.and_then(|v| RelayState::from_encoded(v).ok());
        let user = self
            .silo_user_from_authenticated_subject(
                &opctx,
                &authz_silo,
                &db_silo,
                &authenticated_subject,
            )
            .await?;
        let session = self.session_create(opctx, &user).await?;
        let next_url = relay_state
            .and_then(|r| r.redirect_uri)
            .map(|u| u.to_string())
            .unwrap_or_else(|| "/".to_string());
        Ok((session, next_url))
    }
}
