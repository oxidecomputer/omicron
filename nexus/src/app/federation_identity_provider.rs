// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_model as db;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::federation::{
    FederationIdentityProvider, FederationIdentityProviderCreate,
    FederationIdentityProviderUpdate,
};
use omicron_common::api::external::Error;
use omicron_common::api::external::http_pagination::PaginatedBy;
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn federation_identity_provider_create(
        &self,
        opctx: &OpContext,
        params: FederationIdentityProviderCreate,
    ) -> Result<FederationIdentityProvider, Error> {
        self.db_datastore
            .federation_identity_provider_create(opctx, params)
            .await?
            .try_into()
    }

    pub(crate) async fn federation_identity_provider_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> Result<Vec<FederationIdentityProvider>, Error> {
        self.db_datastore
            .federation_identity_provider_list(opctx, pagparams)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect()
    }

    pub(crate) async fn federation_identity_provider_view(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<FederationIdentityProvider, Error> {
        self.db_datastore
            .federation_identity_provider_fetch(opctx, id, authz::Action::Read)
            .await?
            .try_into()
    }

    pub(crate) async fn federation_identity_provider_update(
        &self,
        opctx: &OpContext,
        id: Uuid,
        params: FederationIdentityProviderUpdate,
    ) -> Result<FederationIdentityProvider, Error> {
        let provider = self
            .db_datastore
            .federation_identity_provider_fetch(
                opctx,
                id,
                authz::Action::Modify,
            )
            .await?;
        if params.signing_keys.is_some()
            && provider.verification_type != "static_jwks"
        {
            return Err(Error::invalid_request(
                "signing_keys can only be updated for static_jwks providers",
            ));
        }
        let update = db::FederationIdentityProviderUpdate::try_from(params)?;
        self.db_datastore
            .federation_identity_provider_update(opctx, &provider, update)
            .await?
            .try_into()
    }

    pub(crate) async fn federation_identity_provider_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<(), Error> {
        self.db_datastore.federation_identity_provider_delete(opctx, id).await
    }
}
