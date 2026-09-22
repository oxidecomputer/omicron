// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_queries::context::OpContext;
use nexus_types::external_api::federation::{
    FederationTrustPolicy, FederationTrustPolicyCreate,
    FederationTrustPolicyUpdate,
};
use omicron_common::api::external::Error;
use omicron_common::api::external::http_pagination::PaginatedBy;
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn federation_trust_policy_create(
        &self,
        opctx: &OpContext,
        params: FederationTrustPolicyCreate,
    ) -> Result<FederationTrustPolicy, Error> {
        self.db_datastore.federation_trust_policy_create(opctx, params).await
    }

    pub(crate) async fn federation_trust_policy_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> Result<Vec<FederationTrustPolicy>, Error> {
        self.db_datastore.federation_trust_policy_list(opctx, pagparams).await
    }

    pub(crate) async fn federation_trust_policy_view(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<FederationTrustPolicy, Error> {
        self.db_datastore.federation_trust_policy_view(opctx, id).await
    }

    pub(crate) async fn federation_trust_policy_update(
        &self,
        opctx: &OpContext,
        id: Uuid,
        params: FederationTrustPolicyUpdate,
    ) -> Result<FederationTrustPolicy, Error> {
        self.db_datastore
            .federation_trust_policy_update(opctx, id, params)
            .await
    }

    pub(crate) async fn federation_trust_policy_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<(), Error> {
        self.db_datastore.federation_trust_policy_delete(opctx, id).await
    }
}
