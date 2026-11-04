// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn sled_instance_list(
        &self,
        opctx: &OpContext,
        sled_lookup: &lookup::Sled<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::SledInstance> {
        let (.., authz_sled) =
            sled_lookup.lookup_for(authz::Action::Read).await?;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore
            .sled_instance_list(opctx, &authz_sled, pagparams)
            .await
    }
}
