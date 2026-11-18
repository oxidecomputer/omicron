// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `wicket` SSH authentication control

use nexus_auth::context::OpContext;
use nexus_db_queries::db::datastore::WicketSshAuthGenerationDescription;
use nexus_types::external_api::wicket_ssh_auth::WicketSshAuth;
use nexus_types::external_api::wicket_ssh_auth::WicketSshAuthCreate;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::LookupResult;

impl super::Nexus {
    pub async fn wicket_ssh_auth_insert(
        &self,
        opctx: &OpContext,
        params: WicketSshAuthCreate,
    ) -> CreateResult<WicketSshAuth> {
        self.datastore().wicket_ssh_auth_insert(opctx, params).await
    }

    pub async fn wicket_ssh_auth_view(
        &self,
        opctx: &OpContext,
        generation: WicketSshAuthGenerationDescription,
    ) -> LookupResult<WicketSshAuth> {
        self.datastore().wicket_ssh_auth_read(opctx, generation).await
    }
}
