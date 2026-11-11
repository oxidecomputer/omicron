// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_auth::context::OpContext;
use nexus_db_lookup::{DataStoreConnection, LookupDataStore};
use omicron_common::api::external::Error;

use super::DataStore;

#[async_trait::async_trait]
impl LookupDataStore for DataStore {
    async fn pool_connection_authorized(
        &self,
        opctx: &OpContext,
    ) -> Result<DataStoreConnection, Error> {
        self.pool_connection_authorized(opctx).await
    }
}
