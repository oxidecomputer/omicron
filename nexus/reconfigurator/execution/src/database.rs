// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages deployment of records into the database.

use anyhow::anyhow;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;

/// Idempotently ensure that the Nexus records for the zones are populated
/// in the database.
pub(crate) async fn deploy_db_metadata_nexus_records(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
) -> Result<(), anyhow::Error> {
    datastore
        .database_nexus_access_create(opctx, blueprint)
        .await
        .map_err(|err| anyhow!(err))?;
    Ok(())
}
