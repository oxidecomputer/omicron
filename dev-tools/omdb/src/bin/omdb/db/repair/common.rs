// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_lookup::DbConnection;
use nexus_db_model::LocalStorageUnencryptedDatasetAllocation;
use nexus_db_model::Sled;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_queries::db::DataStore;
use slog::Logger;
use slog::o;

/// Call out to the sled-agent to delete the local storage on the sled, and set
/// time_deleted on the database record.
pub(crate) async fn remove_local_storage_allocation(
    log: &Logger,
    conn: &async_bb8_diesel::Connection<DbConnection>,
    allocation: &LocalStorageUnencryptedDatasetAllocation,
) -> Result<()> {
    let sled_id = allocation.sled_id();
    let zpool_id = allocation.pool_id().upcast();

    let request = sled_agent_client::types::LocalStorageDatasetDeleteRequest {
        zpool_id: allocation.pool_id(),
        dataset_id: allocation.id(),
        encrypted_at_rest: false,
    };

    // If the sled or pool was expunged then there's nothing we have to
    // do!

    let sled_in_service =
        DataStore::check_sled_in_service_on_connection(&conn, sled_id).await?;

    let zpool_in_service =
        DataStore::check_zpool_in_service_on_connection(&conn, zpool_id)
            .await?;

    if !sled_in_service {
        println!("sled {sled_id} no longer in service, exiting");
        return Ok(());
    }

    if !zpool_in_service {
        println!("zpool {zpool_id} no longer in service, exiting");
        return Ok(());
    }

    // Get a sled agent client for this sled

    let sled = {
        use nexus_db_schema::schema::sled::dsl;

        dsl::sled
            .filter(dsl::id.eq(to_db_typed_uuid(sled_id)))
            .select(Sled::as_select())
            .get_result_async(conn)
            .await
            .unwrap()
    };

    let sled_agent_url = format!("http://{}", sled.address());

    let sled_agent_client = {
        let log = log.new(o!("SledAgent" => sled_id.to_string()));
        sled_agent_client::Client::new(&sled_agent_url, log)
    };

    // Ensure that the local storage is deleted

    println!("deleting local storage allocation on sled");

    sled_agent_client.local_storage_dataset_delete(&request).await?;

    // Finally, delete the record

    println!("deleting local storage allocation record");

    DataStore::delete_local_storage_unencrypted_dataset_allocation_in_txn(
        &conn,
        allocation.id(),
    )
    .await?;

    Ok(())
}
