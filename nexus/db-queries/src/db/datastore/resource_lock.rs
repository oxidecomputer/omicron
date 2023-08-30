// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`ResourceLock`]s.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::model::ResourceLock;
use async_bb8_diesel::AsyncConnection;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use uuid::Uuid;

impl DataStore {
    /// Attempt locking a resource with a certain lock ID. If this function
    /// returns Ok, then that lock was grabbed successfully.
    pub async fn attempt_resource_lock(
        &self,
        resource_id: Uuid,
        lock_id: Uuid,
    ) -> CreateResult<()> {
        self.pool()
            .transaction(move |conn| {
                use db::schema::resource_lock::dsl;

                let existing_lock: Option<ResourceLock> = dsl::resource_lock
                    .filter(dsl::resource_id.eq(resource_id))
                    .first(conn)
                    .optional()
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e.into(),
                            ErrorHandler::Server,
                        )
                    })?;

                if let Some(existing_lock) = existing_lock {
                    if existing_lock.lock_id != lock_id {
                        return Err(TransactionError::CustomError(
                            Error::invalid_request(&format!(
                            "resource {} already locked by another lock ID {}",
                            resource_id, lock_id,
                        )),
                        ));
                    } else {
                        return Ok(());
                    }
                }

                diesel::insert_into(dsl::resource_lock)
                    .values(ResourceLock { resource_id, lock_id })
                    .execute(conn)
                    .map(|_rows_inserted| ())
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e.into(),
                            ErrorHandler::Server,
                        )
                    })?;

                Ok(())
            })
            .await
            .map_err(|e: TransactionError<Error>| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }

    /// Delete the resource lock record if we are the owner of that lock.
    pub async fn free_resource_lock(
        &self,
        resource_id: Uuid,
        lock_id: Uuid,
    ) -> DeleteResult {
        self.pool()
            .transaction(move |conn| {
                use db::schema::resource_lock::dsl;

                let existing_lock: Option<ResourceLock> = dsl::resource_lock
                    .filter(dsl::resource_id.eq(resource_id))
                    .first(conn)
                    .optional()
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e.into(),
                            ErrorHandler::Server,
                        )
                    })?;

                if let Some(existing_lock) = existing_lock {
                    if existing_lock.lock_id != lock_id {
                        return Err(TransactionError::CustomError(
                            Error::invalid_request(&format!(
                                "resource {} locked by another lock ID {}",
                                resource_id, lock_id,
                            )),
                        ));
                    }
                }

                diesel::delete(dsl::resource_lock)
                    .filter(
                        dsl::resource_id
                            .eq(resource_id)
                            .and(dsl::lock_id.eq(lock_id)),
                    )
                    .execute(conn)
                    .map(|_rows_updated| ())
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e.into(),
                            ErrorHandler::Server,
                        )
                    })?;

                Ok(())
            })
            .await
            .map_err(|e: TransactionError<Error>| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::datastore_test;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn resource_locking_works() {
        let logctx = dev::test_setup_log("upsert_sled");
        let db = test_setup_database(&logctx.log).await;
        let (_opctx, datastore) = datastore_test(&logctx, &db).await;

        let resource_id = Uuid::new_v4();
        let lock_id = Uuid::new_v4();

        // Grab a lock
        datastore.attempt_resource_lock(resource_id, lock_id).await.unwrap();

        // Ensure attempting to grab a lock with another lock ID fails
        datastore
            .attempt_resource_lock(resource_id, Uuid::new_v4())
            .await
            .unwrap_err();

        // Ensure attempting to delete a lock with another lock ID fails
        datastore
            .attempt_resource_lock(resource_id, Uuid::new_v4())
            .await
            .unwrap_err();

        // Grabbing the lock multiple times is ok
        datastore.attempt_resource_lock(resource_id, lock_id).await.unwrap();

        // Delete the lock
        datastore.free_resource_lock(resource_id, lock_id).await.unwrap();

        // Deleting the lock multiple times is ok
        datastore.free_resource_lock(resource_id, lock_id).await.unwrap();

        // If there is no lock, deleting a lock with another lock ID succeeds,
        // but there is no lock so nothing happened.
        datastore
            .free_resource_lock(resource_id, Uuid::new_v4())
            .await
            .unwrap();
    }
}
