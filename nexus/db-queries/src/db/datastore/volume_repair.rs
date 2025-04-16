// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`VolumeRepair`]s.

use super::DataStore;
use crate::db;
use crate::db::DbConnection;
use crate::db::datastore::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::model::VolumeRepair;
use crate::db::model::to_db_typed_uuid;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::VolumeUuid;
use uuid::Uuid;

impl DataStore {
    /// Insert a volume repair record, taking a "lock" on the volume pointed to
    /// by volume id with some repair id.
    ///
    /// If there exists a record that has a matching volume id and repair id,
    /// return Ok(()).
    ///
    /// If there is no volume that matches the given volume id, return an error:
    /// it should not be possible to lock a volume that does not exist! Note
    /// that it is possible to lock a soft-deleted volume.
    ///
    /// If there is already an existing record that has a matching volume id but
    /// a different repair id, then this function returns an Error::conflict.
    pub(super) async fn volume_repair_insert_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<Error>,
        volume_id: VolumeUuid,
        repair_id: Uuid,
    ) -> Result<(), diesel::result::Error> {
        use nexus_db_schema::schema::volume_repair::dsl;

        // If a lock that matches the arguments exists already, return Ok
        //
        // Note: if rerunning this function (for example if a saga node was
        // rerun), the volume could have existed when this lock was inserted the
        // first time, but have been deleted now.
        let maybe_lock = dsl::volume_repair
            .filter(dsl::repair_id.eq(repair_id))
            .filter(dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
            .first_async::<VolumeRepair>(conn)
            .await
            .optional()?;

        if maybe_lock.is_some() {
            return Ok(());
        }

        // Do not allow a volume repair record to be created if the volume does
        // not exist, or was hard-deleted!
        let maybe_volume = Self::volume_get_impl(conn, volume_id).await?;

        if maybe_volume.is_none() {
            return Err(err.bail(Error::invalid_request(format!(
                "cannot create record: volume {volume_id} does not exist"
            ))));
        }

        // Do not check for soft-deletion here: We may want to request locks for
        // soft-deleted volumes.

        match diesel::insert_into(dsl::volume_repair)
            .values(VolumeRepair { volume_id: volume_id.into(), repair_id })
            .execute_async(conn)
            .await
        {
            Ok(_) => Ok(()),

            Err(e) => match e {
                DieselError::DatabaseError(
                    DatabaseErrorKind::UniqueViolation,
                    ref error_information,
                ) if error_information.constraint_name()
                    == Some("volume_repair_pkey") =>
                {
                    Err(err.bail(Error::conflict("volume repair lock")))
                }

                _ => Err(e),
            },
        }
    }

    pub async fn volume_repair_lock(
        &self,
        opctx: &OpContext,
        volume_id: VolumeUuid,
        repair_id: Uuid,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper("volume_repair_lock")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::volume_repair_insert_in_txn(
                        &conn, err, volume_id, repair_id,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub(super) fn volume_repair_delete_query(
        volume_id: VolumeUuid,
        repair_id: Uuid,
    ) -> impl RunnableQuery<VolumeRepair> {
        use nexus_db_schema::schema::volume_repair::dsl;

        diesel::delete(
            dsl::volume_repair
                .filter(dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
                .filter(dsl::repair_id.eq(repair_id)),
        )
    }

    pub async fn volume_repair_unlock(
        &self,
        opctx: &OpContext,
        volume_id: VolumeUuid,
        repair_id: Uuid,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::volume_repair_delete_query(volume_id, repair_id)
            .execute_async(&*conn)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn volume_repair_get(
        conn: &async_bb8_diesel::Connection<db::DbConnection>,
        volume_id: VolumeUuid,
        repair_id: Uuid,
    ) -> Result<VolumeRepair, DieselError> {
        use nexus_db_schema::schema::volume_repair::dsl;

        dsl::volume_repair
            .filter(dsl::repair_id.eq(repair_id))
            .filter(dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
            .first_async::<VolumeRepair>(conn)
            .await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::VolumeConstructionRequest;

    #[tokio::test]
    async fn volume_lock_conflict_error_returned() {
        let logctx = dev::test_setup_log("volume_lock_conflict_error_returned");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let lock_1 = Uuid::new_v4();
        let lock_2 = Uuid::new_v4();
        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        datastore.volume_repair_lock(&opctx, volume_id, lock_1).await.unwrap();

        let err = datastore
            .volume_repair_lock(&opctx, volume_id, lock_2)
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Conflict { .. }));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that you can't take a volume repair lock if the volume does not
    /// exist yet!
    #[tokio::test]
    async fn volume_lock_should_fail_without_volume() {
        let logctx =
            dev::test_setup_log("volume_lock_should_fail_without_volume");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let lock_1 = Uuid::new_v4();
        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_repair_lock(&opctx, volume_id, lock_1)
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn volume_lock_relock_allowed() {
        let logctx = dev::test_setup_log("volume_lock_relock_allowed");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let lock_id = Uuid::new_v4();
        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        datastore.volume_repair_lock(&opctx, volume_id, lock_id).await.unwrap();
        datastore.volume_repair_lock(&opctx, volume_id, lock_id).await.unwrap();

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
