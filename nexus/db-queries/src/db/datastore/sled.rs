// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Sled`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::model::Sled;
use crate::db::model::SledResource;
use crate::db::model::SledUpdate;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Stores a new sled in the database.
    pub async fn sled_upsert(
        &self,
        sled_update: SledUpdate,
    ) -> CreateResult<Sled> {
        use db::schema::sled::dsl;
        diesel::insert_into(dsl::sled)
            .values(sled_update.clone().into_insertable())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::ip.eq(sled_update.ip),
                dsl::port.eq(sled_update.port),
                dsl::rack_id.eq(sled_update.rack_id),
                dsl::is_scrimlet.eq(sled_update.is_scrimlet()),
                dsl::usable_hardware_threads
                    .eq(sled_update.usable_hardware_threads),
                dsl::usable_physical_ram.eq(sled_update.usable_physical_ram),
                dsl::reservoir_size.eq(sled_update.reservoir_size),
            ))
            .returning(Sled::as_returning())
            .get_result_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Sled,
                        &sled_update.id().to_string(),
                    ),
                )
            })
    }

    pub async fn sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Sled> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::sled::dsl;
        paginated(dsl::sled, dsl::id, pagparams)
            .select(Sled::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn sled_reservation_create(
        &self,
        opctx: &OpContext,
        resource_id: Uuid,
        resource_kind: db::model::SledResourceKind,
        resources: db::model::Resources,
        constraints: db::model::SledReservationConstraints,
    ) -> CreateResult<db::model::SledResource> {
        #[derive(Debug)]
        enum SledReservationError {
            NotFound,
        }
        type TxnError = TransactionError<SledReservationError>;

        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::sled_resource::dsl as resource_dsl;
                // Check if resource ID already exists - if so, return it.
                let old_resource = resource_dsl::sled_resource
                    .filter(resource_dsl::id.eq(resource_id))
                    .select(SledResource::as_select())
                    .limit(1)
                    .load_async(&conn)
                    .await?;

                if !old_resource.is_empty() {
                    return Ok(old_resource[0].clone());
                }

                // If it doesn't already exist, find a sled with enough space
                // for the resources we're requesting.
                use db::schema::sled::dsl as sled_dsl;
                // This answers the boolean question:
                // "Does the SUM of all hardware thread usage, plus the one we're trying
                // to allocate, consume less threads than exists on the sled?"
                let sled_has_space_for_threads =
                    (diesel::dsl::sql::<diesel::sql_types::BigInt>(&format!(
                        "COALESCE(SUM(CAST({} as INT8)), 0)",
                        resource_dsl::hardware_threads::NAME
                    )) + resources.hardware_threads)
                        .le(sled_dsl::usable_hardware_threads);

                // This answers the boolean question:
                // "Does the SUM of all RAM usage, plus the one we're trying
                // to allocate, consume less RAM than exists on the sled?"
                let sled_has_space_for_rss =
                    (diesel::dsl::sql::<diesel::sql_types::BigInt>(&format!(
                        "COALESCE(SUM(CAST({} as INT8)), 0)",
                        resource_dsl::rss_ram::NAME
                    )) + resources.rss_ram)
                        .le(sled_dsl::usable_physical_ram);

                // Determine whether adding this service's reservoir allocation
                // to what's allocated on the sled would avoid going over quota.
                let sled_has_space_in_reservoir =
                    (diesel::dsl::sql::<diesel::sql_types::BigInt>(&format!(
                        "COALESCE(SUM(CAST({} as INT8)), 0)",
                        resource_dsl::reservoir_ram::NAME
                    )) + resources.reservoir_ram)
                        .le(sled_dsl::reservoir_size);

                // Generate a query describing all of the sleds that have space
                // for this reservation.
                let mut sled_targets = sled_dsl::sled
                    .left_join(
                        resource_dsl::sled_resource
                            .on(resource_dsl::sled_id.eq(sled_dsl::id)),
                    )
                    .group_by(sled_dsl::id)
                    .having(
                        sled_has_space_for_threads
                            .and(sled_has_space_for_rss)
                            .and(sled_has_space_in_reservoir),
                    )
                    .filter(sled_dsl::time_deleted.is_null())
                    .select(sled_dsl::id)
                    .into_boxed();

                // Further constrain the sled IDs according to any caller-
                // supplied constraints.
                if let Some(must_select_from) = constraints.must_select_from() {
                    sled_targets = sled_targets
                        .filter(sled_dsl::id.eq_any(must_select_from.to_vec()));
                }

                sql_function!(fn random() -> diesel::sql_types::Float);
                let sled_targets = sled_targets
                    .order(random())
                    .limit(1)
                    .get_results_async::<Uuid>(&conn)
                    .await?;

                if sled_targets.is_empty() {
                    return Err(TxnError::CustomError(
                        SledReservationError::NotFound,
                    ));
                }

                // Create a SledResource record, associate it with the target
                // sled.
                let resource = SledResource::new(
                    resource_id,
                    sled_targets[0],
                    resource_kind,
                    resources,
                );

                Ok(diesel::insert_into(resource_dsl::sled_resource)
                    .values(resource)
                    .returning(SledResource::as_returning())
                    .get_result_async(&conn)
                    .await?)
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(SledReservationError::NotFound) => {
                    external::Error::unavail_external(
                        "No sleds can fit the requested instance",
                    )
                }
                TxnError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn sled_reservation_delete(
        &self,
        opctx: &OpContext,
        resource_id: Uuid,
    ) -> DeleteResult {
        use db::schema::sled_resource::dsl as resource_dsl;
        diesel::delete(resource_dsl::sled_resource)
            .filter(resource_dsl::id.eq(resource_id))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::datastore_test;
    use crate::db::datastore::test::{
        sled_baseboard_for_test, sled_system_hardware_for_test,
    };
    use crate::db::model::ByteCount;
    use crate::db::model::SqlU32;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use std::net::{Ipv6Addr, SocketAddrV6};

    fn rack_id() -> Uuid {
        Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
    }

    #[tokio::test]
    async fn upsert_sled_updates_hardware() {
        let logctx = dev::test_setup_log("upsert_sled");
        let mut db = test_setup_database(&logctx.log).await;
        let (_opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled_id = Uuid::new_v4();
        let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
        let mut sled_update = SledUpdate::new(
            sled_id,
            addr,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id(),
        );
        let observed_sled = datastore
            .sled_upsert(sled_update.clone())
            .await
            .expect("Could not upsert sled during test prep");
        assert_eq!(
            observed_sled.usable_hardware_threads,
            sled_update.usable_hardware_threads
        );
        assert_eq!(
            observed_sled.usable_physical_ram,
            sled_update.usable_physical_ram
        );
        assert_eq!(observed_sled.reservoir_size, sled_update.reservoir_size);

        // Modify the sizes of hardware
        sled_update.usable_hardware_threads =
            SqlU32::new(sled_update.usable_hardware_threads.0 + 1);
        const MIB: u64 = 1024 * 1024;
        sled_update.usable_physical_ram = ByteCount::from(
            external::ByteCount::try_from(
                sled_update.usable_physical_ram.0.to_bytes() + MIB,
            )
            .unwrap(),
        );
        sled_update.reservoir_size = ByteCount::from(
            external::ByteCount::try_from(
                sled_update.reservoir_size.0.to_bytes() + MIB,
            )
            .unwrap(),
        );

        // Test that upserting the sled propagates those changes to the DB.
        let observed_sled = datastore
            .sled_upsert(sled_update.clone())
            .await
            .expect("Could not upsert sled during test prep");
        assert_eq!(
            observed_sled.usable_hardware_threads,
            sled_update.usable_hardware_threads
        );
        assert_eq!(
            observed_sled.usable_physical_ram,
            sled_update.usable_physical_ram
        );
        assert_eq!(observed_sled.reservoir_size, sled_update.reservoir_size);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
