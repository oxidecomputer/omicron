// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on `unadopted_rack` and `unadopted_sled`, which track
//! racks and sleds that have joined the underlay network but have not yet been
//! adopted into the multirack cluster.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::UnadoptedRack;
use crate::db::model::UnadoptedSled;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use uuid::Uuid;

impl DataStore {
    /// Insert a new unadopted rack.
    ///
    /// Fails with `ObjectAlreadyExists` if a row with this ID exists, including
    /// one that has been soft-deleted.
    pub async fn unadopted_rack_insert(
        &self,
        opctx: &OpContext,
        rack: UnadoptedRack,
    ) -> CreateResult<UnadoptedRack> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let id = rack.id().to_string();

        use nexus_db_schema::schema::unadopted_rack::dsl;
        diesel::insert_into(dsl::unadopted_rack)
            .values(rack)
            .returning(UnadoptedRack::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(ResourceType::Rack, &id),
                )
            })
    }

    /// Fetch an unadopted rack by ID.
    pub async fn unadopted_rack_get(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> LookupResult<UnadoptedRack> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::unadopted_rack::dsl;
        dsl::unadopted_rack
            .filter(dsl::id.eq(to_db_typed_uuid(rack_id)))
            .filter(dsl::time_deleted.is_null())
            .select(UnadoptedRack::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .ok_or_else(|| {
                Error::not_found_by_id(
                    ResourceType::Rack,
                    &rack_id.into_untyped_uuid(),
                )
            })
    }

    /// List unadopted racks, ordered by ID.
    pub async fn unadopted_rack_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<UnadoptedRack> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::unadopted_rack::dsl;
        paginated(dsl::unadopted_rack, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(UnadoptedRack::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Soft-delete an unadopted rack.
    ///
    /// Fails with `ObjectNotFound` if the rack doesn't exist or was already
    /// deleted. This does not touch the rack's `unadopted_sled` rows.
    pub async fn unadopted_rack_delete(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let now = Utc::now();

        use nexus_db_schema::schema::unadopted_rack::dsl;
        let updated = diesel::update(dsl::unadopted_rack)
            .filter(dsl::id.eq(to_db_typed_uuid(rack_id)))
            .filter(dsl::time_deleted.is_null())
            .set((dsl::time_deleted.eq(now), dsl::time_modified.eq(now)))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if updated == 0 {
            return Err(Error::not_found_by_id(
                ResourceType::Rack,
                &rack_id.into_untyped_uuid(),
            ));
        }
        Ok(())
    }

    /// Insert a new unadopted sled.
    ///
    /// Fails with `ObjectAlreadyExists` if a row with this ID exists, including
    /// one that has been soft-deleted.
    pub async fn unadopted_sled_insert(
        &self,
        opctx: &OpContext,
        sled: UnadoptedSled,
    ) -> CreateResult<UnadoptedSled> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let id = sled.id().to_string();

        use nexus_db_schema::schema::unadopted_sled::dsl;
        diesel::insert_into(dsl::unadopted_sled)
            .values(sled)
            .returning(UnadoptedSled::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(ResourceType::Sled, &id),
                )
            })
    }

    /// Fetch an unadopted sled by ID.
    pub async fn unadopted_sled_get(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> LookupResult<UnadoptedSled> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::unadopted_sled::dsl;
        dsl::unadopted_sled
            .filter(dsl::id.eq(to_db_typed_uuid(sled_id)))
            .filter(dsl::time_deleted.is_null())
            .select(UnadoptedSled::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .ok_or_else(|| {
                Error::not_found_by_id(
                    ResourceType::Sled,
                    &sled_id.into_untyped_uuid(),
                )
            })
    }

    /// List unadopted sleds, ordered by ID.
    pub async fn unadopted_sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<UnadoptedSled> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::unadopted_sled::dsl;
        paginated(dsl::unadopted_sled, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(UnadoptedSled::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Soft-delete an unadopted sled.
    ///
    /// Fails with `ObjectNotFound` if the sled doesn't exist or was already
    /// deleted.
    pub async fn unadopted_sled_delete(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let now = Utc::now();

        use nexus_db_schema::schema::unadopted_sled::dsl;
        let updated = diesel::update(dsl::unadopted_sled)
            .filter(dsl::id.eq(to_db_typed_uuid(sled_id)))
            .filter(dsl::time_deleted.is_null())
            .set((dsl::time_deleted.eq(now), dsl::time_modified.eq(now)))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if updated == 0 {
            return Err(Error::not_found_by_id(
                ResourceType::Sled,
                &sled_id.into_untyped_uuid(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use std::net::Ipv6Addr;

    fn new_rack(id: RackUuid) -> UnadoptedRack {
        UnadoptedRack::new(
            id,
            "fd00:1122:3300::/56".parse().unwrap(),
            Ipv6Addr::LOCALHOST,
        )
    }

    fn new_sled(id: SledUuid, rack_id: RackUuid) -> UnadoptedSled {
        UnadoptedSled::new(
            id,
            rack_id,
            "serial".to_string(),
            "part".to_string(),
            Ipv6Addr::LOCALHOST,
        )
    }

    #[tokio::test]
    async fn test_unadopted_rack_crud() {
        let logctx = dev::test_setup_log("test_unadopted_rack_crud");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let page = DataPageParams::max_page();

        let rack1 = RackUuid::new_v4();
        let rack2 = RackUuid::new_v4();

        // Nothing there yet.
        let err = datastore.unadopted_rack_get(opctx, rack1).await.unwrap_err();
        assert!(matches!(err, Error::ObjectNotFound { .. }), "{err:?}");
        assert!(
            datastore
                .unadopted_rack_list(opctx, &page)
                .await
                .unwrap()
                .is_empty()
        );

        // Insert two, read one back, and make sure the fields round-trip.
        let inserted = datastore
            .unadopted_rack_insert(opctx, new_rack(rack1))
            .await
            .unwrap();
        datastore.unadopted_rack_insert(opctx, new_rack(rack2)).await.unwrap();
        let fetched = datastore.unadopted_rack_get(opctx, rack1).await.unwrap();
        assert_eq!(fetched.id(), rack1);
        assert_eq!(fetched.rack_subnet, inserted.rack_subnet);
        assert_eq!(fetched.join_service_ip, inserted.join_service_ip);
        assert!(fetched.time_deleted.is_none());
        let listed = datastore.unadopted_rack_list(opctx, &page).await.unwrap();
        assert_eq!(listed.len(), 2);

        // Inserting the same ID again conflicts.
        let err = datastore
            .unadopted_rack_insert(opctx, new_rack(rack1))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ObjectAlreadyExists { .. }), "{err:?}");

        // Delete is a soft delete: invisible to get/list, and not repeatable.
        datastore.unadopted_rack_delete(opctx, rack1).await.unwrap();
        let err = datastore.unadopted_rack_get(opctx, rack1).await.unwrap_err();
        assert!(matches!(err, Error::ObjectNotFound { .. }), "{err:?}");
        let listed = datastore.unadopted_rack_list(opctx, &page).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id(), rack2);
        let err =
            datastore.unadopted_rack_delete(opctx, rack1).await.unwrap_err();
        assert!(matches!(err, Error::ObjectNotFound { .. }), "{err:?}");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_unadopted_sled_crud() {
        let logctx = dev::test_setup_log("test_unadopted_sled_crud");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let page = DataPageParams::max_page();

        let rack_id = RackUuid::new_v4();
        let sled1 = SledUuid::new_v4();
        let sled2 = SledUuid::new_v4();

        let err = datastore.unadopted_sled_get(opctx, sled1).await.unwrap_err();
        assert!(matches!(err, Error::ObjectNotFound { .. }), "{err:?}");
        assert!(
            datastore
                .unadopted_sled_list(opctx, &page)
                .await
                .unwrap()
                .is_empty()
        );

        datastore
            .unadopted_sled_insert(opctx, new_sled(sled1, rack_id))
            .await
            .unwrap();
        datastore
            .unadopted_sled_insert(opctx, new_sled(sled2, rack_id))
            .await
            .unwrap();

        // Check the fields, including `rack_id` and the nullable error column.
        let fetched = datastore.unadopted_sled_get(opctx, sled1).await.unwrap();
        assert_eq!(fetched.id(), sled1);
        assert_eq!(fetched.rack_id(), rack_id);
        assert_eq!(fetched.serial_number, "serial");
        assert_eq!(fetched.part_number, "part");
        assert!(!fetched.sled_agent_started);
        assert_eq!(fetched.last_start_error_seen, None);
        assert_eq!(
            datastore.unadopted_sled_list(opctx, &page).await.unwrap().len(),
            2
        );

        let err = datastore
            .unadopted_sled_insert(opctx, new_sled(sled1, rack_id))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ObjectAlreadyExists { .. }), "{err:?}");

        datastore.unadopted_sled_delete(opctx, sled1).await.unwrap();
        let err = datastore.unadopted_sled_get(opctx, sled1).await.unwrap_err();
        assert!(matches!(err, Error::ObjectNotFound { .. }), "{err:?}");
        let listed = datastore.unadopted_sled_list(opctx, &page).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id(), sled2);
        let err =
            datastore.unadopted_sled_delete(opctx, sled1).await.unwrap_err();
        assert!(matches!(err, Error::ObjectNotFound { .. }), "{err:?}");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_unadopted_sled_start_error_constraint() {
        let logctx =
            dev::test_setup_log("test_unadopted_sled_start_error_constraint");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Build an insert with the given values for the two columns covered
        // by the constraint. The datastore API can't produce an inconsistent
        // row, so we need raw SQL to exercise the constraint.
        let insert_sql = |sled_agent_started: &str, last_start_error: &str| {
            format!(
                "INSERT INTO omicron.public.unadopted_sled \
                 (id, rack_id, time_created, time_modified, serial_number, \
                  part_number, sled_agent_ip, sled_agent_started, \
                  last_start_error_seen) \
                 VALUES (gen_random_uuid(), gen_random_uuid(), now(), now(), \
                  'serial', 'part', '::1', {sled_agent_started}, \
                  {last_start_error})"
            )
        };

        // Every combination except "started with an error" is allowed.
        for (started, error) in
            [("false", "NULL"), ("false", "'some error'"), ("true", "NULL")]
        {
            diesel::sql_query(insert_sql(started, error))
                .execute_async(&*conn)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "insert with sled_agent_started = {started}, \
                         last_start_error_seen = {error} should succeed: {e}"
                    )
                });
        }

        let err = diesel::sql_query(insert_sql("true", "'some error'"))
            .execute_async(&*conn)
            .await
            .expect_err("insert of a started sled with an error was rejected");
        match err {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::CheckViolation,
                info,
            ) => {
                assert_eq!(
                    info.constraint_name(),
                    Some("start_error_is_null_if_started"),
                );
            }
            other => {
                panic!("expected a check-constraint violation, got {other:?}")
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
