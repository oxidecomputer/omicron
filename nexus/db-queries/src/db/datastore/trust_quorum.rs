// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum related queries

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::HwBaseboardId;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use std::collections::BTreeSet;

impl DataStore {
    /// Return all `HwBaseboardId`s for a given rack that has run LRTQ
    ///
    /// No need for pagination, as there at most 32 member sleds per rack
    pub async fn lrtq_members(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> ListResultVec<HwBaseboardId> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::hw_baseboard_id::dsl as hw_baseboard_id_dsl;
        use nexus_db_schema::schema::lrtq_member::dsl as lrtq_member_dsl;

        lrtq_member_dsl::lrtq_member
            .filter(lrtq_member_dsl::rack_id.eq(rack_id.into_untyped_uuid()))
            .inner_join(hw_baseboard_id_dsl::hw_baseboard_id.on(
                hw_baseboard_id_dsl::id.eq(lrtq_member_dsl::hw_baseboard_id),
            ))
            .select(HwBaseboardId::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::{HwBaseboardId, LrtqMember};
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::RackUuid;
    use uuid::Uuid;

    async fn insert_hw_baseboard_ids(db: &TestDatabase) -> Vec<Uuid> {
        let (_, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::hw_baseboard_id::dsl;
        let mut uuids = Vec::new();
        for i in 0..10 {
            let uuid = Uuid::new_v4();
            uuids.push(uuid);
            diesel::insert_into(dsl::hw_baseboard_id)
                .values(HwBaseboardId {
                    id: uuid,
                    part_number: "test-part".to_string(),
                    serial_number: i.to_string(),
                })
                .execute_async(&*conn)
                .await
                .unwrap();
        }
        uuids
    }

    async fn insert_lrtq_members(
        db: &TestDatabase,
        rack_id1: RackUuid,
        rack_id2: RackUuid,
        hw_ids: Vec<Uuid>,
    ) {
        let (_, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::lrtq_member::dsl;
        for (i, &hw_baseboard_id) in hw_ids.iter().enumerate() {
            let rack_id = if i < 5 { rack_id1.into() } else { rack_id2.into() };
            diesel::insert_into(dsl::lrtq_member)
                .values(LrtqMember { rack_id, hw_baseboard_id })
                .execute_async(&*conn)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_lrtq_members() {
        let logctx = test_setup_log("test_lrtq_members");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let rack_id1 = RackUuid::new_v4();
        let rack_id2 = RackUuid::new_v4();

        // Listing lrtq members should return an empty vec
        assert!(
            datastore.lrtq_members(opctx, rack_id1).await.unwrap().is_empty()
        );

        // Insert some data
        let hw_ids = insert_hw_baseboard_ids(&db).await;
        insert_lrtq_members(&db, rack_id1, rack_id2, hw_ids.clone()).await;

        let hw_baseboard_ids1 =
            datastore.lrtq_members(opctx, rack_id1).await.unwrap();
        println!("{:?}", hw_baseboard_ids1);
        assert_eq!(hw_baseboard_ids1.len(), 5);
        let hw_baseboard_ids2 =
            datastore.lrtq_members(opctx, rack_id2).await.unwrap();
        assert_eq!(hw_baseboard_ids2.len(), 5);
        assert_ne!(hw_baseboard_ids1, hw_baseboard_ids2);
    }
}
