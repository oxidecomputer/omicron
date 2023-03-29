// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Sled`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Sled;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::OptionalLookupResult;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Stores a new sled in the database.
    pub async fn sled_upsert(&self, sled: Sled) -> CreateResult<Sled> {
        use db::schema::sled::dsl;
        diesel::insert_into(dsl::sled)
            .values(sled.clone())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::ip.eq(sled.ip),
                dsl::port.eq(sled.port),
                dsl::rack_id.eq(sled.rack_id),
                dsl::is_scrimlet.eq(sled.is_scrimlet()),
                dsl::usable_hardware_threads.eq(sled.usable_hardware_threads),
                dsl::usable_physical_ram.eq(sled.usable_physical_ram),
                dsl::reservoir_size.eq(sled.reservoir_size),
            ))
            .returning(Sled::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Sled,
                        &sled.id().to_string(),
                    ),
                )
            })
    }

    pub async fn sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Sled> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::sled::dsl;
        paginated(dsl::sled, dsl::id, pagparams)
            .select(Sled::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn random_sled(
        &self,
        opctx: &OpContext,
    ) -> OptionalLookupResult<Sled> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::sled::dsl;

        sql_function!(fn random() -> diesel::sql_types::Float);
        Ok(dsl::sled
            .filter(dsl::time_deleted.is_null())
            .order(random())
            .limit(1)
            .select(Sled::as_select())
            .load_async::<Sled>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?
            .pop())
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
        let mut sled = Sled::new(
            sled_id,
            addr,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id(),
        );
        let observed_sled = datastore
            .sled_upsert(sled.clone())
            .await
            .expect("Could not upsert sled during test prep");
        assert_eq!(
            observed_sled.usable_hardware_threads,
            sled.usable_hardware_threads
        );
        assert_eq!(observed_sled.usable_physical_ram, sled.usable_physical_ram);
        assert_eq!(observed_sled.reservoir_size, sled.reservoir_size);

        // Modify the sizes of hardware
        sled.usable_hardware_threads =
            SqlU32::new(sled.usable_hardware_threads.0 + 1);
        const MIB: u64 = 1024 * 1024;
        sled.usable_physical_ram = ByteCount::from(
            external::ByteCount::try_from(
                sled.usable_physical_ram.0.to_bytes() + MIB,
            )
            .unwrap(),
        );
        sled.reservoir_size = ByteCount::from(
            external::ByteCount::try_from(
                sled.reservoir_size.0.to_bytes() + MIB,
            )
            .unwrap(),
        );

        // Test that upserting the sled propagates those changes to the DB.
        let observed_sled = datastore
            .sled_upsert(sled.clone())
            .await
            .expect("Could not upsert sled during test prep");
        assert_eq!(
            observed_sled.usable_hardware_threads,
            sled.usable_hardware_threads
        );
        assert_eq!(observed_sled.usable_physical_ram, sled.usable_physical_ram);
        assert_eq!(observed_sled.reservoir_size, sled.reservoir_size);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
