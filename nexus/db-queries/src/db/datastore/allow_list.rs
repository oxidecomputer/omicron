// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Datastore methods for operating on source IP allowlists.

use crate::authz;
use crate::context::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::fixed_data::allow_list::USER_FACING_SERVICES_ALLOW_LIST_ID;
use crate::db::DbConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use nexus_db_model::schema::allow_list;
use nexus_db_model::AllowList;
use omicron_common::api::external::AllowedSourceIps;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;

impl super::DataStore {
    /// Fetch the list of allowed source IPs.
    ///
    /// This is currently effectively a singleton, since it is populated by RSS
    /// and we only provide APIs in Nexus to update it, not create / delete.
    pub async fn allow_list_view(
        &self,
        opctx: &OpContext,
    ) -> Result<AllowList, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        allow_list::dsl::allow_list
            .find(USER_FACING_SERVICES_ALLOW_LIST_ID)
            .first_async::<AllowList>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::AllowList,
                        LookupType::ById(USER_FACING_SERVICES_ALLOW_LIST_ID),
                    ),
                )
            })
    }

    /// Upsert and return a list of allowed source IPs.
    pub async fn allow_list_upsert(
        &self,
        opctx: &OpContext,
        allowed_ips: AllowedSourceIps,
    ) -> Result<AllowList, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::allow_list_upsert_on_connection(opctx, &conn, allowed_ips).await
    }

    pub(crate) async fn allow_list_upsert_on_connection(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        allowed_ips: AllowedSourceIps,
    ) -> Result<AllowList, Error> {
        use allow_list::dsl;
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let record =
            AllowList::new(USER_FACING_SERVICES_ALLOW_LIST_ID, allowed_ips);
        diesel::insert_into(dsl::allow_list)
            .values(record.clone())
            .returning(AllowList::as_returning())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::allowed_ips.eq(record.allowed_ips),
                dsl::time_modified.eq(record.time_modified),
            ))
            .get_result_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{
        datastore::test_utils::datastore_test,
        fixed_data::allow_list::USER_FACING_SERVICES_ALLOW_LIST_ID,
    };
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_allowed_source_ip_database_ops() {
        let logctx = dev::test_setup_log("test_allowed_source_ip_database_ops");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Should have the default to start with.
        let record = datastore
            .allow_list_view(&opctx)
            .await
            .expect("Expected default data populated in dbinit.sql");
        assert!(
            record.allowed_ips.is_none(),
            "Expected default ANY allowlist, represented as NULL in the DB"
        );

        // Upsert an allowlist, with some specific IPs.
        let ips =
            vec!["10.0.0.0/8".parse().unwrap(), "::1/64".parse().unwrap()];
        let allowed_ips =
            external::AllowedSourceIps::try_from(ips.as_slice()).unwrap();
        let record = datastore
            .allow_list_upsert(&opctx, allowed_ips)
            .await
            .expect("Expected this insert to succeed");
        assert_eq!(
            record.id, USER_FACING_SERVICES_ALLOW_LIST_ID,
            "Record should have hard-coded allowlist ID"
        );
        assert_eq!(
            ips,
            record.allowed_ips.unwrap(),
            "Inserted and re-read incorrect allowed source ips"
        );

        // Upsert a new list, verify it's changed.
        let new_ips =
            vec!["10.0.0.0/4".parse().unwrap(), "fd00::10/32".parse().unwrap()];
        let allowed_ips =
            external::AllowedSourceIps::try_from(new_ips.as_slice()).unwrap();
        let new_record = datastore
            .allow_list_upsert(&opctx, allowed_ips)
            .await
            .expect("Expected this insert to succeed");
        assert_eq!(
            new_record.id, USER_FACING_SERVICES_ALLOW_LIST_ID,
            "Record should have hard-coded allowlist ID"
        );
        assert_eq!(
            record.time_created, new_record.time_created,
            "Time created should not have changed"
        );
        assert!(
            record.time_modified < new_record.time_modified,
            "Time modified should have changed"
        );
        assert_eq!(
            &new_ips,
            new_record.allowed_ips.as_ref().unwrap(),
            "Updated allowed IPs are incorrect"
        );

        // Insert an allowlist letting anything in, and check it.
        let record = new_record;
        let allowed_ips = external::AllowedSourceIps::Any;
        let new_record = datastore
            .allow_list_upsert(&opctx, allowed_ips)
            .await
            .expect("Expected this insert to succeed");
        assert_eq!(
            new_record.id, USER_FACING_SERVICES_ALLOW_LIST_ID,
            "Record should have hard-coded allowlist ID"
        );
        assert_eq!(
            record.time_created, new_record.time_created,
            "Time created should not have changed"
        );
        assert!(
            record.time_modified < new_record.time_modified,
            "Time modified should have changed"
        );
        assert!(
            new_record.allowed_ips.is_none(),
            "NULL should be used in the DB to represent any allowed source IPs",
        );

        // Lastly change it back to a real list again.
        let record = new_record;
        let allowed_ips =
            external::AllowedSourceIps::try_from(new_ips.as_slice()).unwrap();
        let new_record = datastore
            .allow_list_upsert(&opctx, allowed_ips)
            .await
            .expect("Expected this insert to succeed");
        assert_eq!(
            new_record.id, USER_FACING_SERVICES_ALLOW_LIST_ID,
            "Record should have hard-coded allowlist ID"
        );
        assert_eq!(
            record.time_created, new_record.time_created,
            "Time created should not have changed"
        );
        assert!(
            record.time_modified < new_record.time_modified,
            "Time modified should have changed"
        );
        assert_eq!(
            &new_ips,
            new_record.allowed_ips.as_ref().unwrap(),
            "Updated allowed IPs are incorrect"
        );

        db.cleanup().await.expect("failed to cleanup database");
        logctx.cleanup_successful();
    }
}
