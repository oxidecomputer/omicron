use super::DataStore;
use crate::authz;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::model::AuditLogCompletion;
use crate::db::model::AuditLogEntry;
use crate::db::pagination::paginated_multicolumn;
use crate::{context::OpContext, db::error::ErrorHandler};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::{CreateResult, UpdateResult};
use uuid::Uuid;

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
impl DataStore {
    pub async fn audit_log_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
        start_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> ListResultVec<db::model::AuditLogEntry> {
        opctx.authorize(authz::Action::ListChildren, &authz::AUDIT_LOG).await?;

        use db::schema::audit_log;
        let query = paginated_multicolumn(
            audit_log::table,
            (audit_log::timestamp, audit_log::id),
            pagparams,
        )
        .filter(audit_log::timestamp.ge(start_time));
        // TODO: confirm and document exclusive/inclusive behavior
        let query = if let Some(end) = end_time {
            query.filter(audit_log::timestamp.lt(end))
        } else {
            query
        };
        query
            .select(AuditLogEntry::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn audit_log_entry_init(
        &self,
        opctx: &OpContext,
        entry: AuditLogEntry,
    ) -> CreateResult<AuditLogEntry> {
        use db::schema::audit_log;
        opctx.authorize(authz::Action::CreateChild, &authz::AUDIT_LOG).await?;

        let entry_id = entry.id.to_string();

        diesel::insert_into(audit_log::table)
            .values(entry)
            .returning(AuditLogEntry::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::AuditLogEntry,
                        &entry_id,
                    ),
                )
            })
    }

    // set duration and result on an existing entry
    pub async fn audit_log_entry_complete(
        &self,
        opctx: &OpContext,
        entry: &AuditLogEntry,
        completion: AuditLogCompletion,
    ) -> UpdateResult<()> {
        use db::schema::audit_log;
        opctx.authorize(authz::Action::CreateChild, &authz::AUDIT_LOG).await?;
        diesel::update(audit_log::table)
            .filter(audit_log::id.eq(entry.id))
            .set(completion)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(()) // TODO: make sure we don't want to return something else
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use assert_matches::assert_matches;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;

    #[tokio::test]
    async fn test_audit_log_basic() {
        let logctx = dev::test_setup_log("test_audit_log");
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pagparams = DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let t0: DateTime<Utc> = Utc::now();
        let t_future: DateTime<Utc> = "2099-01-01T00:00:00Z".parse().unwrap();

        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t0, None)
            .await
            .expect("retrieve empty audit log");
        assert_eq!(audit_log.len(), 0);

        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t_future, None)
            .await
            .expect("retrieve empty audit log");
        assert_eq!(audit_log.len(), 0);

        let entry1 = AuditLogEntry::new(
            "req-1".to_string(),
            "project_create".to_string(),
            "https://omicron.com/projects".to_string(),
            "1.1.1.1".to_string(),
            None,
            None,
            None,
        );
        datastore
            .audit_log_entry_init(opctx, entry1.clone())
            .await
            .expect("init audit log entry");

        // inserting the same entry again blows up
        let conflict = datastore
            .audit_log_entry_init(opctx, entry1)
            .await
            .expect_err("inserting same entry again should error");
        assert_matches!(conflict, Error::ObjectAlreadyExists { .. });

        let t1 = Utc::now();

        let entry2 = AuditLogEntry::new(
            "req-2".to_string(),
            "project_delete".to_string(),
            "https://omicron.com/projects/123".to_string(),
            "1.1.1.1".to_string(),
            None,
            None,
            None,
        );
        datastore
            .audit_log_entry_init(opctx, entry2.clone())
            .await
            .expect("init second audit log entry");

        // get both entries
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t0, None)
            .await
            .expect("retrieve audit log");
        assert_eq!(audit_log.len(), 2);
        assert_eq!(audit_log[0].request_id, "req-1");
        assert_eq!(audit_log[1].request_id, "req-2");

        // Only get first entry
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t0, Some(t1))
            .await
            .expect("retrieve first audit log entry");
        assert_eq!(audit_log.len(), 1);
        assert_eq!(audit_log[0].request_id, "req-1");

        // Only get second entry
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t1, None)
            .await
            .expect("retrieve second audit log entry");
        assert_eq!(audit_log.len(), 1);
        assert_eq!(audit_log[0].request_id, "req-2");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // timestamps are not unique, so we use record ID as a tiebreaker to ensure
    // stable ordering. Here we create 4 records with the same timestamp but
    // (necessarily) different IDs and confirm that they come out sorted by ID.
    #[tokio::test]
    async fn test_audit_log_id_as_order_tiebreaker() {
        let logctx =
            dev::test_setup_log("test_audit_log_id_as_order_tiebreaker");
        let log = &logctx.log;
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let t0 = Utc::now();

        let base = AuditLogEntry::new(
            "req-1".to_string(),
            "project_create".to_string(),
            "https://omicron.com/projects".to_string(),
            "1.1.1.1".to_string(),
            None,
            None,
            None,
        );

        let id1 = "1710a22e-b29b-4cfc-9e79-e8c93be187d7";
        let id2 = "5d25e766-e026-44b4-8b42-5f90f43c26bc";
        let id3 = "a156ad37-047e-4028-88bd-8034906d5a27";
        let id4 = "d0d59e4f-4c98-4df5-b3c5-39fc0c2ac547";

        // funky order so we can feel really good about the sort order being correct
        for id in [id4, id1, id3, id2] {
            let entry =
                AuditLogEntry { id: id.parse().unwrap(), ..base.clone() };
            datastore
                .audit_log_entry_init(opctx, entry)
                .await
                .expect("init entry");
        }

        let pagparams = DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };

        // retrieve both and check the order -- the one with the lower ID
        // should always be first
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t0, None)
            .await
            .expect("retrieve audit log");
        assert_eq!(audit_log.len(), 4);
        assert_eq!(audit_log[0].id.to_string(), id1);
        assert_eq!(audit_log[1].id.to_string(), id2);
        assert_eq!(audit_log[2].id.to_string(), id3);
        assert_eq!(audit_log[3].id.to_string(), id4);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
