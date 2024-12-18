use super::DataStore;
use crate::authz;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::model::AuditLogCompletion;
use crate::db::model::AuditLogEntry;
use crate::db::model::AuditLogEntryInit;
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

        use db::schema::audit_log_complete;
        let query = paginated_multicolumn(
            audit_log_complete::table,
            (audit_log_complete::time_completed, audit_log_complete::id),
            pagparams,
        )
        .filter(audit_log_complete::time_completed.ge(start_time));
        // TODO: confirm and document exclusive/inclusive behavior
        let query = if let Some(end) = end_time {
            query.filter(audit_log_complete::time_completed.lt(end))
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
        entry: AuditLogEntryInit,
    ) -> CreateResult<AuditLogEntryInit> {
        use db::schema::audit_log;
        opctx.authorize(authz::Action::CreateChild, &authz::AUDIT_LOG).await?;

        let entry_id = entry.id.to_string();

        diesel::insert_into(audit_log::table)
            .values(entry)
            .returning(AuditLogEntryInit::as_returning())
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
        entry: &AuditLogEntryInit,
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
        let t0 = Utc::now();
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

        let entry1 = AuditLogEntryInit::new(
            "req-1".to_string(),
            "project_create".to_string(),
            "https://omicron.com/projects".to_string(),
            "1.1.1.1".parse().unwrap(),
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
            .audit_log_entry_init(opctx, entry1.clone())
            .await
            .expect_err("inserting same entry again should error");
        assert_matches!(conflict, Error::ObjectAlreadyExists { .. });

        let t1 = Utc::now();

        let completion = AuditLogCompletion::new(201);
        datastore
            .audit_log_entry_complete(opctx, &entry1, completion)
            .await
            .expect("complete audit log entry");

        let t2 = Utc::now();

        let entry2 = AuditLogEntryInit::new(
            "req-2".to_string(),
            "project_delete".to_string(),
            "https://omicron.com/projects/123".to_string(),
            "1.1.1.1".parse().unwrap(),
            None,
            None,
            None,
        );
        let entry2 = datastore
            .audit_log_entry_init(opctx, entry2.clone())
            .await
            .expect("init second audit log entry");

        let t3 = Utc::now();

        // before entry2 is completed, it doesn't come back in the list
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t0, None)
            .await
            .expect("retrieve audit log");
        assert_eq!(audit_log.len(), 1);
        assert_eq!(audit_log[0].request_id, "req-1");

        // now complete entry2
        let completion = AuditLogCompletion::new(204);
        datastore
            .audit_log_entry_complete(opctx, &entry2.clone(), completion)
            .await
            .expect("complete audit log entry");

        let t4 = Utc::now();

        // get both entries
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t0, None)
            .await
            .expect("retrieve audit log");
        assert_eq!(audit_log.len(), 2);
        assert_eq!(audit_log[0].request_id, "req-1");
        assert_eq!(audit_log[0].http_status_code.0, 201);
        assert_eq!(audit_log[1].request_id, "req-2");
        assert_eq!(audit_log[1].http_status_code.0, 204);

        // Only get first entry
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t1, Some(t2))
            .await
            .expect("retrieve first audit log entry");
        assert_eq!(audit_log.len(), 1);
        assert_eq!(audit_log[0].request_id, "req-1");
        assert!(
            audit_log[0].time_completed > t1
                && audit_log[0].time_completed < t2
        );

        // Only get second entry
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams, t2, None)
            .await
            .expect("retrieve second audit log entry");
        assert_eq!(audit_log.len(), 1);
        assert_eq!(audit_log[0].request_id, "req-2");
        assert!(
            audit_log[0].time_completed > t3
                && audit_log[0].time_completed < t4
        );

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

        let base = AuditLogEntryInit::new(
            "req-1".to_string(),
            "project_create".to_string(),
            "https://omicron.com/projects".to_string(),
            "1.1.1.1".parse().unwrap(),
            None,
            None,
            None,
        );
        let completion = AuditLogCompletion::new(201);

        let id1 = "1710a22e-b29b-4cfc-9e79-e8c93be187d7";
        let id2 = "5d25e766-e026-44b4-8b42-5f90f43c26bc";
        let id3 = "a156ad37-047e-4028-88bd-8034906d5a27";
        let id4 = "d0d59e4f-4c98-4df5-b3c5-39fc0c2ac547";

        // funky order so we can feel really good about the sort order being correct
        for id in [id4, id1, id3, id2] {
            let entry =
                AuditLogEntryInit { id: id.parse().unwrap(), ..base.clone() };
            let entry = datastore
                .audit_log_entry_init(opctx, entry)
                .await
                .expect("init entry");
            // they all get the same completion time
            datastore
                .audit_log_entry_complete(opctx, &entry, completion.clone())
                .await
                .expect("complete entry");
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

        // now check descending order
        let pagparams_desc = DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        };
        let audit_log = datastore
            .audit_log_list(opctx, &pagparams_desc, t0, None)
            .await
            .expect("retrieve audit log");
        assert_eq!(audit_log.len(), 4);
        assert_eq!(audit_log[0].id.to_string(), id4);
        assert_eq!(audit_log[1].id.to_string(), id3);
        assert_eq!(audit_log[2].id.to_string(), id2);
        assert_eq!(audit_log[3].id.to_string(), id1);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
