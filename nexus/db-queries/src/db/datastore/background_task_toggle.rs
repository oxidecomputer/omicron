use crate::db::error::{public_error_from_diesel, ErrorHandler};
use crate::{
    context::OpContext,
    db::model::background_task_toggles::{
        BackgroundTaskToggle, BackgroundTaskToggleValues,
    },
};
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::{prelude::*, ExpressionMethods, SelectableHelper};
use omicron_common::api::external::{CreateResult, Error, LookupResult};

use super::DataStore;

impl DataStore {
    pub async fn set_background_task_toggle(
        &self,
        opctx: &OpContext,
        values: BackgroundTaskToggleValues,
    ) -> CreateResult<()> {
        use crate::db::schema::background_task_toggles::dsl;

        diesel::insert_into(dsl::background_task_toggles)
            .values(values.clone())
            .on_conflict(dsl::name)
            .do_update()
            .set(dsl::enabled.eq(values.enabled))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    pub async fn get_background_task_toggle(
        &self,
        opctx: &OpContext,
        name: String,
    ) -> LookupResult<BackgroundTaskToggle> {
        use crate::db::schema::background_task_toggles::dsl;

        let result = dsl::background_task_toggles
            .filter(dsl::name.eq(name))
            .select(BackgroundTaskToggle::as_select())
            .limit(1)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if let Some(nat_entry) = result.first() {
            Ok(nat_entry.clone())
        } else {
            Err(Error::invalid_request("no matching records"))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::datastore::test_utils::datastore_test;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;

    // Test our ability to track additions and deletions since a given version number
    #[tokio::test]
    async fn set_and_get_toggles() {
        let logctx = dev::test_setup_log("test_set_and_get_toggles");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // no toggle should be set for "my_task"
        assert!(datastore
            .get_background_task_toggle(&opctx, "my_task".into())
            .await
            .is_err());

        assert!(datastore
            .set_background_task_toggle(
                &opctx,
                super::BackgroundTaskToggleValues {
                    name: "my_task".into(),
                    enabled: true
                }
            )
            .await
            .is_ok());

        assert!(datastore
            .get_background_task_toggle(&opctx, "my_task".into())
            .await
            .is_ok());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
