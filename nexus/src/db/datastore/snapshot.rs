// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Snapshot`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Resource;
use crate::db::model::Generation;
use crate::db::model::Name;
use crate::db::model::Snapshot;
use crate::db::model::SnapshotState;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::UpdateResult;
use omicron_common::bail_unless;
use uuid::Uuid;

impl DataStore {
    pub async fn project_create_snapshot(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        snapshot: Snapshot,
    ) -> CreateResult<Snapshot> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        use db::schema::snapshot::dsl;

        let gen = snapshot.gen;
        let snapshot: Snapshot = diesel::insert_into(dsl::snapshot)
            .values(snapshot)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(Snapshot::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Server,
                )
            })?;

        bail_unless!(
            snapshot.state == SnapshotState::Creating,
            "newly-created Snapshot has unexpected state: {:?}",
            snapshot.state
        );
        bail_unless!(
            snapshot.gen == gen,
            "newly-created Snapshot has unexpected generation: {:?}",
            snapshot.gen
        );

        Ok(snapshot)
    }

    pub async fn project_snapshot_update_state(
        &self,
        opctx: &OpContext,
        authz_snapshot: &authz::Snapshot,
        old_gen: Generation,
        new_state: SnapshotState,
    ) -> UpdateResult<Snapshot> {
        opctx.authorize(authz::Action::Modify, authz_snapshot).await?;

        use db::schema::snapshot::dsl;

        let next_gen: Generation = old_gen.next().into();

        diesel::update(dsl::snapshot)
            .filter(dsl::id.eq(authz_snapshot.id()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::gen.eq(old_gen))
            .set((dsl::state.eq(new_state), dsl::gen.eq(next_gen)))
            .returning(Snapshot::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_snapshot),
                )
            })
    }

    pub async fn project_list_snapshots(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Snapshot> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::snapshot::dsl;
        paginated(dsl::snapshot, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(authz_project.id()))
            .select(Snapshot::as_select())
            .load_async::<Snapshot>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn project_delete_snapshot(
        &self,
        opctx: &OpContext,
        authz_snapshot: &authz::Snapshot,
        db_snapshot: &Snapshot,
    ) -> Result<Uuid, Error> {
        opctx.authorize(authz::Action::Delete, authz_snapshot).await?;

        let now = Utc::now();

        // A snapshot can be deleted in any state. It's never attached to an
        // instance, and any disk launched from it will copy and modify the volume
        // construction request it's based on. The associated volume can be also
        // be deleted - this will not affect any active crucible connections
        // because no actual resources are cleaned up here.
        //
        // TODO-correctness this will leak on-disk snapshots and currently
        // running read-only downstairs, which will need to be cleaned up once
        // all volumes that reference them are gone.

        let snapshot_id = authz_snapshot.id();
        let gen = db_snapshot.gen;
        let volume_id = db_snapshot.volume_id;

        let volume_delete_query = self.volume_delete_query(volume_id);

        self.pool_authorized(&opctx)
            .await?
            .transaction(move |conn| {
                use db::schema::snapshot::dsl;

                diesel::update(dsl::snapshot)
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::gen.eq(gen))
                    .filter(dsl::id.eq(snapshot_id))
                    .set(dsl::time_deleted.eq(now))
                    .check_if_exists::<Snapshot>(snapshot_id)
                    .execute(conn)?;

                volume_delete_query.execute(conn)?;

                Ok(())
            })
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })?;

        Ok(snapshot_id)
    }
}
