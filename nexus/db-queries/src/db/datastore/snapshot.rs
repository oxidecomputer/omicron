// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Snapshot`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Generation;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::Snapshot;
use crate::db::model::SnapshotState;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::OptionalExtension;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::bail_unless;
use ref_cast::RefCast;
use uuid::Uuid;

impl DataStore {
    pub async fn project_ensure_snapshot(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        snapshot: Snapshot,
    ) -> CreateResult<Snapshot> {
        let gen = snapshot.gen;
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        #[derive(Debug, thiserror::Error)]
        pub enum CustomError {
            #[error("Resource already exists")]
            ResourceAlreadyExists,

            #[error("saw AsyncInsertError")]
            InsertError(AsyncInsertError),
        }

        type TxnError = TransactionError<CustomError>;

        let snapshot_name = snapshot.name().to_string();
        let project_id = snapshot.project_id;

        let snapshot: Snapshot = self
            .pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::snapshot::dsl;

                // If an undeleted snapshot exists in the database with the
                // same name and project but a different id to the snapshot
                // this function was passed as an argument, then return an
                // error here.
                //
                // As written below,
                //
                //    .on_conflict((dsl::project_id, dsl::name))
                //    .filter_target(dsl::time_deleted.is_null())
                //    .do_update()
                //    .set(dsl::time_modified.eq(dsl::time_modified))
                //
                // will set any existing record's `time_modified` if the
                // project id and name match, even if the snapshot ID does
                // not match. diesel supports adding a filter below like so
                // (marked with >>):
                //
                //    .on_conflict((dsl::project_id, dsl::name))
                //    .filter_target(dsl::time_deleted.is_null())
                //    .do_update()
                //    .set(dsl::time_modified.eq(dsl::time_modified))
                // >> .filter(dsl::id.eq(snapshot.id()))
                //
                // which will restrict the `insert_into`'s set so that it
                // only applies if the snapshot ID matches. But,
                // AsyncInsertError does not have a ObjectAlreadyExists
                // variant, so this will be returned as CollectionNotFound
                // due to the `insert_into` failing.
                //
                // If this function is passed a snapshot with an ID that
                // does not match, but a project and name that does, return
                // ObjectAlreadyExists here.

                let existing_snapshot_id: Option<Uuid> = dsl::snapshot
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::name.eq(snapshot.name().to_string()))
                    .filter(dsl::project_id.eq(snapshot.project_id))
                    .select(dsl::id)
                    .limit(1)
                    .first_async(&conn)
                    .await
                    .optional()?;

                if let Some(existing_snapshot_id) = existing_snapshot_id {
                    if existing_snapshot_id != snapshot.id() {
                        return Err(TransactionError::CustomError(
                            CustomError::ResourceAlreadyExists,
                        ));
                    }
                }

                Project::insert_resource(
                    project_id,
                    diesel::insert_into(dsl::snapshot)
                        .values(snapshot)
                        .on_conflict((dsl::project_id, dsl::name))
                        .filter_target(dsl::time_deleted.is_null())
                        .do_update()
                        .set(dsl::time_modified.eq(dsl::time_modified)),
                )
                .insert_and_get_result_async(&conn)
                .await
                .map_err(|e| {
                    TransactionError::CustomError(CustomError::InsertError(e))
                })
            })
            .await
            .map_err(|e: TxnError| match e {
                TxnError::CustomError(e) => match e {
                    CustomError::ResourceAlreadyExists => {
                        Error::ObjectAlreadyExists {
                            type_name: ResourceType::Snapshot,
                            object_name: snapshot_name,
                        }
                    }
                    CustomError::InsertError(e) => match e {
                        AsyncInsertError::CollectionNotFound => {
                            Error::ObjectNotFound {
                                type_name: ResourceType::Project,
                                lookup_type: LookupType::ById(project_id),
                            }
                        }
                        AsyncInsertError::DatabaseError(e) => {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        }
                    },
                },
                TxnError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
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
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_snapshot),
                )
            })
    }

    pub async fn snapshot_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Snapshot> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::snapshot::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::snapshot, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::snapshot,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::project_id.eq(authz_project.id()))
        .select(Snapshot::as_select())
        .load_async::<Snapshot>(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn project_delete_snapshot(
        &self,
        opctx: &OpContext,
        authz_snapshot: &authz::Snapshot,
        db_snapshot: &Snapshot,
        ok_to_delete_states: Vec<SnapshotState>,
    ) -> Result<Uuid, Error> {
        opctx.authorize(authz::Action::Delete, authz_snapshot).await?;

        let now = Utc::now();

        // A snapshot can be deleted in states Ready and Faulted. It's never
        // attached to an instance, and any disk launched from it will copy and
        // modify the volume construction request it's based on. However, if its
        // state is Creating then the snapshot_create saga is currently running
        // and this delete action would disrupt that. If its in state Destroyed,
        // then it was already deleted.

        let snapshot_id = authz_snapshot.id();
        let gen = db_snapshot.gen;

        use db::schema::snapshot::dsl;

        let updated_rows = diesel::update(dsl::snapshot)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::gen.eq(gen))
            .filter(dsl::id.eq(snapshot_id))
            .filter(dsl::state.eq_any(ok_to_delete_states))
            .set((
                dsl::time_deleted.eq(now),
                dsl::state.eq(SnapshotState::Destroyed),
            ))
            .check_if_exists::<Snapshot>(snapshot_id)
            .execute_async(&*self.pool_connection_authorized(&opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if updated_rows == 0 {
            // Either:
            //
            // - the snapshot was already deleted
            // - the generation number changed
            // - the state of the snapshot isn't one of `ok_to_delete_states`

            return Err(Error::invalid_request("snapshot cannot be deleted"));
        }

        Ok(snapshot_id)
    }
}
