// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implement a query for updating an instance and VMM in a single CTE.

use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::QueryResult;
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::result::Error as DieselError;
use diesel::sql_types::{Nullable, Uuid as SqlUuid};
use diesel::{pg::Pg, query_builder::AstPass};
use diesel::{Column, ExpressionMethods, QueryDsl, RunQueryDsl};
use nexus_db_model::{
    schema::{
        instance::dsl as instance_dsl, migration::dsl as migration_dsl,
        vmm::dsl as vmm_dsl,
    },
    Generation, InstanceRuntimeState, MigrationState, VmmRuntimeState,
};
use omicron_common::api::internal::nexus::{
    MigrationRole, MigrationRuntimeState,
};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, PropolisUuid};
use uuid::Uuid;

use crate::db::pool::DbConnection;
use crate::db::update_and_check::UpdateStatus;

/// A CTE that checks and updates the instance and VMM tables in a single
/// atomic operation.
//
// The single-table update-and-check CTE has the following form:
//
// WITH found   AS (SELECT <primary key> FROM T WHERE <primary key = value>)
//      updated AS (UPDATE T SET <values> RETURNING *)
// SELECT
//      found.<primary key>
//      updated.<primary key>
//      found.*
// FROM
//      found
// LEFT JOIN
//      updated
// ON
//      found.<primary_key> = updated.<primary_key>;
//
// The idea behind this query is to have separate "found" and "updated"
// subqueries for the instance and VMM tables, then use those to create two more
// subqueries that perform the joins and yield the results, along the following
// lines:
//
// WITH vmm_found AS (SELECT(SELECT id FROM vmm WHERE vmm.id = id) AS id),
//      vmm_updated AS (UPDATE vmm SET ... RETURNING *),
//      instance_found AS (SELECT(
//          SELECT id FROM instance WHERE instance.id = id
//      ) AS id),
//      instance_updated AS (UPDATE instance SET ... RETURNING *),
//      vmm_result AS (
//          SELECT vmm_found.id AS found, vmm_updated.id AS updated
//          FROM vmm_found
//          LEFT JOIN vmm_updated
//          ON vmm_found.id = vmm_updated.id
//      ),
//      instance_result AS (
//          SELECT instance_found.id AS found, instance_updated.id AS updated
//          FROM instance_found
//          LEFT JOIN instance_updated
//          ON instance_found.id = instance_updated.id
//      )
// SELECT vmm_result.found, vmm_result.updated, instance_result.found,
//        instance_result.updated
// FROM vmm_result, instance_result;
///
/// If a [`MigrationRuntimeState`] is provided, similar "found" and "update"
/// clauses  are also added to join the `migration` record for the instance's
/// active migration, if one exists, and update the migration record. If no
/// migration record is provided, this part of the query is skipped, and the
/// `migration_found` and `migration_updated` portions are always `false`.
//
// The "wrapper" SELECTs when finding instances and VMMs are used to get a NULL
// result in the final output instead of failing the entire query if the target
// object is missing. This maximizes Nexus's flexibility when dealing with
// updates from sled agent that refer to one valid and one deleted object. (This
// can happen if, e.g., sled agent sends a message indicating that a retired VMM
// has finally been destroyed when its instance has since been deleted.)
pub struct InstanceAndVmmUpdate {
    instance_find: Box<dyn QueryFragment<Pg> + Send>,
    vmm_find: Box<dyn QueryFragment<Pg> + Send>,
    instance_update: Box<dyn QueryFragment<Pg> + Send>,
    vmm_update: Box<dyn QueryFragment<Pg> + Send>,
    migration: Option<MigrationUpdate>,
}

struct MigrationUpdate {
    find: Box<dyn QueryFragment<Pg> + Send>,
    update: Box<dyn QueryFragment<Pg> + Send>,
}

/// Contains the result of a combined instance-and-VMM update operation.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct InstanceAndVmmUpdateResult {
    /// `Some(status)` if the target instance was found; the wrapped
    /// `UpdateStatus` indicates whether the row was updated. `None` if the
    /// instance was not found.
    pub instance_status: Option<UpdateStatus>,

    /// `Some(status)` if the target VMM was found; the wrapped `UpdateStatus`
    /// indicates whether the row was updated. `None` if the VMM was not found.
    pub vmm_status: Option<UpdateStatus>,

    /// `Some(status)` if the target migration was found; the wrapped `UpdateStatus`
    /// indicates whether the row was updated. `None` if the migration was not
    /// found, or no migration update was performed.
    pub migration_status: Option<UpdateStatus>,
}

/// Computes the update status to return from the results of queries that find
/// and update an object with an ID of type `T`.
fn compute_update_status<T>(
    found: Option<T>,
    updated: Option<T>,
) -> Option<UpdateStatus>
where
    T: PartialEq + std::fmt::Display,
{
    match (found, updated) {
        // If both the "find" and "update" prongs returned an ID, the row was
        // updated. The IDs should match in this case (if they don't then the
        // query was constructed very strangely!).
        (Some(found_id), Some(updated_id)) if found_id == updated_id => {
            Some(UpdateStatus::Updated)
        }
        // If the "find" prong returned an ID but the "update" prong didn't, the
        // row exists but wasn't updated.
        (Some(_), None) => Some(UpdateStatus::NotUpdatedButExists),
        // If neither prong returned anything, indicate the row is missing.
        (None, None) => None,
        // If both prongs returned an ID, but they don't match, something
        // terrible has happened--the prongs must have referred to different
        // IDs!
        (Some(found_id), Some(mismatched_id)) => unreachable!(
            "updated ID {} didn't match found ID {}",
            mismatched_id, found_id
        ),
        // Similarly, if the target ID was not found but something was updated
        // anyway, then something is wrong with the update query--either it has
        // the wrong ID or did not filter rows properly.
        (None, Some(updated_id)) => unreachable!(
            "ID {} was updated but no found ID was supplied",
            updated_id
        ),
    }
}

impl InstanceAndVmmUpdate {
    pub fn new(
        instance_id: InstanceUuid,
        new_instance_runtime_state: InstanceRuntimeState,
        vmm_id: PropolisUuid,
        new_vmm_runtime_state: VmmRuntimeState,
        migration: Option<MigrationRuntimeState>,
    ) -> Self {
        let instance_find = Box::new(
            instance_dsl::instance
                .filter(instance_dsl::id.eq(instance_id.into_untyped_uuid()))
                .select(instance_dsl::id),
        );

        let vmm_find = Box::new(
            vmm_dsl::vmm
                .filter(vmm_dsl::id.eq(vmm_id.into_untyped_uuid()))
                .select(vmm_dsl::id),
        );

        let instance_update = Box::new(
            diesel::update(instance_dsl::instance)
                .filter(instance_dsl::time_deleted.is_null())
                .filter(instance_dsl::id.eq(instance_id.into_untyped_uuid()))
                .filter(
                    instance_dsl::state_generation
                        .lt(new_instance_runtime_state.gen),
                )
                .set(new_instance_runtime_state),
        );

        let vmm_update = Box::new(
            diesel::update(vmm_dsl::vmm)
                .filter(vmm_dsl::time_deleted.is_null())
                .filter(vmm_dsl::id.eq(vmm_id.into_untyped_uuid()))
                .filter(vmm_dsl::state_generation.lt(new_vmm_runtime_state.gen))
                .set(new_vmm_runtime_state),
        );

        let migration = migration.map(
            |MigrationRuntimeState {
                 role,
                 migration_id,
                 state,
                 gen,
                 time_updated,
             }| {
                let state = MigrationState::from(state);
                let find = Box::new(
                    migration_dsl::migration
                        .filter(migration_dsl::id.eq(migration_id))
                        .filter(migration_dsl::time_deleted.is_null())
                        .select(migration_dsl::id),
                );
                let gen = Generation::from(gen);
                let update: Box<dyn QueryFragment<Pg> + Send> = match role {
                    MigrationRole::Target => Box::new(
                        diesel::update(migration_dsl::migration)
                            .filter(migration_dsl::id.eq(migration_id))
                            .filter(
                                migration_dsl::target_propolis_id
                                    .eq(vmm_id.into_untyped_uuid()),
                            )
                            .filter(migration_dsl::target_gen.lt(gen))
                            .set((
                                migration_dsl::target_state.eq(state),
                                migration_dsl::time_target_updated
                                    .eq(time_updated),
                            )),
                    ),
                    MigrationRole::Source => Box::new(
                        diesel::update(migration_dsl::migration)
                            .filter(migration_dsl::id.eq(migration_id))
                            .filter(
                                migration_dsl::source_propolis_id
                                    .eq(vmm_id.into_untyped_uuid()),
                            )
                            .filter(migration_dsl::source_gen.lt(gen))
                            .set((
                                migration_dsl::source_state.eq(state),
                                migration_dsl::time_source_updated
                                    .eq(time_updated),
                            )),
                    ),
                };
                MigrationUpdate { find, update }
            },
        );

        Self { instance_find, vmm_find, instance_update, vmm_update, migration }
    }

    pub async fn execute_and_check(
        self,
        conn: &(impl async_bb8_diesel::AsyncConnection<DbConnection> + Sync),
    ) -> Result<InstanceAndVmmUpdateResult, DieselError> {
        let (
            vmm_found,
            vmm_updated,
            instance_found,
            instance_updated,
            migration_found,
            migration_updated,
        ) = self
            .get_result_async::<(
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
            )>(conn)
            .await?;

        let instance_status =
            compute_update_status(instance_found, instance_updated);
        let vmm_status = compute_update_status(vmm_found, vmm_updated);
        let migration_status =
            compute_update_status(migration_found, migration_updated);

        Ok(InstanceAndVmmUpdateResult {
            instance_status,
            vmm_status,
            migration_status,
        })
    }
}

impl QueryId for InstanceAndVmmUpdate {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Query for InstanceAndVmmUpdate {
    type SqlType = (
        Nullable<SqlUuid>,
        Nullable<SqlUuid>,
        Nullable<SqlUuid>,
        Nullable<SqlUuid>,
        Nullable<SqlUuid>,
        Nullable<SqlUuid>,
    );
}

impl RunQueryDsl<DbConnection> for InstanceAndVmmUpdate {}

impl QueryFragment<Pg> for InstanceAndVmmUpdate {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("WITH instance_found AS (SELECT (");
        self.instance_find.walk_ast(out.reborrow())?;
        out.push_sql(") AS id), ");

        out.push_sql("vmm_found AS (SELECT (");
        self.vmm_find.walk_ast(out.reborrow())?;
        out.push_sql(") AS id), ");

        if let Some(MigrationUpdate { ref find, .. }) = self.migration {
            out.push_sql("migration_found AS (SELECT (");
            find.walk_ast(out.reborrow())?;
            out.push_sql(") AS id), ");
        }

        out.push_sql("instance_updated AS (");
        self.instance_update.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING id), ");

        out.push_sql("vmm_updated AS (");
        self.vmm_update.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING id), ");

        if let Some(MigrationUpdate { ref update, .. }) = self.migration {
            out.push_sql("migration_updated AS (");
            update.walk_ast(out.reborrow())?;
            out.push_sql(" RETURNING id), ");
        }

        out.push_sql("vmm_result AS (");
        out.push_sql("SELECT vmm_found.");
        out.push_identifier(vmm_dsl::id::NAME)?;
        out.push_sql(" AS found, vmm_updated.");
        out.push_identifier(vmm_dsl::id::NAME)?;
        out.push_sql(" AS updated");
        out.push_sql(" FROM vmm_found LEFT JOIN vmm_updated ON vmm_found.");
        out.push_identifier(vmm_dsl::id::NAME)?;
        out.push_sql(" = vmm_updated.");
        out.push_identifier(vmm_dsl::id::NAME)?;
        out.push_sql("), ");

        out.push_sql("instance_result AS (");
        out.push_sql("SELECT instance_found.");
        out.push_identifier(instance_dsl::id::NAME)?;
        out.push_sql(" AS found, instance_updated.");
        out.push_identifier(instance_dsl::id::NAME)?;
        out.push_sql(" AS updated");
        out.push_sql(
            " FROM instance_found LEFT JOIN instance_updated ON instance_found.",
        );
        out.push_identifier(instance_dsl::id::NAME)?;
        out.push_sql(" = instance_updated.");
        out.push_identifier(instance_dsl::id::NAME)?;
        out.push_sql(")");

        if self.migration.is_some() {
            out.push_sql(", ");
            out.push_sql("migration_result AS (");
            out.push_sql("SELECT migration_found.");
            out.push_identifier(migration_dsl::id::NAME)?;
            out.push_sql(" AS found, migration_updated.");
            out.push_identifier(migration_dsl::id::NAME)?;
            out.push_sql(" AS updated");
            out.push_sql(
                " FROM migration_found LEFT JOIN migration_updated ON migration_found.",
            );
            out.push_identifier(migration_dsl::id::NAME)?;
            out.push_sql(" = migration_updated.");
            out.push_identifier(migration_dsl::id::NAME)?;
            out.push_sql(")");
        }
        out.push_sql(" ");

        out.push_sql("SELECT vmm_result.found, vmm_result.updated, ");
        out.push_sql("instance_result.found, instance_result.updated, ");
        if self.migration.is_some() {
            out.push_sql("migration_result.found, migration_result.updated ");
        } else {
            out.push_sql("NULL, NULL ");
        }
        out.push_sql("FROM vmm_result, instance_result");
        if self.migration.is_some() {
            out.push_sql(", migration_result");
        }

        Ok(())
    }
}
