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
use omicron_common::api::internal::nexus::{MigrationRuntimeState, Migrations};
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
    vmm_find: Box<dyn QueryFragment<Pg> + Send>,
    vmm_update: Box<dyn QueryFragment<Pg> + Send>,
    instance: Option<Update>,
    migration_in: Option<Update>,
    migration_out: Option<Update>,
}

struct Update {
    name: &'static str,
    id: &'static str,
    find: Box<dyn QueryFragment<Pg> + Send>,
    update: Box<dyn QueryFragment<Pg> + Send>,
}

/// Contains the result of a combined instance-and-VMM update operation.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct InstanceAndVmmUpdateResult {
    /// `Some(status)` if the target instance was found; the wrapped
    /// `UpdateStatus` indicates whether the row was updated. `None` if the
    /// instance was not found.
    pub instance_status: RecordUpdateStatus,

    /// `Some(status)` if the target VMM was found; the wrapped `UpdateStatus`
    /// indicates whether the row was updated. `None` if the VMM was not found.
    pub vmm_status: Option<UpdateStatus>,

    /// Indicates whether a migration-in update was performed.
    pub migration_in_status: RecordUpdateStatus,

    /// Indicates whether a migration-out update was performed.
    pub migration_out_status: RecordUpdateStatus,
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum RecordUpdateStatus {
    /// No record was found for the provided ID.
    NotFound,
    /// No record for this table was provided as part of the update.
    NotProvided,
    /// An update for this record was provided, and a a record matching the
    /// provided ID exists.
    Found(UpdateStatus),
}

impl RecordUpdateStatus {
    pub fn was_updated(self) -> bool {
        matches!(self, Self::Found(UpdateStatus::Updated))
    }
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
        vmm_id: PropolisUuid,
        new_vmm_runtime_state: VmmRuntimeState,
        instance: Option<(InstanceUuid, InstanceRuntimeState)>,
        Migrations { migration_in, migration_out }: Migrations<'_>,
    ) -> Self {
        let vmm_find = Box::new(
            vmm_dsl::vmm
                .filter(vmm_dsl::id.eq(vmm_id.into_untyped_uuid()))
                .select(vmm_dsl::id),
        );

        let vmm_update = Box::new(
            diesel::update(vmm_dsl::vmm)
                .filter(vmm_dsl::time_deleted.is_null())
                .filter(vmm_dsl::id.eq(vmm_id.into_untyped_uuid()))
                .filter(vmm_dsl::state_generation.lt(new_vmm_runtime_state.gen))
                .set(new_vmm_runtime_state),
        );

        let instance = instance.map(|(instance_id, new_runtime_state)| {
            let instance_id = instance_id.into_untyped_uuid();
            let find = Box::new(
                instance_dsl::instance
                    .filter(instance_dsl::id.eq(instance_id))
                    .select(instance_dsl::id),
            );

            let update = Box::new(
                diesel::update(instance_dsl::instance)
                    .filter(instance_dsl::time_deleted.is_null())
                    .filter(instance_dsl::id.eq(instance_id))
                    .filter(
                        instance_dsl::state_generation
                            .lt(new_runtime_state.gen),
                    )
                    .set(new_runtime_state),
            );
            Update {
                find,
                update,
                name: "instance",
                id: instance_dsl::id::NAME,
            }
        });

        fn migration_find(
            migration_id: Uuid,
        ) -> Box<dyn QueryFragment<Pg> + Send> {
            Box::new(
                migration_dsl::migration
                    .filter(migration_dsl::id.eq(migration_id))
                    .filter(migration_dsl::time_deleted.is_null())
                    .select(migration_dsl::id),
            )
        }

        let migration_in = migration_in.cloned().map(
            |MigrationRuntimeState {
                 migration_id,
                 state,
                 gen,
                 time_updated,
             }| {
                let state = MigrationState::from(state);
                let gen = Generation::from(gen);
                let update = Box::new(
                    diesel::update(migration_dsl::migration)
                        .filter(migration_dsl::id.eq(migration_id))
                        .filter(
                            migration_dsl::target_propolis_id
                                .eq(vmm_id.into_untyped_uuid()),
                        )
                        .filter(migration_dsl::target_gen.lt(gen))
                        .set((
                            migration_dsl::target_state.eq(state),
                            migration_dsl::time_target_updated.eq(time_updated),
                        )),
                );
                Update {
                    find: migration_find(migration_id),
                    update,
                    name: "migration_in",
                    id: migration_dsl::id::NAME,
                }
            },
        );

        let migration_out = migration_out.cloned().map(
            |MigrationRuntimeState {
                 migration_id,
                 state,
                 gen,
                 time_updated,
             }| {
                let state = MigrationState::from(state);
                let gen = Generation::from(gen);
                let update = Box::new(
                    diesel::update(migration_dsl::migration)
                        .filter(migration_dsl::id.eq(migration_id))
                        .filter(
                            migration_dsl::source_propolis_id
                                .eq(vmm_id.into_untyped_uuid()),
                        )
                        .filter(migration_dsl::source_gen.lt(gen))
                        .set((
                            migration_dsl::source_state.eq(state),
                            migration_dsl::time_source_updated.eq(time_updated),
                        )),
                );
                Update {
                    find: migration_find(migration_id),
                    update,
                    name: "migration_out",
                    id: migration_dsl::id::NAME,
                }
            },
        );

        Self { vmm_find, vmm_update, instance, migration_in, migration_out }
    }

    pub async fn execute_and_check(
        self,
        conn: &(impl async_bb8_diesel::AsyncConnection<DbConnection> + Sync),
    ) -> Result<InstanceAndVmmUpdateResult, DieselError> {
        let has_migration_in = self.migration_in.is_some();
        let has_migration_out = self.migration_out.is_some();
        let has_instance = self.instance.is_some();
        let (
            vmm_found,
            vmm_updated,
            instance_found,
            instance_updated,
            migration_in_found,
            migration_in_updated,
            migration_out_found,
            migration_out_updated,
        ) = self
            .get_result_async::<(
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                Option<Uuid>,
                // WHEW!
            )>(conn)
            .await?;

        let vmm_status = compute_update_status(vmm_found, vmm_updated);

        let instance_status = if has_instance {
            compute_update_status(instance_found, instance_updated)
                .map(RecordUpdateStatus::Found)
                .unwrap_or(RecordUpdateStatus::NotFound)
        } else {
            RecordUpdateStatus::NotProvided
        };

        let migration_in_status = if has_migration_in {
            compute_update_status(migration_in_found, migration_in_updated)
                .map(RecordUpdateStatus::Found)
                .unwrap_or(RecordUpdateStatus::NotFound)
        } else {
            RecordUpdateStatus::NotProvided
        };

        let migration_out_status = if has_migration_out {
            compute_update_status(migration_out_found, migration_out_updated)
                .map(RecordUpdateStatus::Found)
                .unwrap_or(RecordUpdateStatus::NotFound)
        } else {
            RecordUpdateStatus::NotProvided
        };

        Ok(InstanceAndVmmUpdateResult {
            instance_status,
            vmm_status,
            migration_in_status,
            migration_out_status,
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
        Nullable<SqlUuid>,
        Nullable<SqlUuid>,
    );
}

impl RunQueryDsl<DbConnection> for InstanceAndVmmUpdate {}

impl Update {
    fn push_subqueries<'b>(
        &'b self,
        out: &mut AstPass<'_, 'b, Pg>,
    ) -> QueryResult<()> {
        out.push_sql(self.name);
        out.push_sql("_found AS (SELECT (");
        self.find.walk_ast(out.reborrow())?;
        out.push_sql(") AS ID), ");
        out.push_sql(self.name);
        out.push_sql("_updated AS (");
        self.update.walk_ast(out.reborrow())?;
        out.push_sql("RETURNING id), ");
        out.push_sql(self.name);
        out.push_sql("_result AS (SELECT ");
        out.push_sql(self.name);
        out.push_sql("_found.");
        out.push_identifier(self.id)?;
        out.push_sql(" AS found, ");
        out.push_sql(self.name);
        out.push_sql("_updated.");
        out.push_identifier(self.id)?;
        out.push_sql(" AS updated");
        out.push_sql(" FROM ");
        out.push_sql(self.name);
        out.push_sql("_found LEFT JOIN ");
        out.push_sql(self.name);
        out.push_sql("_updated ON ");
        out.push_sql(self.name);
        out.push_sql("_found.");
        out.push_identifier(self.id)?;
        out.push_sql("= ");
        out.push_sql(self.name);
        out.push_sql("_updated.");
        out.push_identifier(self.id)?;
        out.push_sql(")");

        Ok(())
    }
}

impl QueryFragment<Pg> for InstanceAndVmmUpdate {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("WITH ");
        if let Some(ref instance) = self.instance {
            instance.push_subqueries(&mut out)?;
            out.push_sql(", ");
        }

        if let Some(ref m) = self.migration_in {
            m.push_subqueries(&mut out)?;
            out.push_sql(", ");
        }

        if let Some(ref m) = self.migration_out {
            m.push_subqueries(&mut out)?;
            out.push_sql(", ");
        }

        out.push_sql("vmm_found AS (SELECT (");
        self.vmm_find.walk_ast(out.reborrow())?;
        out.push_sql(") AS id), ");

        out.push_sql("vmm_updated AS (");
        self.vmm_update.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING id), ");
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
        out.push_sql(") ");

        fn push_select_from_result(
            update: Option<&Update>,
            out: &mut AstPass<'_, '_, Pg>,
        ) {
            if let Some(update) = update {
                out.push_sql(update.name);
                out.push_sql("_result.found, ");
                out.push_sql(update.name);
                out.push_sql("_result.updated");
            } else {
                out.push_sql("NULL, NULL")
            }
        }

        out.push_sql("SELECT vmm_result.found, vmm_result.updated, ");
        push_select_from_result(self.instance.as_ref(), &mut out);
        out.push_sql(", ");
        push_select_from_result(self.migration_in.as_ref(), &mut out);
        out.push_sql(", ");
        push_select_from_result(self.migration_out.as_ref(), &mut out);
        out.push_sql(" ");

        out.push_sql("FROM vmm_result");
        if self.instance.is_some() {
            out.push_sql(", instance_result");
        }
        if self.migration_in.is_some() {
            out.push_sql(", migration_in_result");
        }

        if self.migration_out.is_some() {
            out.push_sql(", migration_out_result");
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::model::Generation;
    use crate::db::model::VmmState;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use chrono::Utc;
    use omicron_common::api::internal::nexus::MigrationRuntimeState;
    use omicron_common::api::internal::nexus::MigrationState;
    use uuid::Uuid;

    // These tests are a bit of a "change detector", but they're here to help
    // with debugging too. If you change this query, it can be useful to see
    // exactly how the output SQL has been altered.

    fn mk_vmm_state() -> VmmRuntimeState {
        VmmRuntimeState {
            time_state_updated: Utc::now(),
            gen: Generation::new(),
            state: VmmState::Starting,
        }
    }

    fn mk_migration_state() -> MigrationRuntimeState {
        let migration_id = Uuid::nil();
        MigrationRuntimeState {
            migration_id,
            state: MigrationState::Pending,
            gen: Generation::new().into(),
            time_updated: Utc::now(),
        }
    }

    fn mk_instance_state() -> (InstanceUuid, InstanceRuntimeState) {
        let id = InstanceUuid::nil();
        let state = InstanceRuntimeState {
            time_updated: Utc::now(),
            gen: Generation::new(),
            propolis_id: Some(Uuid::nil()),
            dst_propolis_id: Some(Uuid::nil()),
            migration_id: Some(Uuid::nil()),
            nexus_state: nexus_db_model::InstanceState::Vmm,
        };
        (id, state)
    }

    #[tokio::test]
    async fn expectorate_query_only_vmm() {
        let vmm_id = PropolisUuid::nil();
        let vmm_state = mk_vmm_state();

        let query = InstanceAndVmmUpdate::new(
            vmm_id,
            vmm_state,
            None,
            Migrations::default(),
        );
        expectorate_query_contents(
            &query,
            "tests/output/instance_and_vmm_update_vmm_only.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_vmm_and_instance() {
        let vmm_id = PropolisUuid::nil();
        let vmm_state = mk_vmm_state();
        let instance = mk_instance_state();

        let query = InstanceAndVmmUpdate::new(
            vmm_id,
            vmm_state,
            Some(instance),
            Migrations::default(),
        );
        expectorate_query_contents(
            &query,
            "tests/output/instance_and_vmm_update_vmm_and_instance.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_vmm_and_migration_in() {
        let vmm_id = PropolisUuid::nil();
        let vmm_state = mk_vmm_state();
        let migration = mk_migration_state();

        let query = InstanceAndVmmUpdate::new(
            vmm_id,
            vmm_state,
            None,
            Migrations { migration_in: Some(&migration), migration_out: None },
        );
        expectorate_query_contents(
            &query,
            "tests/output/instance_and_vmm_update_vmm_and_migration_in.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_vmm_instance_and_migration_in() {
        let vmm_id = PropolisUuid::nil();
        let vmm_state = mk_vmm_state();
        let instance = mk_instance_state();
        let migration = mk_migration_state();

        let query = InstanceAndVmmUpdate::new(
            vmm_id,
            vmm_state,
            Some(instance),
            Migrations { migration_in: Some(&migration), migration_out: None },
        );
        expectorate_query_contents(
            &query,
            "tests/output/instance_and_vmm_update_vmm_instance_and_migration_in.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_vmm_and_migration_out() {
        let vmm_id = PropolisUuid::nil();
        let vmm_state = mk_vmm_state();
        let migration = mk_migration_state();

        let query = InstanceAndVmmUpdate::new(
            vmm_id,
            vmm_state,
            None,
            Migrations { migration_out: Some(&migration), migration_in: None },
        );
        expectorate_query_contents(
            &query,
            "tests/output/instance_and_vmm_update_vmm_and_migration_out.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_vmm_instance_and_migration_out() {
        let vmm_id = PropolisUuid::nil();
        let vmm_state = mk_vmm_state();
        let instance = mk_instance_state();
        let migration = mk_migration_state();

        let query = InstanceAndVmmUpdate::new(
            vmm_id,
            vmm_state,
            Some(instance),
            Migrations { migration_out: Some(&migration), migration_in: None },
        );
        expectorate_query_contents(
            &query,
            "tests/output/instance_and_vmm_update_vmm_instance_and_migration_out.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_vmm_and_both_migrations() {
        let vmm_id = PropolisUuid::nil();
        let vmm_state = mk_vmm_state();
        let migration_in = mk_migration_state();
        let migration_out = mk_migration_state();

        let query = InstanceAndVmmUpdate::new(
            vmm_id,
            vmm_state,
            None,
            Migrations {
                migration_in: Some(&migration_in),
                migration_out: Some(&migration_out),
            },
        );
        expectorate_query_contents(
            &query,
            "tests/output/instance_and_vmm_update_vmm_and_both_migrations.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_vmm_instance_and_both_migrations() {
        let vmm_id = PropolisUuid::nil();
        let vmm_state = mk_vmm_state();
        let instance = mk_instance_state();
        let migration_in = mk_migration_state();
        let migration_out = mk_migration_state();

        let query = InstanceAndVmmUpdate::new(
            vmm_id,
            vmm_state,
            Some(instance),
            Migrations {
                migration_in: Some(&migration_in),
                migration_out: Some(&migration_out),
            },
        );
        expectorate_query_contents(
            &query,
            "tests/output/instance_and_vmm_update_vmm_instance_and_both_migrations.sql",
        )
        .await;
    }
}
