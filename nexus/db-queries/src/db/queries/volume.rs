// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper queries for working with volumes.

use crate::db;
use crate::db::pool::DbConnection;
use diesel::expression::is_aggregate;
use diesel::expression::ValidGrouping;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::Column;
use diesel::Expression;
use diesel::QueryResult;
use diesel::RunQueryDsl;
use uuid::Uuid;

/// Produces a query fragment that will act as a filter for the data modifying
/// sub-queries of the "decrease crucible resource count and soft delete volume" CTE.
///
/// The output should look like:
///
/// ```sql
/// (SELECT CASE
///   WHEN volume.resources_to_clean_up is null then true
///   ELSE false
/// END
/// FROM volume WHERE id = '{}')
/// ```
#[must_use = "Queries must be executed"]
struct ResourcesToCleanUpColumnIsNull {
    volume_id: Uuid,
}

impl ResourcesToCleanUpColumnIsNull {
    pub fn new(volume_id: Uuid) -> Self {
        Self { volume_id }
    }
}

impl QueryId for ResourcesToCleanUpColumnIsNull {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for ResourcesToCleanUpColumnIsNull {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        use db::schema::volume;

        out.push_sql("SELECT CASE WHEN ");
        out.push_identifier(volume::dsl::resources_to_clean_up::NAME)?;
        out.push_sql(" is null then true ELSE false END FROM ");
        volume::dsl::volume.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(volume::dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.volume_id)?;

        Ok(())
    }
}

impl Expression for ResourcesToCleanUpColumnIsNull {
    type SqlType = sql_types::Bool;
}

impl<GroupByClause> ValidGrouping<GroupByClause>
    for ResourcesToCleanUpColumnIsNull
{
    type IsAggregate = is_aggregate::Never;
}

/// Produces a query fragment that will conditionally reduce the volume
/// references for region_snapshot rows whose snapshot_addr column is part of a
/// list.
///
/// The output should look like:
///
/// ```sql
///  update region_snapshot set
///   volume_references = volume_references - 1,
///   deleting = case when volume_references = 1
///    then true
///    else false
///   end
///  where
///   snapshot_addr in ('a1', 'a2', 'a3') and
///   volume_references >= 1 and
///   deleting = false and
///   (<ResourcesToCleanUpColumnIsNull>)
///  returning *
/// ```
#[must_use = "Queries must be executed"]
struct ConditionallyDecreaseReferences {
    resources_to_clean_up_column_is_null_clause: ResourcesToCleanUpColumnIsNull,
    snapshot_addrs: Vec<String>,
}

impl ConditionallyDecreaseReferences {
    pub fn new(volume_id: Uuid, snapshot_addrs: Vec<String>) -> Self {
        Self {
            resources_to_clean_up_column_is_null_clause:
                ResourcesToCleanUpColumnIsNull::new(volume_id),
            snapshot_addrs,
        }
    }
}

impl QueryId for ConditionallyDecreaseReferences {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for ConditionallyDecreaseReferences {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        use db::schema::region_snapshot::dsl;

        out.push_sql("UPDATE ");
        dsl::region_snapshot.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(dsl::volume_references::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(dsl::volume_references::NAME)?;
        out.push_sql(" - 1, ");
        out.push_identifier(dsl::deleting::NAME)?;
        out.push_sql(" = CASE WHEN ");
        out.push_identifier(dsl::volume_references::NAME)?;
        out.push_sql(" = 1 THEN TRUE ELSE FALSE END WHERE ");
        out.push_identifier(dsl::snapshot_addr::NAME)?;
        out.push_sql(" IN (");

        // If self.snapshot_addrs is empty, this query fragment will intentionally not update any
        // region_snapshot rows. The rest of the CTE should still run to completion.
        for (i, snapshot_addr) in self.snapshot_addrs.iter().enumerate() {
            out.push_bind_param::<sql_types::Text, String>(snapshot_addr)?;
            if i == self.snapshot_addrs.len() - 1 {
                out.push_sql(" ");
            } else {
                out.push_sql(", ");
            }
        }

        out.push_sql(") AND ");
        out.push_identifier(dsl::volume_references::NAME)?;
        out.push_sql(" >= 1 AND ");
        out.push_identifier(dsl::deleting::NAME)?;
        out.push_sql(" = false AND ( ");
        self.resources_to_clean_up_column_is_null_clause
            .walk_ast(out.reborrow())?;
        out.push_sql(") RETURNING *");

        Ok(())
    }
}

impl Expression for ConditionallyDecreaseReferences {
    type SqlType = sql_types::Array<db::model::RegionSnapshot>;
}

impl<GroupByClause> ValidGrouping<GroupByClause>
    for ConditionallyDecreaseReferences
{
    type IsAggregate = is_aggregate::Never;
}

/// Produces a query fragment that will find all resources that can be cleaned
/// up as a result of a volume delete, and build a serialized JSON struct that
/// can be deserialized into a CrucibleResources::V3 variant. The output of this
/// will be written into the 'resources_to_clean_up` column of the volume being
/// soft-deleted.
///
/// The output should look like:
///
/// ```sql
/// json_build_object('V3',
///       json_build_object(
///         'regions', (select json_agg(id) from region join t2 on region.id = t2.region_id where (t2.volume_references = 0 or t2.volume_references is null) and region.volume_id = '<volume_id>'),
///         'region_snapshots', (select json_agg(json_build_object('dataset', dataset_id, 'region', region_id, 'snapshot', snapshot_id)) from t2 where t2.volume_references = 0)
///       )
///     )
/// ```
///
/// Note if json_agg is executing over zero rows, then the output is `null`, not
/// `[]`. For example, if the sub-query meant to return regions to clean up
/// returned zero rows, the output of json_build_object would look like:
///
/// ```json
/// {
///   "V3": {
///     "regions": null,
///     ...
///   }
/// }
/// ```
///
/// Correctly handling `null` here is done in the deserializer for
/// CrucibleResourcesV3.
///
/// A populated object should look like:
///
/// ```json
/// {
///   "V3": {
///     "regions": [
///       "9caae5bb-a212-4496-882a-af1ee242c62f",
///       "713c84ee-6b13-4301-b7a2-36debc7ee37e"
///     ],
///     "region_snapshots": [
///       {
///         "dataset": "33ec5f07-5e7f-481e-966a-0fbc50d9ed3b",
///         "region": "1e2b1a75-9a58-4e5c-89a0-0cfd19ba055a",
///         "snapshot": "f7c8ed87-a67e-4d2b-8f35-3e8034de1c6f"
///       },
///       {
///         "dataset": "5a16b1d6-7381-4c51-b49c-997624d43ead",
///         "region": "52b4c9bc-d1c9-4a3b-87c3-8e4501a883b0",
///         "snapshot": "2dd912e4-74db-409a-8d55-9795496cb320"
///       }
///     ]
///   }
/// }
/// ```
#[must_use = "Queries must be executed"]
struct BuildJsonResourcesToCleanUp {
    table: &'static str,
    volume_id: Uuid,
}

impl BuildJsonResourcesToCleanUp {
    pub fn new(table: &'static str, volume_id: Uuid) -> Self {
        Self { table, volume_id }
    }
}

impl QueryId for BuildJsonResourcesToCleanUp {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for BuildJsonResourcesToCleanUp {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        use db::schema::region::dsl as region_dsl;
        use db::schema::region_snapshot::dsl as region_snapshot_dsl;
        use db::schema::volume::dsl;

        out.push_sql("json_build_object('V3', ");
        out.push_sql("json_build_object('regions', ");
        out.push_sql("(SELECT json_agg(");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(") FROM ");
        region_dsl::region.walk_ast(out.reborrow())?;
        out.push_sql(" JOIN ");
        out.push_sql(self.table);
        out.push_sql(" ON ");
        out.push_identifier(region_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_sql(self.table);
        out.push_sql(".");
        out.push_identifier(region_snapshot_dsl::region_id::NAME)?; // table's schema is equivalent to region_snapshot
        out.push_sql(" WHERE ( ");

        out.push_sql(self.table);
        out.push_sql(".");
        out.push_identifier(region_snapshot_dsl::volume_references::NAME)?;
        out.push_sql(" = 0 OR ");
        out.push_sql(self.table);
        out.push_sql(".");
        out.push_identifier(region_snapshot_dsl::volume_references::NAME)?;
        out.push_sql(" IS NULL");

        out.push_sql(") AND ");
        out.push_identifier(region_dsl::volume_id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.volume_id)?;

        out.push_sql("), 'region_snapshots', (");
        out.push_sql("SELECT json_agg(json_build_object(");
        out.push_sql("'dataset', ");
        out.push_identifier(region_snapshot_dsl::dataset_id::NAME)?;
        out.push_sql(", 'region', ");
        out.push_identifier(region_snapshot_dsl::region_id::NAME)?;
        out.push_sql(", 'snapshot', ");
        out.push_identifier(region_snapshot_dsl::snapshot_id::NAME)?;
        out.push_sql(")) from ");
        out.push_sql(self.table);
        out.push_sql(" where ");
        out.push_sql(self.table);
        out.push_sql(".");
        out.push_identifier(region_snapshot_dsl::volume_references::NAME)?;
        out.push_sql(" = 0)))");

        Ok(())
    }
}

impl<GroupByClause> ValidGrouping<GroupByClause>
    for BuildJsonResourcesToCleanUp
{
    type IsAggregate = is_aggregate::Never;
}

/// Produces a query fragment that will set the `resources_to_clean_up` column
/// of the volume being deleted if it is not set already.
///
/// The output should look like:
///
/// ```sql
///   update volume set
///    time_deleted = now(),
///    resources_to_clean_up = ( select <BuildJsonResourcesToCleanUp> )
///    where id = '<volume_id>' and
///    (<ResourcesToCleanUpColumnIsNull>)
///    returning volume.*
/// ```
#[must_use = "Queries must be executed"]
struct ConditionallyUpdateVolume {
    resources_to_clean_up_column_is_null_clause: ResourcesToCleanUpColumnIsNull,
    build_json_resources_to_clean_up_query: BuildJsonResourcesToCleanUp,
    volume_id: Uuid,
}

impl ConditionallyUpdateVolume {
    pub fn new(volume_id: Uuid, table: &'static str) -> Self {
        Self {
            resources_to_clean_up_column_is_null_clause:
                ResourcesToCleanUpColumnIsNull::new(volume_id),
            build_json_resources_to_clean_up_query:
                BuildJsonResourcesToCleanUp::new(table, volume_id),
            volume_id,
        }
    }
}

impl QueryId for ConditionallyUpdateVolume {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for ConditionallyUpdateVolume {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        use db::schema::volume::dsl;

        out.push_sql("UPDATE ");
        dsl::volume.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" = now(), ");
        out.push_identifier(dsl::resources_to_clean_up::NAME)?;
        out.push_sql(" = (SELECT ");

        self.build_json_resources_to_clean_up_query.walk_ast(out.reborrow())?;

        out.push_sql(") WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.volume_id)?;
        out.push_sql(" AND (");

        self.resources_to_clean_up_column_is_null_clause
            .walk_ast(out.reborrow())?;

        out.push_sql(") RETURNING volume.*");

        Ok(())
    }
}

impl Expression for ConditionallyUpdateVolume {
    type SqlType = diesel::sql_types::Array<db::model::Volume>;
}

impl<GroupByClause> ValidGrouping<GroupByClause> for ConditionallyUpdateVolume {
    type IsAggregate = is_aggregate::Never;
}

/// Produces a query fragment that will
///
/// 1. decrease the number of references for each region snapshot that
///    a volume references
/// 2. soft-delete the volume
/// 3. record the resources to clean up as a serialized CrucibleResources
///    struct in volume's `resources_to_clean_up` column.
///
/// The output should look like:
///
/// ```sql
///  with UPDATED_REGION_SNAPSHOTS_TABLE as (
///    UPDATE region_snapshot <ConditionallyDecreaseReferences>
///  ),
///  REGION_SNAPSHOTS_TO_CLEAN_UP_TABLE as (
///    select * from UPDATED_REGION_SNAPSHOTS_TABLE where deleting = true and volume_references = 0
///  ),
///  UPDATED_VOLUME_TABLE as (
///    UPDATE volume <ConditionallyUpdateVolume>
///  )
///  select case
///   when volume.resources_to_clean_up is not null then volume.resources_to_clean_up
///   else (select resources_to_clean_up from UPDATED_VOLUME_TABLE where id = '<volume id>')
///  end
///  from volume where id = '<volume id>';
/// ```
#[must_use = "Queries must be executed"]
pub struct DecreaseCrucibleResourceCountAndSoftDeleteVolume {
    conditionally_decrease_references: ConditionallyDecreaseReferences,
    conditionally_update_volume_query: ConditionallyUpdateVolume,
    volume_id: Uuid,
}

impl DecreaseCrucibleResourceCountAndSoftDeleteVolume {
    const UPDATED_REGION_SNAPSHOTS_TABLE: &'static str =
        "updated_region_snapshots";
    const REGION_SNAPSHOTS_TO_CLEAN_UP_TABLE: &'static str =
        "region_snapshots_to_clean_up";
    const UPDATED_VOLUME_TABLE: &'static str = "updated_volume";

    pub fn new(volume_id: Uuid, snapshot_addrs: Vec<String>) -> Self {
        Self {
            conditionally_decrease_references:
                ConditionallyDecreaseReferences::new(volume_id, snapshot_addrs),
            conditionally_update_volume_query: ConditionallyUpdateVolume::new(
                volume_id,
                Self::REGION_SNAPSHOTS_TO_CLEAN_UP_TABLE,
            ),
            volume_id,
        }
    }
}

impl QueryId for DecreaseCrucibleResourceCountAndSoftDeleteVolume {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for DecreaseCrucibleResourceCountAndSoftDeleteVolume {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        use db::schema::region_snapshot::dsl as rs_dsl;
        use db::schema::volume::dsl;

        out.push_sql("WITH ");
        out.push_sql(Self::UPDATED_REGION_SNAPSHOTS_TABLE);
        out.push_sql(" as (");
        self.conditionally_decrease_references.walk_ast(out.reborrow())?;
        out.push_sql("), ");

        out.push_sql(Self::REGION_SNAPSHOTS_TO_CLEAN_UP_TABLE);
        out.push_sql(" AS (SELECT * FROM ");
        out.push_sql(Self::UPDATED_REGION_SNAPSHOTS_TABLE);
        out.push_sql(" WHERE ");
        out.push_identifier(rs_dsl::deleting::NAME)?;
        out.push_sql(" = TRUE AND ");
        out.push_identifier(rs_dsl::volume_references::NAME)?;
        out.push_sql(" = 0), ");

        out.push_sql(Self::UPDATED_VOLUME_TABLE);
        out.push_sql(" AS (");
        self.conditionally_update_volume_query.walk_ast(out.reborrow())?;
        out.push_sql(") ");

        out.push_sql("SELECT ");
        dsl::volume.walk_ast(out.reborrow())?;
        out.push_sql(".* FROM ");
        dsl::volume.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.volume_id)?;

        Ok(())
    }
}

impl Expression for DecreaseCrucibleResourceCountAndSoftDeleteVolume {
    type SqlType = diesel::sql_types::Array<db::model::Volume>;
}

impl<GroupByClause> ValidGrouping<GroupByClause>
    for DecreaseCrucibleResourceCountAndSoftDeleteVolume
{
    type IsAggregate = is_aggregate::Never;
}

impl RunQueryDsl<DbConnection>
    for DecreaseCrucibleResourceCountAndSoftDeleteVolume
{
}

type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;

impl Query for DecreaseCrucibleResourceCountAndSoftDeleteVolume {
    type SqlType = SelectableSql<db::model::Volume>;
}
