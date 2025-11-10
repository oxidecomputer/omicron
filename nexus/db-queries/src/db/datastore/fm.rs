// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on fault management internal data, such as situation
//! reports (sitreps).
//!
//! See [RFD 603](https://rfd.shared.oxide.computer/rfd/0603) for details on the
//! fault management sitrep.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::model;
use crate::db::model::SqlU32;
use crate::db::pagination::paginated;
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::TypedSqlQuery;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use dropshot::PaginationOrder;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::fm_sitrep::dsl as sitrep_dsl;
use nexus_db_schema::schema::fm_sitrep_history::dsl as history_dsl;
use nexus_types::fm;
use nexus_types::fm::Sitrep;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SitrepUuid;
use uuid::Uuid;

impl DataStore {
    /// Reads the current [sitrep version](fm::SitrepVersion) from CRDB.
    ///
    /// If no sitreps have been generated, this returns `None`.
    pub async fn fm_current_sitrep_version(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<fm::SitrepVersion>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let version = self
            .fm_current_sitrep_version_on_conn(&conn)
            .await?
            .map(Into::into);
        Ok(version)
    }

    async fn fm_current_sitrep_version_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<model::SitrepVersion>, Error> {
        history_dsl::fm_sitrep_history
            .order_by(history_dsl::version.desc())
            .select(model::SitrepVersion::as_select())
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Reads the [`fm::SitrepMetadata`] describing the sitrep with the given
    /// ID, if one exists.
    pub async fn fm_sitrep_metadata_read(
        &self,
        opctx: &OpContext,
        id: SitrepUuid,
    ) -> Result<fm::SitrepMetadata, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let meta =
            self.fm_sitrep_metadata_read_on_conn(id, &conn).await?.into();
        Ok(meta)
    }

    async fn fm_sitrep_metadata_read_on_conn(
        &self,
        id: SitrepUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<model::SitrepMetadata, Error> {
        sitrep_dsl::fm_sitrep
            .filter(sitrep_dsl::id.eq(id.into_untyped_uuid()))
            .select(model::SitrepMetadata::as_select())
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .ok_or_else(|| {
                Error::non_resourcetype_not_found(format!("sitrep {id:?}"))
            })
    }

    /// Reads the *entire* current sitrep, along with its version.
    ///
    /// This is equivalent to reading the current sitrep version using
    /// [`DataStore::fm_current_sitrep_version`], and then reading the sitrep
    /// itself using [`DataStore::fm_sitrep_read`].
    ///
    /// If this method returns `None`, there is no current sitrep, meaning that
    /// no sitreps have been created.
    pub async fn fm_sitrep_read_current(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<(fm::SitrepVersion, Sitrep)>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let version: fm::SitrepVersion =
            match self.fm_current_sitrep_version_on_conn(&conn).await? {
                Some(version) => version.into(),
                None => return Ok(None),
            };
        let sitrep = self.fm_sitrep_read_on_conn(version.id, &conn).await?;
        Ok(Some((version, sitrep)))
    }

    /// Reads the entire content of the sitrep with the provided ID, if one exists.
    pub async fn fm_sitrep_read(
        &self,
        opctx: &OpContext,
        id: SitrepUuid,
    ) -> Result<Sitrep, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.fm_sitrep_read_on_conn(id, &conn).await
    }

    async fn fm_sitrep_read_on_conn(
        &self,
        id: SitrepUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Sitrep, Error> {
        let metadata =
            self.fm_sitrep_metadata_read_on_conn(id, &conn).await?.into();

        // TODO(eliza): this is where we would read all the other sitrep data,
        // if there was any.

        Ok(Sitrep { metadata })
    }

    /// Insert the provided [`Sitrep`] into the database, and attempt to mark it
    /// as the current sitrep.
    ///
    /// If the sitrep's parent is not the current sitrep, the new sitrep is not
    /// added to the sitrep history, and an error is returned. See [this
    /// section](https://rfd.shared.oxide.computer/rfd/0603#_creating_sitreps)
    /// in RFD 603 for details.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the new sitrep was both successfully inserted *and* added
    ///   to the sitrep history as the current sitrep.
    ///
    /// - `Err(`[`InsertSitrepError::ParentNotCurrent`]`)` if the sitrep's
    ///    `parent_sitrep_id` is not the current sitrep, indicating that it was
    ///    generated based on out of date inputs.
    ///
    ///    This error indicates that the sitrep is orphaned and should be
    ///    deleted. It is out of date, and another sitrep has already been
    ///    generated based on the same inputs.
    ///
    /// - `Err(`[`InsertSitrepError::Other`]`)` if another error occurred while
    ///    inserting the sitrep.
    pub async fn fm_sitrep_insert(
        &self,
        opctx: &OpContext,
        sitrep: &Sitrep,
    ) -> Result<(), InsertSitrepError> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO(eliza): there should probably be an authz object for the fm sitrep?
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // Create the sitrep metadata record.
        diesel::insert_into(sitrep_dsl::fm_sitrep)
            .values(model::SitrepMetadata::from(sitrep.metadata.clone()))
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to insert sitrep metadata record")
            })?;

        // TODO(eliza): other sitrep records would be inserted here...

        // Now, try to make the sitrep current.
        let query = InsertSitrepVersionQuery { sitrep_id: sitrep.id() };
        query
            .execute_async(&*conn)
            .await
            .map_err(|e| query.decode_error(e))
            .map(|_| ())
    }

    /// Lists all orphaned alternative sitreps for the provided
    /// [`fm::SitrepVersion`].
    ///
    /// Orphaned sitreps at a given version are those which descend from the
    /// same parent sitrep as the current sitrep at that version, but which were
    /// not committed successfully to the sitrep history.
    ///
    /// Note that this operation is only performed relative to a committed
    /// sitrep version. This is in order to prevent the returned list of sitreps
    /// from including sitreps which are in the process of being prepared, by
    /// only performing it on committed versions and selecting sitreps with the
    /// same parent.
    pub async fn fm_sitrep_list_orphaned(
        &self,
        opctx: &OpContext,
        pagparams: DataPageParams<'_, SitrepUuid>,
    ) -> ListResultVec<SitrepUuid> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO(eliza): there should probably be an authz object for the fm sitrep?
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let list = Self::list_orphaned_query(&pagparams)
            .load_async::<Uuid>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .into_iter()
            .map(|id| SitrepUuid::from_untyped_uuid(id))
            .collect();
        Ok(list)
    }

    fn list_orphaned_query(
        pagparams: &DataPageParams<'_, SitrepUuid>,
    ) -> TypedSqlQuery<sql_types::Uuid> {
        let mut builder = QueryBuilder::new();
        builder.sql(
            "WITH current_sitrep_id AS ( \
                SELECT sitrep_id \
                FROM omicron.public.fm_sitrep_history \
                ORDER BY version DESC \
                LIMIT 1 \
            ),",
        );

        // batch AS (
        //     SELECT s.id, s.parent_sitrep_id
        //     FROM omicron.public.fm_sitrep s
        //     WHERE s.id > $1
        //     ORDER BY s.id
        //     LIMIT $2
        // )
        let (dir, cmp) = match pagparams.direction {
            PaginationOrder::Ascending => ("ASC", "> "),
            PaginationOrder::Descending => ("DESC", "< "),
        };
        builder.sql(
            "batch AS ( \
                SELECT s.id, s.parent_sitrep_id \
                FROM omicron.public.fm_sitrep s ",
        );
        if let Some(marker) = pagparams.marker {
            builder
                .sql("WHERE s.id ")
                .sql(cmp)
                .param()
                .bind::<sql_types::Uuid, _>(marker.into_untyped_uuid());
        }
        builder
            .sql(" ORDER BY s.id ")
            .sql(dir)
            .sql(" LIMIT ")
            .param()
            .bind::<sql_types::Int8, _>(pagparams.limit.get() as i64)
            .sql(") ");

        builder.sql(
            "SELECT id \
            FROM omicron.public.fm_sitrep \
            WHERE id IN ( \
                SELECT b.id \
                FROM batch b \
                LEFT JOIN omicron.public.fm_sitrep_history h \
                    ON h.sitrep_id = b.id \
                WHERE \
                    h.sitrep_id IS NULL \
                AND ( \
                    (b.parent_sitrep_id IS NULL \
                     AND current_sitrep_id.sitrep_id IS NOT NULL)
                    OR b.parent_sitrep_id != ( \
                        SELECT sitrep_id FROM current_sitrep_id \
                    ) \
                ) \
            );",
        );
        builder.query()
    }

    /// Deletes all sitreps with the provided IDs.
    pub async fn fm_sitrep_delete_all(
        &self,
        opctx: &OpContext,
        ids: impl IntoIterator<Item = SitrepUuid>,
    ) -> Result<usize, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO(eliza): there should probably be an authz object for the fm sitrep?
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let ids = ids
            .into_iter()
            .map(|id| id.into_untyped_uuid())
            .collect::<Vec<_>>();

        // TODO(eliza): when other tables are added to store data that is part
        // of the sitrep, we'll need to delete any records with matching IDs in
        // those tables, too!

        // Delete the sitrep metadata entries *last*. This is necessary because
        // the rest of the delete operation is unsynchronized, and it is
        // possible for a Nexus to die before it has "fully deleted" a sitrep,
        // but deleted some of its records. The `fm_sitrep` (metadata) table is
        // the one that is used to determine whether a sitrep "exists" so that
        // the sitrep GC task can determine if it needs to be deleted, so don't
        // touch it until all the other records are gone.
        diesel::delete(sitrep_dsl::fm_sitrep.filter(sitrep_dsl::id.eq_any(ids)))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn fm_sitrep_version_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, SqlU32>,
    ) -> ListResultVec<fm::SitrepVersion> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO(eliza): there should probably be an authz object for the fm sitrep?
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        let sitreps = Self::sitrep_version_list_query(pagparams)
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        Ok(sitreps)
    }

    fn sitrep_version_list_query(
        pagparams: &DataPageParams<'_, SqlU32>,
    ) -> impl RunnableQuery<model::SitrepVersion> {
        paginated(
            history_dsl::fm_sitrep_history,
            history_dsl::version,
            &pagparams,
        )
        .select(model::SitrepVersion::as_select())
    }
}

/// Errors returned by [`DataStore::fm_sitrep_insert`].
#[derive(Debug, thiserror::Error)]
pub enum InsertSitrepError {
    #[error(transparent)]
    Other(#[from] Error),
    /// The parent sitrep ID is no longer the current sitrep.
    #[error("sitrep {0}'s parent is not the current sitrep")]
    ParentNotCurrent(SitrepUuid),
}

/// Query to insert a new sitrep version into the `fm_sitrep_history` table,
/// making it the current sitrep.
///
/// This implements the "compare-and-swap" operation [described in RFD
/// 603](https://rfd.shared.oxide.computer/rfd/0603#_creating_sitreps). In
/// particular, this query will insert a new sitrep version into the
/// `fm_sitrep_history` table IF AND ONLY IF one of the following conditions
/// are true:
///
/// 1. The new sitrep's parent sitrep ID is the current sitrep (i.e. the sitrep
///    with the highest version number in `fm_sitrep_history`)
/// 2. The new sitrep's parent sitrep ID is `NULL`, AND there are no other
///    sitreps in `fm_sitrep_history` (i.e., we are inserting the first-ever
///    sitrep)
///
/// Upholding these invariants ensures that sitreps are sequentially consistent,
/// and `fm_sitrep_history` always contains a linear history of sitreps which
/// were generated based on the previous current sitrep.
///
/// The CTE used to perform this operation is based on the one used in the
/// `deployment` module to insert blueprints into the `bp_target` table. It
/// differs in that it does not perform an existence check on the sitrep to be
/// made current. This is because the `db::datastore::deployment` module's
/// public API treats inserting a new blueprint and setting it as the current
/// target as separate operations, so it is possible for a consumer of the API
/// to try and set a blueprint as the target without first having created it.
/// Here, however, we only ever set a sitrep as the current sitrep in the
/// `Datastore::fm_sitrep_insert` method, which also creates the sitrep. So, it
/// is impossible for a consumer of this API to attempt to make a sitrep current
/// without having first created it.
///
/// The SQL generated for this CTE looks like this:
///
/// ```sql
/// WITH
///   -- Subquery to fetch the current sitrep (i.e., the row with the max
///   -- version).
///   current_sitrep AS (
///     SELECT
///       "version" AS version,
///       "sitrep_id" AS sitrep_id,
///     FROM "fm_sitrep_history"
///     ORDER BY "version" DESC
///     LIMIT 1
///   ),
///
///   -- Error checking subquery: This uses similar tricks as elsewhere in
///   -- this crate to `CAST(... AS UUID)` with non-UUID values that result
///   -- in runtime errors in specific cases, allowing us to give accurate
///   -- error messages.
///   --
///   -- This checks that the sitrep descends directly from the current
///   -- sitrep, and will fail the query if it does not.
///   check_validity AS MATERIALIZED (
///     SELECT CAST(IF(
///       -- Check for whether our new sitrep's parent matches our current
///       -- sitrep. There are two cases here: The first is the common case
///       -- (i.e., the new sitrep has a parent: does it match the current
///       -- sitrep ID?). The second is the bootstrapping check: if we're
///       -- trying to insert a new sitrep that does not have a parent,
///       -- we should not have a sitrep target at all.
///       --
///       -- If either of these cases fails, we return `parent-not-current`.
///       (
///          SELECT "parent_sitrep_id" FROM "sitrep", current_sitrep
///          WHERE
///            "id" = <new_sitrep_id>
///            AND current_sitrep.sitrep_id = "parent_sitrep_id"
///       ) IS NOT NULL
///       OR
///       (
///          SELECT 1 FROM "sitrep"
///          WHERE
///            "id" = <new_target_id>
///            AND "parent_sitrep_id" IS NULL
///            AND NOT EXISTS (SELECT version FROM current_sitrep)
///       ) = 1,
///       -- Sometime between v22.1.9 and v22.2.19, Cockroach's type checker
///       -- became too smart for our `CAST(... as UUID)` error checking
///       -- gadget: it can infer that `<new_target_id>` must be a UUID, so
///       -- then tries to parse 'parent-not-target' and 'no-such-blueprint'
///       -- as UUIDs _during typechecking_, which causes the query to always
///       -- fail. We can defeat this by casting the UUID to text here, which
///       -- will allow the 'parent-not-target' and 'no-such-blueprint'
///       -- sentinels to survive type checking, making it to query execution
///       -- where they will only be cast to UUIDs at runtime in the failure
///       -- cases they're supposed to catch.
///       CAST(<new_sitrep_id> AS text),
///       'parent-not-current'
///     ) AS UUID)
///   ),
///
///   -- Determine the new version number to use: either 1 if this is the
///   -- first sitrep being made the current sitrep, or 1 higher than
///   -- the previous sitrep's version.
///   --
///   -- The final clauses of each of these WHERE clauses repeat the
///   -- checks performed above in `check_validity`, and will cause this
///   -- subquery to return no rows if we should not allow the new
///   -- target to be set.
///   new_sitrep AS (
///     SELECT 1 AS new_version FROM "sitrep"
///       WHERE
///         "id" = <new_sitrep_id>
///         AND "parent_sitrep_id" IS NULL
///         AND NOT EXISTS (SELECT version FROM current_sitrep)
///     UNION
///     SELECT current_sitrep.version + 1 FROM current_sitrep, "sitrep"
///       WHERE
///         "id" = <new_sitrep_id>
///         AND "parent_sitrep_id" IS NOT NULL
///         AND "parent_sitrep_id" = current_sitrep.sitrep_id
///   )
///
///   -- Perform the actual insertion.
///   INSERT INTO "sitrep_history"(
///     "version","sitrep_id","time_made_current"
///   )
///   SELECT
///     new_sitrep.new_version,
///     <new_sitrep_id>,
///     NOW()
///     FROM new_sitrep
/// ```
#[derive(Debug, Clone, Copy)]
struct InsertSitrepVersionQuery {
    sitrep_id: SitrepUuid,
}

// Uncastable sentinel used to detect we attempt to make a sitrep current when
// its parent sitrep ID is no longer the current sitrep.
const PARENT_NOT_CURRENT: &str = "parent-not-current";

// Error messages generated from the above sentinel values.
const PARENT_NOT_CURRENT_ERROR_MESSAGE: &str = "could not parse \
    \"parent-not-current\" as type uuid: \
     uuid: incorrect UUID length: parent-not-current";

impl InsertSitrepVersionQuery {
    fn decode_error(&self, err: DieselError) -> InsertSitrepError {
        match err {
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
                if info.message() == PARENT_NOT_CURRENT_ERROR_MESSAGE =>
            {
                InsertSitrepError::ParentNotCurrent(self.sitrep_id)
            }
            err => {
                let err = public_error_from_diesel(err, ErrorHandler::Server)
                    .internal_context("failed to insert new sitrep version");
                InsertSitrepError::Other(err)
            }
        }
    }
}

impl QueryId for InsertSitrepVersionQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
impl QueryFragment<Pg> for InsertSitrepVersionQuery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        use nexus_db_schema::schema;

        type FromClause<T> =
            diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
        const SITREP_FROM_CLAUSE: FromClause<schema::fm_sitrep::table> =
            FromClause::new();
        const SITREP_HISTORY_FROM_CLAUSE: FromClause<
            schema::fm_sitrep_history::table,
        > = FromClause::new();

        const CURRENT_SITREP: &'static str = "current_sitrep";

        out.push_sql("WITH ");
        out.push_identifier(CURRENT_SITREP)?;
        out.push_sql(" AS (SELECT ");
        out.push_identifier(history_dsl::version::NAME)?;
        out.push_sql(" AS version,");
        out.push_identifier(history_dsl::sitrep_id::NAME)?;
        out.push_sql(" AS sitrep_id");
        out.push_sql(" FROM ");
        SITREP_HISTORY_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" ORDER BY ");
        out.push_identifier(history_dsl::version::NAME)?;
        out.push_sql(" DESC LIMIT 1),");

        out.push_sql(
            "check_validity AS MATERIALIZED ( \
               SELECT \
                 CAST( \
                   IF(",
        );
        out.push_sql("(SELECT ");
        out.push_identifier(sitrep_dsl::parent_sitrep_id::NAME)?;
        out.push_sql(" FROM ");
        SITREP_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(", ");
        out.push_identifier(CURRENT_SITREP)?;
        out.push_sql(" WHERE ");
        out.push_identifier(sitrep_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.sitrep_id.as_untyped_uuid(),
        )?;
        out.push_sql(" AND ");
        out.push_identifier(CURRENT_SITREP)?;
        out.push_sql(".sitrep_id = ");
        out.push_identifier(sitrep_dsl::parent_sitrep_id::NAME)?;
        out.push_sql(
            ") IS NOT NULL \
            OR \
            (SELECT 1 FROM ",
        );
        SITREP_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(sitrep_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.sitrep_id.as_untyped_uuid(),
        )?;
        out.push_sql(" AND ");
        out.push_identifier(sitrep_dsl::parent_sitrep_id::NAME)?;
        out.push_sql(
            "IS NULL \
            AND NOT EXISTS ( \
                SELECT version FROM current_sitrep) \
            ) = 1, ",
        );
        out.push_sql("  CAST(");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.sitrep_id.as_untyped_uuid(),
        )?;
        out.push_sql("  AS text), ");
        out.push_bind_param::<sql_types::Text, &'static str>(
            &PARENT_NOT_CURRENT,
        )?;
        out.push_sql(
            ") \
            AS UUID) \
          ), ",
        );

        out.push_sql("new_sitrep AS (SELECT 1 AS new_version FROM ");
        SITREP_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(sitrep_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.sitrep_id.as_untyped_uuid(),
        )?;
        out.push_sql(" AND ");
        out.push_identifier(sitrep_dsl::parent_sitrep_id::NAME)?;
        out.push_sql(
            " IS NULL \
            AND NOT EXISTS \
            (SELECT version FROM current_sitrep) \
             UNION \
            SELECT current_sitrep.version + 1 FROM \
              current_sitrep, ",
        );
        SITREP_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(sitrep_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.sitrep_id.as_untyped_uuid(),
        )?;
        out.push_sql(" AND ");
        out.push_identifier(sitrep_dsl::parent_sitrep_id::NAME)?;
        out.push_sql(" IS NOT NULL AND ");
        out.push_identifier(sitrep_dsl::parent_sitrep_id::NAME)?;
        out.push_sql(" = current_sitrep.sitrep_id) ");

        out.push_sql("INSERT INTO ");
        SITREP_HISTORY_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql("(");
        out.push_identifier(history_dsl::version::NAME)?;
        out.push_sql(",");
        out.push_identifier(history_dsl::sitrep_id::NAME)?;
        out.push_sql(",");
        out.push_identifier(history_dsl::time_made_current::NAME)?;
        out.push_sql(") SELECT new_sitrep.new_version, ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.sitrep_id.as_untyped_uuid(),
        )?;
        out.push_sql(", NOW()");
        out.push_sql(" FROM new_sitrep");

        Ok(())
    }
}

impl RunQueryDsl<DbConnection> for InsertSitrepVersionQuery {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pagination::Paginator;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use chrono::Utc;
    use nexus_types::fm;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn explain_insert_sitrep_version_query() {
        let logctx = dev::test_setup_log("explain_insert_sitrep_version_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = InsertSitrepVersionQuery { sitrep_id: SitrepUuid::nil() };

        // Before trying to explain the query, let's start by making sure it's
        // valid SQL...
        let q = diesel::debug_query::<Pg, _>(&query).to_string();
        match dev::db::format_sql(&q).await {
            Ok(q) => eprintln!("query: {q}"),
            Err(e) => panic!("query is malformed: {e}\n{q}"),
        }

        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        eprintln!("{explanation}");
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Ensure we have the right query contents.
    #[tokio::test]
    async fn expectorate_sitrep_list_orphans_no_marker() {
        let pagparams = DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(420).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        };
        let query = DataStore::list_orphaned_query(&pagparams);
        expectorate_query_contents(
            &query,
            "tests/output/sitrep_list_orphans_no_marker.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_sitrep_list_orphans_with_marker() {
        let pagparams = DataPageParams {
            marker: Some(&SitrepUuid::nil()),
            limit: std::num::NonZeroU32::new(420).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        };
        let query = DataStore::list_orphaned_query(&pagparams);
        expectorate_query_contents(
            &query,
            "tests/output/sitrep_list_orphans_with_marker.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_sitrep_list_orphaned_query() {
        let logctx = dev::test_setup_log("explain_sitrep_list_orphaned_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let pagparams = DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(420).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        };
        let query = DataStore::list_orphaned_query(&pagparams);
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        eprintln!("{explanation}");
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_sitrep_version_list_query() {
        let logctx = dev::test_setup_log("explain_sitrep_version_list_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let pagparams = DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(420).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        };
        let query = DataStore::sitrep_version_list_query(&pagparams);
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        eprintln!("{explanation}");
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_insert_sitrep_without_parent() {
        // Setup
        let logctx = dev::test_setup_log("test_insert_sitrep_without_parent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Base case: there should be no current sitrep.
        let current = datastore.fm_sitrep_read_current(&opctx).await.unwrap();
        assert!(current.is_none());

        // Okay, let's create a new sitrep.
        let sitrep = nexus_types::fm::Sitrep {
            metadata: nexus_types::fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "TEST SITREP PLEASE IGNORE".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
        };

        datastore.fm_sitrep_insert(&opctx, &sitrep).await.unwrap();

        let current = datastore
            .fm_sitrep_read_current(&opctx)
            .await
            .expect("should successfully read current sitrep");
        let (version, current_sitrep) = current.expect("sitrep should be Some");
        assert_eq!(version.id, sitrep.metadata.id);
        assert_eq!(version.version, 1);
        assert_eq!(sitrep.id(), current_sitrep.id());
        assert_eq!(sitrep.parent_id(), current_sitrep.parent_id());
        assert_eq!(
            sitrep.metadata.creator_id,
            current_sitrep.metadata.creator_id
        );
        assert_eq!(sitrep.metadata.comment, current_sitrep.metadata.comment);

        // Trying to insert the same sitrep again should fail.
        let err =
            datastore.fm_sitrep_insert(&opctx, &sitrep).await.unwrap_err();
        assert!(err.to_string().contains("duplicate key"));

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_insert_sitrep_with_current_parent() {
        let logctx =
            dev::test_setup_log("test_insert_sitrep_with_current_parent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let creator_id = OmicronZoneUuid::new_v4();
        // Create an initial sitrep (no parent)
        let sitrep1 = nexus_types::fm::Sitrep {
            metadata: nexus_types::fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id,
                comment: "TEST SITREP 1".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
        };
        datastore.fm_sitrep_insert(&opctx, &sitrep1).await.unwrap();

        // Create a second sitrep with the first as parent
        let sitrep2 = nexus_types::fm::Sitrep {
            metadata: nexus_types::fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id,
                comment: "TEST SITREP 2".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: Some(sitrep1.id()),
            },
        };
        datastore.fm_sitrep_insert(&opctx, &sitrep2).await.expect(
            "inserting a sitrep whose parent is current should succeed",
        );

        // Verify the second sitrep is now current
        let (version, current_sitrep) = datastore
            .fm_sitrep_read_current(&opctx)
            .await
            .unwrap()
            .expect("current sitrep should be Some");
        assert_eq!(version.id, sitrep2.id());
        assert_eq!(version.version, 2);
        assert_eq!(sitrep2.id(), current_sitrep.id());
        assert_eq!(sitrep2.parent_id(), current_sitrep.parent_id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_insert_sitrep_with_nonexistent_parent_fails() {
        let logctx = dev::test_setup_log(
            "test_insert_sitrep_with_nonexistent_parent_fails",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let creator_id = OmicronZoneUuid::new_v4();

        // Create an initial sitrep (no parent)
        let sitrep1 = nexus_types::fm::Sitrep {
            metadata: nexus_types::fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id,
                comment: "TEST SITREP 1".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
        };
        datastore.fm_sitrep_insert(&opctx, &sitrep1).await.unwrap();

        // Try to insert a sitrep with a non-existent parent ID
        let nonexistent_id = SitrepUuid::new_v4();
        let sitrep2 = nexus_types::fm::Sitrep {
            metadata: nexus_types::fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id,
                comment: "TEST SITREP WITH BAD PARENT".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: Some(nonexistent_id),
            },
        };

        let result = datastore.fm_sitrep_insert(&opctx, &sitrep2).await;

        // Should fail with ParentNotCurrent error
        match result {
            Err(super::InsertSitrepError::ParentNotCurrent(_)) => {}
            _ => panic!("expected ParentNotCurrent error, got {result:?}"),
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_insert_sitrep_with_outdated_parent_fails() {
        let logctx = dev::test_setup_log(
            "test_insert_sitrep_with_outdated_parent_fails",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let creator_id = OmicronZoneUuid::new_v4();

        // Create an initial sitrep (no parent)
        let sitrep1 = nexus_types::fm::Sitrep {
            metadata: nexus_types::fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id,
                comment: "TEST SITREP 1".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
        };
        datastore.fm_sitrep_insert(&opctx, &sitrep1).await.unwrap();

        // Create a second sitrep with the first as parent
        let sitrep2 = nexus_types::fm::Sitrep {
            metadata: nexus_types::fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id,
                comment: "TEST SITREP 2".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: Some(sitrep1.id()),
            },
        };
        datastore.fm_sitrep_insert(&opctx, &sitrep2).await.unwrap();

        // Try to create a third sitrep with sitrep1 (outdated) as parent.
        // This should fail, as sitrep2 is now the current sitrep.
        let sitrep3 = nexus_types::fm::Sitrep {
            metadata: nexus_types::fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "TEST SITREP 3 WITH OUTDATED PARENT".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: Some(sitrep1.id()),
            },
        };
        let result = datastore.fm_sitrep_insert(&opctx, &sitrep3).await;

        // Should fail with ParentNotCurrent error
        match result {
            Err(InsertSitrepError::ParentNotCurrent(_)) => {}
            _ => panic!("expected ParentNotCurrent error, got {result:?}"),
        }

        // Verify sitrep2 is still current
        let (version, current_sitrep) = datastore
            .fm_sitrep_read_current(&opctx)
            .await
            .unwrap()
            .expect("current sitrep should be Some");
        assert_eq!(version.id, sitrep2.id());
        assert_eq!(version.version, 2);
        assert_eq!(sitrep2.id(), current_sitrep.id());
        assert_eq!(sitrep2.parent_id(), current_sitrep.parent_id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sitrep_list_orphaned() {
        let logctx = dev::test_setup_log("test_sitrep_list_orphaned");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // First, insert an initial sitrep. This should succeed.
        let sitrep1 = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep v1".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
        };
        datastore
            .fm_sitrep_insert(&opctx, &sitrep1)
            .await
            .expect("inserting initial sitrep should succeed");

        // Now, create some orphaned sitreps which also have no parent.
        let mut expected_orphans = BTreeSet::new();
        for i in 1..5 {
            insert_orphan(
                &datastore,
                &opctx,
                &mut expected_orphans,
                None,
                1,
                i,
            )
            .await;
        }

        // List orphans at the current version.
        let v1 = datastore
            .fm_current_sitrep_version(&opctx)
            .await
            .unwrap()
            .expect("should have a version");
        assert_eq!(dbg!(&v1).id, sitrep1.metadata.id);
        let listed_orphans = list_orphans(&datastore, &opctx).await.unwrap();
        assert_eq!(dbg!(&listed_orphans), &expected_orphans);

        // Next, create a new sitrep which descends from sitrep 1.
        let sitrep2 = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep v2".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: Some(sitrep1.metadata.id),
            },
        };
        datastore
            .fm_sitrep_insert(&opctx, &sitrep2)
            .await
            .expect("inserting child sitrep should succeed");

        // Now, create some orphaned sitreps which also descend from sitrep 1.
        for i in 1..5 {
            insert_orphan(
                &datastore,
                &opctx,
                &mut expected_orphans,
                Some(sitrep1.metadata.id),
                2,
                i,
            )
            .await;
        }

        // List orphans at the current version.
        let listed_orphans = list_orphans(&datastore, &opctx).await.unwrap();
        assert_eq!(dbg!(&listed_orphans), &expected_orphans);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn list_orphans(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> Result<BTreeSet<SitrepUuid>, Error> {
        let mut listed_orphans = BTreeSet::new();
        let mut paginator = Paginator::new(
            crate::db::datastore::SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Descending,
        );
        while let Some(p) = paginator.next() {
            let orphans = datastore
                .fm_sitrep_list_orphaned(&opctx, p.current_pagparams())
                .await?;
            paginator = p.found_batch(&orphans, &|id| *id);
            listed_orphans.extend(orphans);
        }
        Ok(listed_orphans)
    }

    async fn insert_orphan(
        datastore: &DataStore,
        opctx: &OpContext,
        orphans: &mut BTreeSet<SitrepUuid>,
        parent_sitrep_id: Option<SitrepUuid>,
        v: usize,
        i: usize,
    ) {
        let sitrep = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: format!("test sitrep v{i}; orphan {i}"),
                time_created: Utc::now(),
                parent_sitrep_id,
            },
        };
        match datastore.fm_sitrep_insert(&opctx, &sitrep).await {
            Ok(_) => {
                panic!("inserting sitrep v{v} orphan {i} should not succeed")
            }
            Err(InsertSitrepError::ParentNotCurrent(id)) => {
                orphans.insert(id);
            }
            Err(InsertSitrepError::Other(e)) => {
                panic!(
                    "expected inserting sitrep v{v} orphan {i} to fail because \
                     its parent is out of date, but saw an unexpected error: {e}"
                );
            }
        }
    }
}
