// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on fault management internal data.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::SitrepMetadata;
use crate::db::model::SitrepVersion;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::fm_sitrep::dsl as sitrep_dsl;
use nexus_db_schema::schema::fm_sitrep_history::dsl as current_sitrep_dsl;
use nexus_types::fm::Sitrep;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SitrepUuid;

impl DataStore {
    pub async fn fm_get_current_sitrep_version(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<SitrepVersion>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.fm_get_current_sitrep_version_on_conn(&conn).await
    }

    async fn fm_get_current_sitrep_version_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<SitrepVersion>, Error> {
        current_sitrep_dsl::fm_sitrep_history
            .order_by(current_sitrep_dsl::version.desc())
            .select(SitrepVersion::as_select())
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn fm_sitrep_metadata_read(
        &self,
        opctx: &OpContext,
        id: SitrepUuid,
    ) -> Result<Option<SitrepMetadata>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.fm_sitrep_metadata_read_on_conn(id, &conn).await
    }

    async fn fm_sitrep_metadata_read_on_conn(
        &self,
        id: SitrepUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<SitrepMetadata>, Error> {
        sitrep_dsl::fm_sitrep
            .filter(sitrep_dsl::id.eq(id.into_untyped_uuid()))
            .select(SitrepMetadata::as_select())
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn fm_sitrep_read(
        &self,
        opctx: &OpContext,
        id: SitrepUuid,
    ) -> Result<Sitrep, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let metadata = self
            .fm_sitrep_metadata_read_on_conn(id, &conn)
            .await?
            .ok_or_else(|| {
                Error::non_resourcetype_not_found(format!("sitrep {id:?}"))
            })?
            .into();

        // TODO(eliza): this is where we would read all the other sitrep data,
        // if there was any.

        Ok(Sitrep { metadata })
    }
}

/// Query to insert a new current sitrep.
///
/// ```sql
/// WITH
///   -- Subquery to fetch the current sitrep (i.e., the row with the max
///   -- version).
///   current_sitrep AS (
///     SELECT
///       "version" AS version,
///       "sitrep_id" AS sitrep_id,
///       "inv_collection"."time_done" AS inv_collection_time_done,
///     FROM "sitrep_history"
///     INNER JOIN "sitrep" ON "sitrep_history"."sitrep_id" = "sitrep"."id"
///     LEFT JOIN "inv_collection" ON "sitrep"."inv_collection_id" = "inv_collection"."id"
///     ORDER BY "version" DESC
///     LIMIT 1
///   ),
///
///   -- Error checking subquery: This uses similar tricks as elsewhere in
///   -- this crate to `CAST(... AS UUID)` with non-UUID values that result
///   -- in runtime errors in specific cases, allowing us to give accurate
///   -- error messages.
///   --
///   -- These checks are not required for correct behavior by the insert
///   -- below. If we removed them, the insert would insert 0 rows if
///   -- these checks would have failed. But they make it easier to report
///   -- specific problems to our caller.
///   --
///   -- The specific cases we check here are noted below.
///   check_validity AS MATERIALIZED (
///     SELECT CAST(IF(
///       -- Check for whether our new sitrep's inventory collection was
///       -- completed earlier than the inventory collection of the parent
///       -- sitrep. If this is the case, the new sitrep was generated based
///       -- on an older inventory correction, and may be invalid.
///       --
///       -- Note that if the parent's inventory collection no longer exists
///       -- in the inv_collection table, it has been deleted, and therefore
///       -- we just kinda assume it must be older than ours.
///       -- However, if *ours* no longer exists, then it must be older than
///       -- the parent's inventory collection.
///       (SELECT 1 FROM "sitrep"
///        INNER JOIN "inv_collection" ON "sitrep"."inv_collection_id" = "inv_collection"."id"
///        WHERE
///          "id" = <new_sitrep_id> AND
///          "inv_collection"."time_done" < current_sitrep.inv_collection_time_done
///       ) = 1,
///       'inv-collection-time-travelled',
///       IF(
///         -- Check for whether our new sitrep's parent matches our current
///         -- sitrep. There are two cases here: The first is the common case
///         -- (i.e., the new sitrep has a parent: does it match the current
///         -- sitrep ID?). The second is the bootstrapping check: if we're
///         -- trying to insert a new sitrep that does not have a parent,
///         -- we should not have a sitrep target at all.
///         --
///         -- If either of these cases fails, we return `parent-not-current`.
///         (
///            SELECT "parent_sitrep_id" FROM "sitrep", current_sitrep
///            WHERE
///              "id" = <new_sitrep_id>
///              AND current_sitrep.sitrep_id = "parent_sitrep_id"
///         ) IS NOT NULL
///         OR
///         (
///            SELECT 1 FROM "sitrep"
///            WHERE
///              "id" = <new_target_id>
///              AND "parent_sitrep_id" IS NULL
///              AND NOT EXISTS (SELECT version FROM current_sitrep)
///         ) = 1,
///         -- Sometime between v22.1.9 and v22.2.19, Cockroach's type checker
///         -- became too smart for our `CAST(... as UUID)` error checking
///         -- gadget: it can infer that `<new_target_id>` must be a UUID, so
///         -- then tries to parse 'parent-not-target' and 'no-such-blueprint'
///         -- as UUIDs _during typechecking_, which causes the query to always
///         -- fail. We can defeat this by casting the UUID to text here, which
///         -- will allow the 'parent-not-target' and 'no-such-blueprint'
///         -- sentinels to survive type checking, making it to query execution
///         -- where they will only be cast to UUIDs at runtime in the failure
///         -- cases they're supposed to catch.
///         CAST(<new_sitrep_id> AS text),
///         'parent-not-current'
///       )
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
    time_made_current: DateTime<Utc>,
}

#[derive(Debug)]
pub enum InsertSitrepError {
    Other(DieselError),
    /// The parent sitrep ID is no longer the current sitrep.
    ParentNotCurrent(SitrepUuid),
    InvCollectionTimeTravelled(SitrepUuid),
    InventoryCollectionTimeTravelled,
}

// Uncastable sentinel used to detect we attempt to make a sitrep current when
// its parent sitrep ID is no longer the current sitrep.
const PARENT_NOT_CURRENT: &str = "parent-not-current";

// Uncastable sentinel used to detect we attempt to make a sitrep current when
// its inventory collection ID refers to an inventory collection that is older
// than the parent sitrep's inventory collection.
const INV_COLLECTION_TIME_TRAVELLED: &str = "inv-collection-time-travelled";

// Error messages generated from the above sentinel values.
const PARENT_NOT_CURRENT_ERROR_MESSAGE: &str = "could not parse \
    \"parent-not-current\" as type uuid: \
     uuid: incorrect UUID length: parent-not-current";
const INV_COLLECTION_TIME_TRAVELLED_ERROR_MESSAGE: &str = "could not parse \
    \"inv-collection-time-travelled\" as type uuid: \
     uuid: incorrect UUID length: inv-collection-time-travelled";

impl InsertSitrepVersionQuery {
    fn decode_error(&self, err: DieselError) -> InsertSitrepError {
        match err {
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
                if info.message() == PARENT_NOT_CURRENT_ERROR_MESSAGE =>
            {
                InsertSitrepError::ParentNotCurrent(self.sitrep_id)
            }
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
                if info.message()
                    == INV_COLLECTION_TIME_TRAVELLED_ERROR_MESSAGE =>
            {
                InsertSitrepError::InventoryCollectionTimeTravelled(
                    self.sitrep_id,
                )
            }
            other => InsertSitrepError::Other(other),
        }
    }
}
