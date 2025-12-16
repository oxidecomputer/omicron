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
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model;
use crate::db::model::DbTypedUuid;
use crate::db::model::SqlU32;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::TypedSqlQuery;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use dropshot::PaginationOrder;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::ereport::dsl as ereport_dsl;
use nexus_db_schema::schema::fm_case::dsl as case_dsl;
use nexus_db_schema::schema::fm_ereport_in_case::dsl as case_ereport_dsl;
use nexus_db_schema::schema::fm_sitrep::dsl as sitrep_dsl;
use nexus_db_schema::schema::fm_sitrep_history::dsl as history_dsl;
use nexus_types::fm;
use nexus_types::fm::Sitrep;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::CaseEreportKind;
use omicron_uuid_kinds::CaseKind;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SitrepUuid;
use std::collections::HashMap;
use std::sync::Arc;
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

    /// Reads the entire content of the sitrep with the provided ID, if one
    /// exists.
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

        // Fetch all ereports assigned to cases in this sitrep. We do this by
        // querying the `fm_ereport_in_case` table for all entries with this
        // sitrep ID, paginated by the ereport assignment's UUID. This query is
        // `INNER JOIN`ed with the `fm_ereport` table to fetch the ereport's
        // data for that assignment.
        //
        // We use the results of this query to populate a map of case UUIDs to
        // the map of ereports assigned to that case. Ereports are de-duplicated
        // using an additional map of `Arc`ed ereports, to reduce the in-memory
        // size of the sitrep when an ereport is assigned to multiple cases. the
        // JOINed query *will* potentially load the same ereport multiple times
        // in that case, but this is still probably much more efficient than
        // issuing a bunch of smaller queries to load ereports individually.
        let mut case_ereports =
            {
                // TODO(eliza): as a potential optimization, since ereport
                // records are immutable, we might consider hanging onto this
                // map of all ereports in the `Sitrep` structure. Then, when we
                // load the next sitrep, we could first check if the ereports in
                // that sitrep are contained in the map before loading them
                // again. That would require changing the rest of this code to
                // not `JOIN` with the ereports table here, and instead populate
                // a list of additional ereports we need to load, and issue a
                // separate query for that. But, it's worth considering maybe if
                // this becomes a bottleneck...
                let mut ereports = iddqd::IdOrdMap::<Arc<fm::Ereport>>::new();
                let mut map = HashMap::<CaseUuid, iddqd::IdOrdMap<_>>::new();

                let mut paginator =
                    Paginator::new(SQL_BATCH_SIZE, PaginationOrder::Descending);
                while let Some(p) = paginator.next() {
                    let batch = DataStore::fm_sitrep_read_ereports_query(
                        id,
                        &p.current_pagparams(),
                    )
                    .load_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                            .internal_context(
                                "failed to load case ereport assignments",
                            )
                    })?;

                    paginator =
                        p.found_batch(&batch, &|(assignment, _)| assignment.id);
                    for (assignment, ereport) in batch {
                        let ereport_id = fm::EreportId {
                            restart_id: ereport.restart_id.into(),
                            ena: ereport.ena.into(),
                        };
                        let ereport = match ereports.entry(&ereport_id) {
                            iddqd::id_ord_map::Entry::Occupied(entry) => {
                                entry.get().clone()
                            }
                            iddqd::id_ord_map::Entry::Vacant(entry) => {
                                let ereport =
                                    Arc::new(fm::Ereport::try_from(ereport)?);
                                entry.insert(ereport.clone());
                                ereport
                            }
                        };
                        let id = assignment.id.into();
                        let case_id = assignment.case_id.into();
                        map.entry(case_id).or_default().insert_unique(
                    fm::case::CaseEreport {
                        id,
                        ereport,
                        assigned_sitrep_id: assignment
                            .assigned_sitrep_id
                            .into(),
                        comment: assignment.comment,
                    },
                ).map_err(|_| Error::InternalError { internal_message:
                    format!(
                        "encountered multiple case ereports for case \
                            {case_id} with the same UUID {id}. this should \
                            really not be possible, as the assignment UUID \
                            is a primary key!",
                    )})?;
                    }
                }

                map
            };
        // Next, load the case metadata entries and marry them to the sets of
        // ereports assigned to those cases that we loaded in the previous step.
        let cases = {
            let mut cases = iddqd::IdOrdMap::new();
            let mut paginator =
                Paginator::new(SQL_BATCH_SIZE, PaginationOrder::Descending);
            while let Some(p) = paginator.next() {
                let batch = self
                    .fm_sitrep_cases_list_on_conn(
                        id,
                        &p.current_pagparams(),
                        &conn,
                    )
                    .await
                    .map_err(|e| {
                        e.internal_context("failed to list sitrep cases")
                    })?;
                paginator = p.found_batch(&batch, &|case| case.id);
                cases.extend(batch.into_iter().map(|case| {
                    let model::fm::CaseMetadata {
                        id,
                        sitrep_id: _,
                        created_sitrep_id,
                        closed_sitrep_id,
                        comment,
                        de,
                    } = case;
                    let id = id.into();

                    // Take all the case ereport assignments we've collected for this case.
                    let ereports = case_ereports
                        .remove(&id)
                        // If there's no entry in the map of case ereport
                        // assignments for this case, then that just means that the
                        // case has no ereports assigned to it, so insert an empty
                        // map here.
                        .unwrap_or_default();
                    fm::Case {
                        id,
                        created_sitrep_id: created_sitrep_id.into(),
                        closed_sitrep_id: closed_sitrep_id.map(Into::into),
                        de: de.into(),
                        comment,
                        ereports,
                    }
                }));
            }

            cases
        };

        Ok(Sitrep { metadata, cases })
    }

    async fn fm_sitrep_cases_list_on_conn(
        &self,
        sitrep_id: SitrepUuid,
        pagparams: &DataPageParams<'_, DbTypedUuid<CaseKind>>,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> ListResultVec<model::fm::CaseMetadata> {
        paginated(case_dsl::fm_case, case_dsl::id, &pagparams)
            .filter(case_dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()))
            .select(model::fm::CaseMetadata::as_select())
            .load_async::<model::fm::CaseMetadata>(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn fm_sitrep_read_ereports_query(
        sitrep_id: SitrepUuid,
        pagparams: &DataPageParams<'_, DbTypedUuid<CaseEreportKind>>,
    ) -> impl RunnableQuery<(model::fm::CaseEreport, model::Ereport)> + use<>
    {
        paginated(
            case_ereport_dsl::fm_ereport_in_case,
            case_ereport_dsl::id,
            pagparams,
        )
        .filter(case_ereport_dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()))
        .inner_join(
            ereport_dsl::ereport.on(ereport_dsl::restart_id
                .eq(case_ereport_dsl::restart_id)
                .and(ereport_dsl::ena.eq(case_ereport_dsl::ena))),
        )
        .select((
            model::fm::CaseEreport::as_select(),
            model::Ereport::as_select(),
        ))
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
        sitrep: Sitrep,
    ) -> Result<(), InsertSitrepError> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO(eliza): there should probably be an authz object for the fm sitrep?
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let sitrep_id = sitrep.id();

        // Create the sitrep metadata record.
        //
        // NOTE: we must insert this record before anything else, because it's
        // how orphaned sitreps are found when performing garbage collection.
        // Were we to first insert some other records and insert the metadata
        // record *last*, we could die when we have inserted some sitrep data
        // but have yet to create the metadata record. If this occurs, those
        // records could not be easily found by the garbage collection task.
        // Those (unused) records would then be permanently leaked without
        // manual human intervention to delete them.
        diesel::insert_into(sitrep_dsl::fm_sitrep)
            .values(model::SitrepMetadata::from(sitrep.metadata))
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to insert sitrep metadata record")
            })?;

        // Create case records.
        let mut cases = Vec::with_capacity(sitrep.cases.len());
        for case in sitrep.cases {
            // TODO(eliza): some of this could be done in parallel using a
            // `ParallelTaskSet`, if the time it takes to insert a sitrep were
            // to become important?
            let model::fm::Case { metadata, ereports } =
                model::fm::Case::from_sitrep(sitrep_id, case);

            if !ereports.is_empty() {
                diesel::insert_into(case_ereport_dsl::fm_ereport_in_case)
                    .values(ereports)
                    .execute_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                            .internal_context(format!(
                                "failed to insert ereport records for case {}",
                                metadata.id
                            ))
                    })?;
            }

            cases.push(metadata);
        }

        if !cases.is_empty() {
            diesel::insert_into(case_dsl::fm_case)
                .values(cases)
                .execute_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context("failed to insert case records")
                })?;
        }

        // Now, try to make the sitrep current.
        let query = Self::insert_sitrep_version_query(sitrep_id);
        query
            .execute_async(&*conn)
            .await
            .map_err(|err| match err {
                DieselError::DatabaseError(
                    DatabaseErrorKind::Unknown,
                    info,
                ) if info.message()
                    == Self::PARENT_NOT_CURRENT_ERROR_MESSAGE =>
                {
                    InsertSitrepError::ParentNotCurrent(sitrep_id)
                }
                err => {
                    let err =
                        public_error_from_diesel(err, ErrorHandler::Server)
                            .internal_context(
                                "failed to insert new sitrep version",
                            );
                    InsertSitrepError::Other(err)
                }
            })
            .map(|_| ())
    }

    // Uncastable sentinel used to detect we attempt to make a sitrep current when
    // its parent sitrep ID is no longer the current sitrep.
    const PARENT_NOT_CURRENT: &str = "parent-not-current";

    // Error messages generated from the above sentinel values.
    const PARENT_NOT_CURRENT_ERROR_MESSAGE: &str = "could not parse \
        \"parent-not-current\" as type uuid: \
         uuid: incorrect UUID length: parent-not-current";

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
    fn insert_sitrep_version_query(
        sitrep_id: SitrepUuid,
    ) -> TypedSqlQuery<sql_types::BigInt> {
        let mut builder = QueryBuilder::new();

        // Subquery to fetch the current sitrep (i.e., the row with the max
        // version).
        builder.sql(
            "WITH current_sitrep AS ( \
                SELECT version, sitrep_id \
                FROM omicron.public.fm_sitrep_history \
                ORDER BY version DESC \
                LIMIT 1 \
            ), ",
        );

        // Error checking subquery: This uses similar tricks as elsewhere in
        // this crate to `CAST(... AS UUID)` with non-UUID values that result
        // in runtime errors in specific cases, allowing us to give accurate
        // error messages.
        //
        // This checks that the sitrep descends directly from the current
        // sitrep, and will fail the query if it does not.
        builder
            .sql("check_validity AS MATERIALIZED ( ")
            // Check for whether our new sitrep's parent matches our current
            // sitrep. There are two cases here: The first is the common case
            // (i.e., the new sitrep has a parent: does it match the current
            // sitrep ID?). The second is the bootstrapping check: if we're
            // trying to insert a new sitrep that does not have a parent, we
            // should not have a sitrep target at all.
            //
            // If either of these cases fails, we return `parent-not-current`.
            .sql(
                "SELECT CAST( \
                    IF( \
                        (SELECT parent_sitrep_id \
                         FROM omicron.public.fm_sitrep, current_sitrep \
                         WHERE id = ",
            )
            .param()
            .bind::<sql_types::Uuid, _>(sitrep_id.into_untyped_uuid())
            .sql(
                " AND current_sitrep.sitrep_id = parent_sitrep_id) IS NOT NULL \
                    OR \
                    (SELECT 1 FROM omicron.public.fm_sitrep \
                     WHERE id = ",
            )
            .param()
            .bind::<sql_types::Uuid, _>(sitrep_id.into_untyped_uuid())
            .sql(
                " AND parent_sitrep_id IS NULL \
                  AND NOT EXISTS (SELECT version FROM current_sitrep)) = 1,",
            )
            // Sometime between v22.1.9 and v22.2.19, Cockroach's type checker
            // became too smart for our `CAST(... as UUID)` error checking
            // gadget: it can infer that `<new_target_id>` must be a UUID, so
            // then tries to parse 'parent-not-target' as UUIDs _during
            // typechecking_, which causes the query to always fail. We can
            // defeat this by casting the UUID to text here, which will allow
            // the 'parent-not-target' sentinel to survive type checking, making
            // it to query execution where they will only be cast to UUIDs at
            // runtime in the failure cases they're supposed to catch.
            .sql(" CAST(")
            .param()
            .bind::<sql_types::Uuid, _>(sitrep_id.into_untyped_uuid())
            // opening single quote for `PARENT_NOT_CURRENT` string literal
            .sql(" AS text), '")
            .sql(Self::PARENT_NOT_CURRENT)
            .sql(
                "') AS UUID \
                ) \
                ), ",
            );

        // Determine the new version number to use: either 1 if this is the
        // first sitrep being made the current sitrep, or 1 higher than
        // the previous sitrep's version.
        //
        // The final clauses of each of these WHERE clauses repeat the
        // checks performed above in `check_validity`, and will cause this
        // subquery to return no rows if we should not allow the new
        // target to be set.
        //
        // new_sitrep AS (
        //   SELECT 1 AS new_version FROM omicron.public.fm_sitrep
        //   WHERE
        //       id = $5 AND parent_sitrep_id IS NULL
        //   AND
        //      NOT EXISTS (SELECT version FROM current_sitrep)
        //   UNION
        //   SELECT
        //      current_sitrep.version + 1
        //   FROM
        //      current_sitrep, omicron.public.fm_sitrep
        //   WHERE
        //      id = $6 AND parent_sitrep_id IS NOT NULL
        //   AND parent_sitrep_id = current_sitrep.sitrep_id
        // )
        builder
            .sql(
                "new_sitrep AS ( \
                    SELECT 1 AS new_version FROM omicron.public.fm_sitrep \
                    WHERE id = ",
            )
            .param()
            .bind::<sql_types::Uuid, _>(sitrep_id.into_untyped_uuid())
            .sql(
                "AND parent_sitrep_id IS NULL \
                AND NOT EXISTS (SELECT version FROM current_sitrep) \
                UNION \
                SELECT current_sitrep.version + 1 \
                FROM current_sitrep, omicron.public.fm_sitrep \
                WHERE id = ",
            )
            .param()
            .bind::<sql_types::Uuid, _>(sitrep_id.into_untyped_uuid())
            .sql(
                " AND parent_sitrep_id IS NOT NULL \
                  AND parent_sitrep_id = current_sitrep.sitrep_id \
                ) ",
            );

        //Finally, perform the actual insertion.
        builder
            .sql(
                "INSERT INTO omicron.public.fm_sitrep_history \
                (version, sitrep_id, time_made_current) \
                SELECT new_sitrep.new_version, ",
            )
            .param()
            .bind::<sql_types::Uuid, _>(sitrep_id.into_untyped_uuid())
            .sql(", NOW() FROM new_sitrep");

        builder.query()
    }

    /// Lists all orphaned alternative sitreps (paginated by sitrep UUID).
    ///
    /// Orphaned sitreps are those which can never be committed to the
    /// `fm_sitrep_history` table, because their parent sitrep ID is no longer
    /// the current sitrep. Such sitreps are typically created when multiple
    /// Nexus instances attempt to generate a new sitrep based on the same
    /// parent. Only one of these sitreps can "win the race" to be committed to
    /// the history, and any alternative sitreps are left orphaned. Orphaned
    /// sitreps will never be read, since sitreps are only read when they are
    /// current,[^1] so they can safely be deleted at any time.
    ///
    /// This query is used by the `fm_sitrep_gc` background task, which is
    /// responsible for deleting orphaned sitreps.
    ///
    /// [^1]: Well, except for by OMDB, but that doesn't count.
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

    /// Returns the CTE for listing orphaned sitreps.
    ///
    /// This query selects the IDs of sitreps where:
    /// - the sitrep's ID is not present in the `fm_sitrep_history` table
    /// - AND the sitrep's `parent_sitrep_id` is NOT the current sitrep
    ///
    /// This query is paginated by sitrep UUID to avoid performing a full-table
    /// scan.
    ///
    /// This must be performed by a CTE in order to ensure that the
    /// `fm_sitrep_history` table is locked while we are SELECTing orphans, so
    /// that a new sitrep cannot be committed in the midst of the scan. If a new
    /// current sitrep could be inserted while we are SELECTing, any potential
    /// children of that sitrep would incorrectly be considered orphaned, as
    /// their parent sitrep ID would not be the same as the one the SELECT
    /// guards against selecting the children of. Therefore, we cannot perform
    /// this query against the `watch` channel provided by the
    /// `fm_sitrep_loader` background task, or by performing a separate query to
    /// select the current sitrep ID before selecting orphans.
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
            .bind::<sql_types::Int8, _>(i64::from(pagparams.limit.get()))
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
                    ( \
                        b.parent_sitrep_id IS NULL AND \
                        (SELECT sitrep_id from current_sitrep_id) IS NOT NULL \
                    ) \
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

        // Delete case ereport assignments
        let case_ereports_deleted = diesel::delete(
            case_ereport_dsl::fm_ereport_in_case
                .filter(case_ereport_dsl::sitrep_id.eq_any(ids.clone())),
        )
        .execute_async(&*conn)
        .await
        .map_err(|e| {
            public_error_from_diesel(e, ErrorHandler::Server)
                .internal_context("failed to delete case ereport assignments")
        })?;

        // Delete case metadata records.
        let cases_deleted = diesel::delete(
            case_dsl::fm_case.filter(case_dsl::sitrep_id.eq_any(ids.clone())),
        )
        .execute_async(&*conn)
        .await
        .map_err(|e| {
            public_error_from_diesel(e, ErrorHandler::Server)
                .internal_context("failed to delete case metadata")
        })?;

        // Delete the sitrep metadata entries *last*. This is necessary because
        // the rest of the delete operation is unsynchronized, and it is
        // possible for a Nexus to die before it has "fully deleted" a sitrep,
        // but deleted some of its records. The `fm_sitrep` (metadata) table is
        // the one that is used to determine whether a sitrep "exists" so that
        // the sitrep GC task can determine if it needs to be deleted, so don't
        // touch it until all the other records are gone.
        let sitreps_deleted = diesel::delete(
            sitrep_dsl::fm_sitrep.filter(sitrep_dsl::id.eq_any(ids.clone())),
        )
        .execute_async(&*conn)
        .await
        .map_err(|e| {
            public_error_from_diesel(e, ErrorHandler::Server)
                .internal_context("failed to delete sitrep metadata")
        })?;

        slog::debug!(
            &opctx.log,
            "deleted {sitreps_deleted} of {} sitreps sitreps", ids.len();
            "ids" => ?ids,
            "sitreps_deleted" => sitreps_deleted,
            "cases_deleted" => cases_deleted,
            "case_ereports_deleted" => case_ereports_deleted,
        );

        Ok(sitreps_deleted)
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
    ) -> impl RunnableQuery<model::SitrepVersion> + use<> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pagination::Paginator;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use chrono::Utc;
    use diesel::pg::Pg;
    use ereport_types;
    use nexus_types::fm;
    use nexus_types::fm::ereport::{EreportData, Reporter};
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    #[tokio::test]
    async fn expectorate_insert_sitrep_version_query() {
        let query = DataStore::insert_sitrep_version_query(SitrepUuid::nil());
        expectorate_query_contents(
            &query,
            "tests/output/insert_sitrep_version_query.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_insert_sitrep_version_query() {
        let logctx = dev::test_setup_log("explain_insert_sitrep_version_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::insert_sitrep_version_query(SitrepUuid::nil());

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
    async fn explain_sitrep_read_ereports_query() {
        let logctx = dev::test_setup_log("explain_sitrep_read_ereports_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let pagparams = DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(420).unwrap(),
            direction: dropshot::PaginationOrder::Descending,
        };
        let query = DataStore::fm_sitrep_read_ereports_query(
            SitrepUuid::nil(),
            &pagparams,
        );
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
            cases: Default::default(),
        };

        datastore.fm_sitrep_insert(&opctx, sitrep.clone()).await.unwrap();

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
        let err = datastore
            .fm_sitrep_insert(&opctx, sitrep.clone())
            .await
            .unwrap_err();
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
            cases: Default::default(),
        };
        datastore.fm_sitrep_insert(&opctx, sitrep1.clone()).await.unwrap();

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
            cases: Default::default(),
        };
        datastore.fm_sitrep_insert(&opctx, sitrep2.clone()).await.expect(
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
            cases: Default::default(),
        };
        datastore.fm_sitrep_insert(&opctx, sitrep1.clone()).await.unwrap();

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
            cases: Default::default(),
        };

        let result = datastore.fm_sitrep_insert(&opctx, sitrep2).await;

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
            cases: Default::default(),
        };
        datastore.fm_sitrep_insert(&opctx, sitrep1.clone()).await.unwrap();

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
            cases: Default::default(),
        };
        datastore.fm_sitrep_insert(&opctx, sitrep2.clone()).await.unwrap();

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
            cases: Default::default(),
        };
        let result = datastore.fm_sitrep_insert(&opctx, sitrep3.clone()).await;

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
            cases: Default::default(),
        };
        datastore
            .fm_sitrep_insert(&opctx, sitrep1.clone())
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
            cases: Default::default(),
        };
        datastore
            .fm_sitrep_insert(&opctx, sitrep2.clone())
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
            SQL_BATCH_SIZE,
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
            cases: Default::default(),
        };
        match datastore.fm_sitrep_insert(&opctx, sitrep).await {
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

    async fn make_sitrep_with_cases(
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> fm::Sitrep {
        // In order to read sitreps with case ereport assignments, the
        // corresponding entries in the `ereport` table must also exist, so
        // we'll make those here first.
        let restart_id = omicron_uuid_kinds::EreporterRestartUuid::new_v4();
        let collector_id = OmicronZoneUuid::new_v4();

        let ereport1 = EreportData {
            id: fm::EreportId { restart_id, ena: ereport_types::Ena(2) },
            time_collected: Utc::now(),
            collector_id,
            part_number: Some("930-55555".to_string()),
            serial_number: Some("BRM6900420".to_string()),
            class: Some("ereport.my_cool_ereport.wow".to_string()),
            report: serde_json::json!({"severity": "critical"}),
        };

        let ereport2 = EreportData {
            id: fm::EreportId { restart_id, ena: ereport_types::Ena(3) },
            time_collected: Utc::now(),
            collector_id,
            part_number: Some("930-55555".to_string()),
            serial_number: Some("BRM6900420".to_string()),
            class: Some("ereport.gov.nasa.apollo".to_string()),
            report: serde_json::json!({"message": "houston, we have a problem", "mission": 13,}),
        };

        // Insert the ereports
        let reporter = Reporter::Sp {
            sp_type: nexus_types::inventory::SpType::Sled,
            slot: 0,
        };

        datastore
            .ereports_insert(
                &opctx,
                reporter,
                vec![ereport1.clone(), ereport2.clone()],
            )
            .await
            .expect("failed to insert ereports");

        let sitrep_id = SitrepUuid::new_v4();
        let creator_id = OmicronZoneUuid::new_v4();
        let case1 = {
            let mut ereports = iddqd::IdOrdMap::new();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: omicron_uuid_kinds::CaseEreportUuid::new_v4(),
                    ereport: Arc::new(fm::Ereport { data: ereport1, reporter }),
                    assigned_sitrep_id: sitrep_id,
                    comment: "this has something to do with case 1".to_string(),
                })
                .unwrap();

            fm::Case {
                id: omicron_uuid_kinds::CaseUuid::new_v4(),
                created_sitrep_id: sitrep_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                ereports,
                comment: "my cool case".to_string(),
            }
        };

        let case2 = {
            let mut ereports = iddqd::IdOrdMap::new();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: omicron_uuid_kinds::CaseEreportUuid::new_v4(),
                    ereport: Arc::new(fm::Ereport { data: ereport2, reporter }),
                    assigned_sitrep_id: sitrep_id,
                    comment: "this has something to do with case 2".to_string(),
                })
                .unwrap();
            fm::Case {
                id: omicron_uuid_kinds::CaseUuid::new_v4(),
                created_sitrep_id: sitrep_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                ereports,
                comment: "break in case of emergency".to_string(),
            }
        };

        let mut cases = iddqd::IdOrdMap::new();
        cases.insert_unique(case1.clone()).expect("failed to insert case 1");
        cases.insert_unique(case2.clone()).expect("failed to insert case 2");
        fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: sitrep_id,
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id,
                comment: "i made this sitrep because i felt like it"
                    .to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
            cases,
        }
    }

    #[tokio::test]
    async fn test_sitrep_cases_roundtrip() {
        let logctx = dev::test_setup_log("test_sitrep_cases_roundtrip");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sitrep = make_sitrep_with_cases(&opctx, &datastore).await;
        let sitrep_id = sitrep.id();

        datastore
            .fm_sitrep_insert(&opctx, sitrep.clone())
            .await
            .expect("failed to insert sitrep");

        // Read the sitrep back
        let read_sitrep = datastore
            .fm_sitrep_read(&opctx, sitrep_id)
            .await
            .expect("failed to read sitrep");

        // Verify the sitrep metadata matches --- ignore the timestamp.
        assert_eq!(read_sitrep.id(), sitrep.id());
        assert_eq!(read_sitrep.metadata.creator_id, sitrep.metadata.creator_id);
        assert_eq!(read_sitrep.metadata.comment, sitrep.metadata.comment);
        assert_eq!(read_sitrep.metadata.parent_sitrep_id, None);

        // Verify all the expected cases were read back
        for case in &read_sitrep.cases {
            let fm::Case {
                id,
                created_sitrep_id,
                closed_sitrep_id,
                comment,
                de,
                ereports,
            } = dbg!(case);
            let Some(expected) = sitrep.cases.get(&case.id) else {
                panic!("expected case {id} to exist in the original sitrep")
            };
            // N.B.: we must assert each bit of the case manually, as ereports
            // contain `time_collected` timestamps which will lose a bit of
            // precision when roundtripped through the database.
            // :(
            assert_eq!(id, &expected.id);
            assert_eq!(created_sitrep_id, &expected.created_sitrep_id);
            assert_eq!(closed_sitrep_id, &expected.closed_sitrep_id);
            assert_eq!(comment, &expected.comment);
            assert_eq!(de, &expected.de);
            for expected in &expected.ereports {
                let Some(ereport) = ereports.get(&expected.ereport.id()) else {
                    panic!(
                        "expected ereport {id} to exist in the original case"
                    )
                };
                let fm::case::CaseEreport {
                    id,
                    ereport,
                    assigned_sitrep_id,
                    comment,
                } = dbg!(ereport);
                assert_eq!(id, &expected.id);
                // This is where we go out of our way to avoid the timestamp,
                // btw.
                assert_eq!(ereport.id(), expected.ereport.id());
                assert_eq!(assigned_sitrep_id, &expected.assigned_sitrep_id);
                assert_eq!(comment, &expected.comment);
            }
            eprintln!();
        }

        // Clean up
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sitrep_delete_deletes_cases() {
        let logctx = dev::test_setup_log("test_sitrep_delete_deletes_cases");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sitrep = make_sitrep_with_cases(&opctx, &datastore).await;
        let sitrep_id = sitrep.id();

        datastore
            .fm_sitrep_insert(&opctx, sitrep.clone())
            .await
            .expect("failed to insert sitrep");

        // Verify the sitrep, cases, and ereport assignments exist
        let conn = db
            .datastore()
            .pool_connection_authorized(&opctx)
            .await
            .expect("failed to get connection");

        let sitreps_before: i64 = sitrep_dsl::fm_sitrep
            .filter(sitrep_dsl::id.eq(sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .expect("failed to count sitreps before deletion");
        assert_eq!(sitreps_before, 1, "sitrep should exist before deletion");

        let cases_before: i64 = case_dsl::fm_case
            .filter(case_dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .expect("failed to count cases before deletion");
        assert_eq!(cases_before, 2, "two cases should exist before deletion");

        let case_ereports_before: i64 = case_ereport_dsl::fm_ereport_in_case
            .filter(
                case_ereport_dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()),
            )
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .expect("failed to count case ereports before deletion");
        assert_eq!(
            case_ereports_before, 2,
            "two case ereport assignments should exist before deletion"
        );

        // Now delete the sitrep
        let deleted_count = datastore
            .fm_sitrep_delete_all(&opctx, vec![sitrep_id])
            .await
            .expect("failed to delete sitrep");
        assert_eq!(deleted_count, 1, "should have deleted 1 sitrep");

        // Check that the sitrep and all the cases and associated records no
        // longer exist after deletion.
        let sitreps_after: i64 = sitrep_dsl::fm_sitrep
            .filter(sitrep_dsl::id.eq(sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .expect("failed to count sitreps after deletion");
        assert_eq!(sitreps_after, 0, "sitrep should not exist after deletion");

        let cases_after: i64 = case_dsl::fm_case
            .filter(case_dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .expect("failed to count cases after deletion");
        assert_eq!(cases_after, 0, "cases should not exist after deletion");

        let case_ereports_after: i64 = case_ereport_dsl::fm_ereport_in_case
            .filter(
                case_ereport_dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()),
            )
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .expect("failed to count case ereports after deletion");
        assert_eq!(
            case_ereports_after, 0,
            "case ereport assignments should not exist after deletion"
        );

        // Clean up
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
