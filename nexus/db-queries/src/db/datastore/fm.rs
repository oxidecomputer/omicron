// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on fault management internal data, such as situation
//! reports (sitreps).
//!
//! See [RFD 603](https://rfd.shared.oxide.computer/rfd/0603) for details on the
//! fault management sitrep, and the [datastore module documentation](super) for
//! general conventions.

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
use nexus_db_schema::schema::fm_alert_request::dsl as alert_req_dsl;
use nexus_db_schema::schema::fm_case::dsl as case_dsl;
use nexus_db_schema::schema::fm_ereport_in_case::dsl as case_ereport_dsl;
use nexus_db_schema::schema::fm_sitrep::dsl as sitrep_dsl;
use nexus_db_schema::schema::fm_sitrep_history::dsl as history_dsl;
use nexus_types::fm;
use nexus_types::fm::Sitrep;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::AlertKind;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CaseEreportKind;
use omicron_uuid_kinds::CaseKind;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SitrepUuid;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Declares the [`SitrepChildTable`] enum and its associated table/column
/// metadata. To add a new child table to the sitrep GC, just add a line within
/// the call to "sitrep_child_tables!" below — the GC loop, omdb display,
/// and completeness test all adapt automatically.
///
/// Syntax:
/// ```ignore
/// sitrep_child_tables! {
///     // The sitrep ID column defaults to "sitrep_id"
///     MyTable => { table: "fm_my_table" },
///     // Override the column name if needed:
///     OtherTable => { table: "fm_other_table", sitrep_id: "sitrep_col" },
/// }
/// ```
macro_rules! sitrep_child_tables {
    ($(
        $(#[$meta:meta])*
        $variant:ident => { table: $table:literal $(, sitrep_id: $col:literal)? }
    ),* $(,)?) => {
        /// Identifies a child table of `fm_sitrep` for use in
        /// orphan-deletion queries. Using an enum (rather than raw strings)
        /// ensures the table and column names are known at compile time,
        /// preventing SQL injection.
        #[derive(
            Clone, Copy, Debug,
            PartialEq, Eq, PartialOrd, Ord,
            strum::VariantArray,
        )]
        pub enum SitrepChildTable {
            $( $(#[$meta])* $variant, )*
        }

        impl SitrepChildTable {
            pub const ALL: &[SitrepChildTable] =
                <Self as strum::VariantArray>::VARIANTS;

            pub const fn table_name(&self) -> &'static str {
                match self { $( Self::$variant => $table, )* }
            }

            pub(crate) const fn sitrep_id_column(&self) -> &'static str {
                match self {
                    $( Self::$variant =>
                        sitrep_child_tables!(@sitrep_id $($col)?),
                    )*
                }
            }
        }
    };

    // Default column name when none is specified.
    (@sitrep_id) => { "sitrep_id" };
    // Explicit column name override.
    (@sitrep_id $col:literal) => { $col };
}

sitrep_child_tables! {
    CaseEreport => { table: "fm_ereport_in_case" },
    AlertRequest => { table: "fm_alert_request" },
    Case => { table: "fm_case" },
}

/// Per-child-table statistics from a single GC pass.
#[derive(Clone, Copy, Debug, Default)]
pub struct ChildTableGcStats {
    pub rows_deleted: usize,
    pub batches: usize,
}

/// Result of [`DataStore::fm_sitrep_gc_orphans`], containing the number of
/// rows deleted from each table.
pub struct GcOrphansResult {
    pub sitreps_deleted: usize,
    pub sitrep_metadata_batches: usize,
    pub batch_size: u32,
    pub child_tables: BTreeMap<SitrepChildTable, ChildTableGcStats>,
}

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
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .map(Into::into);
        Ok(version)
    }

    async fn fm_current_sitrep_version_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<model::SitrepVersion>, DieselError> {
        history_dsl::fm_sitrep_history
            .order_by(history_dsl::version.desc())
            .select(model::SitrepVersion::as_select())
            .first_async(conn)
            .await
            .optional()
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
        loop {
            let version =
                self.fm_current_sitrep_version_on_conn(&conn).await.map_err(
                    |e| public_error_from_diesel(e, ErrorHandler::Server),
                )?;
            let version: fm::SitrepVersion = match version {
                Some(version) => version.into(),
                // If there is no current sitrep version, that means no sitreps
                // exist; return `None`.
                None => return Ok(None),
            };
            match self.fm_sitrep_read_on_conn(version.id, &conn).await {
                Ok(sitrep) => return Ok(Some((version, sitrep))),
                // If `fm_sitrep_read_on_conn` returns `NotFound` for a sitrep
                // ID that was returned by `fm_current_sitrep_version_on_conn`,
                // this means that the sitrep we were attempting to read is no
                // longer the current sitrep. There must, therefore, be a *new*
                // current sitrep. This is because:
                //
                // - The `fm_sitrep_delete_all` query does not permit the
                //   current sitrep to be deleted, and,
                // - If a sitrep is the current sitrep, it will no longer be
                //   the current sitrep if and only if a new current sitrep has
                //   been inserted.
                //
                // Therefore, we can just retry, loading the current version
                // again and trying to read the new current sitrep.
                //
                // This is a fairly unlikely situation, but it could occur if
                // there is a particularly long delay between when we read the
                // current version and when we attempt to actually load that
                // sitrep, so we ought to handle it here. It would be incorrect
                // to just return `None` in this case, as `None` means that *no
                // current sitrep has ever been created*.
                Err(e @ Error::NotFound { .. }) => {
                    slog::debug!(
                        opctx.log,
                        "attempted to read current sitrep {}, but it seems to
                         have been deleted out from under us! retrying...",
                        version.id;
                        "sitrep_id" => ?version.id,
                        "sitrep_version" => ?version.version,
                        "error" => %e,
                    );
                    continue;
                }
                // Propagate any unanticipated errors.
                Err(e) => return Err(e),
            }
        }
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
        let mut ereports = iddqd::IdOrdMap::<Arc<fm::Ereport>>::new();
        let mut case_ereports = {
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
                    map.entry(case_id)
                        .or_default()
                        .insert_unique(fm::case::CaseEreport {
                            id,
                            ereport,
                            assigned_sitrep_id: assignment
                                .assigned_sitrep_id
                                .into(),
                            comment: assignment.comment,
                        })
                        .map_err(|_| {
                            let internal_message = format!(
                                "encountered multiple case ereports for case \
                                 {case_id} with the same UUID {id}. this \
                                 should really not be possible, as the \
                                 assignment UUID is a primary key!",
                            );
                            Error::InternalError { internal_message }
                        })?;
                }
            }

            map
        };

        // Next, fetch all the alert requests belonging to cases in this sitrep.
        // We do this in one big batched query that's paginated by case ID,
        // rather than by querying alert requests per case, since this will
        // generally result in fewer queries overall (most cases will have less
        // than SQL_BATCH_SIZE alerts in them).
        let mut alert_requests = {
            let mut by_case = HashMap::<
                CaseUuid,
                iddqd::IdOrdMap<fm::case::AlertRequest>,
            >::new();

            let mut paginator: Paginator<DbTypedUuid<AlertKind>> =
                Paginator::new(SQL_BATCH_SIZE, PaginationOrder::Descending);
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    alert_req_dsl::fm_alert_request,
                    alert_req_dsl::id,
                    &p.current_pagparams(),
                )
                .filter(alert_req_dsl::sitrep_id.eq(id.into_untyped_uuid()))
                .select(model::fm::AlertRequest::as_select())
                .load_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context(
                            "failed to load case alert requests assignments",
                        )
                })?;

                paginator = p.found_batch(&batch, &|alert_req| alert_req.id);
                for alert_req in batch {
                    let case_id = alert_req.case_id.into();
                    let id: AlertUuid = alert_req.id.into();
                    by_case
                        .entry(case_id)
                        .or_default()
                        .insert_unique(alert_req.into())
                        .map_err(|_| {
                            let internal_message = format!(
                                "encountered multiple alert requests for case \
                                 {case_id} with the same alert UUID {id}. \
                                 this should really not be possible, as the \
                                 alert UUID is a primary key!",
                            );
                            Error::InternalError { internal_message }
                        })?;
                }
            }

            by_case
        };

        // Next, load the case metadata entries and marry them to the sets of
        // ereports and alert requests for to those cases that we loaded in the
        // previous steps.
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
                    let alerts_requested =
                        alert_requests.remove(&id).unwrap_or_default();
                    fm::Case {
                        id,
                        created_sitrep_id: created_sitrep_id.into(),
                        closed_sitrep_id: closed_sitrep_id.map(Into::into),
                        de: de.into(),
                        comment,
                        ereports,
                        alerts_requested,
                        support_bundles_requested: iddqd::IdOrdMap::new(),
                    }
                }));
            }

            cases
        };

        // Finally, fetch the sitrep's metadata from the `fm_sitrep` table. We
        // load this record last, because if a concurrent delete operation has
        // started, we will observe that the top-level metadata record has been
        // deleted, and return `NotFound`. This prevents us from returning a
        // potentially torn sitrep where child records were deleted after
        // loading the metadata record.
        //
        // See https://github.com/oxidecomputer/omicron/issues/9594 for details.
        let metadata =
            self.fm_sitrep_metadata_read_on_conn(id, &conn).await?.into();

        Ok(Sitrep { metadata, cases, ereports_by_id: ereports })
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
        // NOTE: we must insert this record before anything else. The GC's
        // "deeply orphaned" cleanup deletes child rows whose sitrep_id
        // has no corresponding fm_sitrep row. If we inserted children
        // *before* metadata, a concurrent GC run could incorrectly delete
        // those in-progress children. Inserting metadata first ensures
        // the children's sitrep_id exists in fm_sitrep, protecting them.
        //
        // This protection is temporary: if our parent becomes stale
        // before we finish inserting (e.g. another writer supersedes it),
        // the GC may delete our metadata row — and subsequently our
        // children — while we're still inserting them. This is harmless:
        // the insert will fail with ParentNotCurrent at the end, and
        // any rows GC didn't already clean up will be collected on the
        // next pass.
        //
        // See https://github.com/oxidecomputer/omicron/issues/10131 for
        // details.
        diesel::insert_into(sitrep_dsl::fm_sitrep)
            .values(model::SitrepMetadata::from(sitrep.metadata))
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to insert sitrep metadata record")
            })?;

        // Create case records.
        //
        // We do this by collecting all the records for cases into big `Vec`s
        // and inserting each category of case records in one big INSERT query,
        // rather than doing smaller ones for each case in the sitrep. This uses
        // more memory in Nexus but reduces the number of small db queries we
        // perform.
        //
        // The ordering of inserts among case child records (ereports, alert
        // requests) and case metadata doesn't matter: there are no foreign key
        // constraints between these tables, and garbage collection is keyed on
        // sitrep_id (which is inserted first above). If we crash partway
        // through, orphaned child records will be cleaned up when the orphaned
        // sitrep is garbage collected.
        let mut cases = Vec::with_capacity(sitrep.cases.len());
        let mut alerts_requested = Vec::new();
        let mut case_ereports = Vec::new();
        for case in sitrep.cases {
            let case_id = case.id;
            cases.push(model::fm::CaseMetadata::from_sitrep(sitrep_id, &case));
            case_ereports.extend(case.ereports.into_iter().map(|ereport| {
                model::fm::CaseEreport::from_sitrep(sitrep_id, case_id, ereport)
            }));
            alerts_requested.extend(case.alerts_requested.into_iter().map(
                |req| {
                    model::fm::AlertRequest::from_sitrep(
                        sitrep_id, case_id, req,
                    )
                },
            ));
        }

        if !case_ereports.is_empty() {
            diesel::insert_into(case_ereport_dsl::fm_ereport_in_case)
                .values(case_ereports)
                .execute_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context(
                            "failed to insert case ereport assignments",
                        )
                })?;
        }

        if !alerts_requested.is_empty() {
            diesel::insert_into(alert_req_dsl::fm_alert_request)
                .values(alerts_requested)
                .execute_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context("failed to insert alert requests")
                })?;
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
                    // Note: if our parent became stale during insertion,
                    // a concurrent GC pass may have already deleted the
                    // metadata and child rows we inserted above (since
                    // they appear orphaned). This is harmless — if GC
                    // hasn't run yet, the rows will be cleaned up on the
                    // next pass.
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

    /// Deletes all sitreps with the provided IDs.
    #[cfg(test)]
    async fn fm_sitrep_delete_all(
        &self,
        opctx: &OpContext,
        ids: impl IntoIterator<Item = SitrepUuid>,
    ) -> Result<usize, Error> {
        use nexus_db_errors::OptionalError;
        use nexus_db_errors::TransactionError;

        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO(eliza): there should probably be an authz object for the fm sitrep?
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let ids = ids
            .into_iter()
            .map(|id| id.into_untyped_uuid())
            .collect::<Vec<_>>();

        struct SitrepDeleteResult {
            sitreps_deleted: usize,
            case_ereports_deleted: usize,
            alert_requests_deleted: usize,
            cases_deleted: usize,
        }

        let err = OptionalError::new();
        let SitrepDeleteResult {
            sitreps_deleted,
            case_ereports_deleted,
            alert_requests_deleted,
            cases_deleted,
        } = self
            // Sitrep deletion is transactional to prevent a sitrep from being
            // left in a partially-deleted state should the Nexus instance
            // attempting the delete operation die suddenly.
            .transaction_retry_wrapper("fm_sitrep_delete_all")
            .transaction(&conn, |conn| {
                let ids = ids.clone();
                let err = err.clone();
                async move {
                    // First, ensure that we are not deleting the current
                    // sitrep, and bail out if we would.
                    if let Some(model::SitrepVersion { sitrep_id, .. }) =
                        self.fm_current_sitrep_version_on_conn(&conn).await?
                    {
                        if ids.contains(&sitrep_id.as_untyped_uuid()) {
                            return Err(err.bail(TransactionError::CustomError(Error::conflict(format!(
                                "cannot delete sitrep {sitrep_id}, as it is the current sitrep"
                            )))));
                        }
                    }

                    // Delete case ereport assignments
                    let case_ereports_deleted = diesel::delete(
                        case_ereport_dsl::fm_ereport_in_case.filter(
                            case_ereport_dsl::sitrep_id.eq_any(ids.clone()),
                        ),
                    )
                    .execute_async(&conn)
                    .await?;

                    // Delete case alert requests.
                    let alert_requests_deleted = diesel::delete(
                        alert_req_dsl::fm_alert_request.filter(alert_req_dsl::sitrep_id.eq_any(ids.clone()))
                    )
                    .execute_async(&conn)
                    .await?;

                    // Delete case metadata records.
                    let cases_deleted = diesel::delete(
                        case_dsl::fm_case
                            .filter(case_dsl::sitrep_id.eq_any(ids.clone())),
                    )
                    .execute_async(&conn)
                    .await?;

                    // Delete sitrep metadata records.
                    let sitreps_deleted = diesel::delete(
                        sitrep_dsl::fm_sitrep
                            .filter(sitrep_dsl::id.eq_any(ids.clone())),
                    )
                    .execute_async(&conn)
                    .await?;

                    Ok(SitrepDeleteResult {
                        sitreps_deleted,
                        cases_deleted,
                        alert_requests_deleted,
                        case_ereports_deleted,
                    })
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })?;

        slog::info!(
            &opctx.log,
            "deleted {sitreps_deleted} of {} sitreps", ids.len();
            "ids" => ?ids,
            "sitreps_deleted" => sitreps_deleted,
            "cases_deleted" => cases_deleted,
            "case_ereports_deleted" => case_ereports_deleted,
            "alert_requests_deleted" => alert_requests_deleted,
        );

        Ok(sitreps_deleted)
    }

    /// Garbage-collects orphaned sitrep data in two phases:
    ///
    /// 1. Deletes orphaned `fm_sitrep` metadata rows (not in history,
    ///    stale parent).
    /// 2. Deletes child rows from each child table that are not referenced
    ///    by sitreps (their `sitrep_id` doesn't exist in `fm_sitrep`).
    ///    This catches children of sitreps deleted in step 1 AND children
    ///    leaked by the race in
    ///    <https://github.com/oxidecomputer/omicron/issues/10131>.
    ///
    /// Each phase runs as a series of individual paginated queries (no
    /// wrapping transaction). This is safe because readers
    /// ([`DataStore::fm_sitrep_read_on_conn`]) load children first and metadata
    /// **last**: once a metadata row is deleted and committed in step 1,
    /// any concurrent reader gets `NotFound` rather than a torn sitrep.
    /// See <https://github.com/oxidecomputer/omicron/issues/9594>.
    ///
    /// Both phases are paginated by `sitrep_id` to avoid full table scans.
    pub async fn fm_sitrep_gc_orphans(
        &self,
        opctx: &OpContext,
    ) -> Result<GcOrphansResult, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO(eliza): there should probably be an authz object for the fm sitrep?
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // Step 1: Delete orphaned fm_sitrep metadata rows.
        let mut sitreps_deleted = 0usize;
        let mut sitrep_metadata_batches = 0usize;
        {
            let mut marker = SitrepUuid::nil();
            loop {
                sitrep_metadata_batches += 1;
                let (deleted, next_marker) =
                    Self::delete_orphaned_sitrep_metadata_query(
                        marker,
                        SQL_BATCH_SIZE,
                    )
                    .get_result_async::<(i64, Option<Uuid>)>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;
                sitreps_deleted += deleted as usize;

                match next_marker {
                    Some(m) => marker = SitrepUuid::from_untyped_uuid(m),
                    None => break,
                }
            }
        }

        // Step 2: For each child table, paginate through and
        // delete rows whose sitrep_id no longer exists in
        // fm_sitrep.
        let mut child_tables = BTreeMap::new();
        for &table in SitrepChildTable::ALL {
            let stats = child_tables
                .entry(table)
                .or_insert(ChildTableGcStats::default());
            let mut marker = SitrepUuid::nil();
            loop {
                stats.batches += 1;
                let (rows_deleted, next_marker) =
                    Self::deeply_orphaned_batch_query(
                        table,
                        marker,
                        SQL_BATCH_SIZE,
                    )
                    .get_result_async::<(i64, Option<Uuid>)>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;
                stats.rows_deleted += rows_deleted as usize;

                match next_marker {
                    Some(m) => {
                        marker = SitrepUuid::from_untyped_uuid(m);
                    }
                    None => break,
                }
            }
        }

        let result = GcOrphansResult {
            sitreps_deleted,
            sitrep_metadata_batches,
            batch_size: SQL_BATCH_SIZE.get(),
            child_tables,
        };

        slog::info!(
            &opctx.log,
            "sitrep GC completed";
            "sitreps_deleted" => result.sitreps_deleted,
            "child_tables" => ?result.child_tables,
        );

        Ok(result)
    }

    /// Builds a DELETE query that removes orphaned `fm_sitrep` metadata
    /// rows in a single paginated batch (sitreps not in history whose
    /// parent is not current). Returns (rows_deleted, next_marker).
    ///
    /// The SQL text of this query is in
    /// `nexus/db-queries/tests/output/delete_orphaned_sitrep_metadata.sql`.
    fn delete_orphaned_sitrep_metadata_query(
        marker: SitrepUuid,
        batch_size: std::num::NonZeroU32,
    ) -> TypedSqlQuery<(sql_types::Int8, sql_types::Nullable<sql_types::Uuid>)>
    {
        let mut builder = QueryBuilder::new();
        builder.sql(
            "WITH current_sitrep_id AS (\
                SELECT sitrep_id \
                FROM omicron.public.fm_sitrep_history \
                ORDER BY version DESC \
                LIMIT 1\
            ), \
            batch AS (\
                SELECT s.id \
                FROM omicron.public.fm_sitrep s \
                LEFT JOIN omicron.public.fm_sitrep_history h \
                    ON h.sitrep_id = s.id \
                WHERE \
                    h.sitrep_id IS NULL \
                AND s.id > ",
        );
        builder.param().bind::<sql_types::Uuid, _>(marker.into_untyped_uuid());
        builder.sql(
            " AND (\
                    (\
                        s.parent_sitrep_id IS NULL AND \
                        (SELECT sitrep_id FROM current_sitrep_id) IS NOT NULL\
                    ) \
                    OR s.parent_sitrep_id != (\
                        SELECT sitrep_id FROM current_sitrep_id\
                    )\
                ) \
                ORDER BY s.id \
                LIMIT ",
        );
        builder.param().bind::<sql_types::Int8, _>(i64::from(batch_size.get()));
        builder.sql(
            "), \
            deleted AS (\
                DELETE FROM omicron.public.fm_sitrep \
                WHERE id IN (SELECT id FROM batch) \
                RETURNING id\
            ) \
            SELECT \
                (SELECT COUNT(*) FROM deleted) AS rows_deleted, \
                CASE WHEN (SELECT COUNT(*) FROM batch) >= ",
        );
        builder.param().bind::<sql_types::Int8, _>(i64::from(batch_size.get()));
        builder.sql(
            " THEN (SELECT MAX(id) FROM batch) \
                ELSE NULL END AS next_marker",
        );
        builder.query()
    }

    /// Builds a single-round-trip query that:
    /// 1. Finds a batch of distinct `sitrep_id` values in the child table
    /// 2. Deletes rows whose `sitrep_id` has no corresponding `fm_sitrep`
    /// 3. Returns (rows_deleted, next_marker) where next_marker is NULL
    ///    when there are no more pages
    ///
    /// The SQL text of this query is in
    /// `nexus/db-queries/tests/output/deeply_orphaned_batch_query.sql`.
    fn deeply_orphaned_batch_query(
        table: SitrepChildTable,
        marker: SitrepUuid,
        batch_size: std::num::NonZeroU32,
    ) -> TypedSqlQuery<(sql_types::Int8, sql_types::Nullable<sql_types::Uuid>)>
    {
        let tbl = table.table_name();
        let col = table.sitrep_id_column();
        let mut builder = QueryBuilder::new();

        // batch: paginated scan of distinct sitrep_ids
        builder.sql("WITH batch AS (SELECT DISTINCT ");
        builder.sql(col);
        builder.sql(" FROM omicron.public.");
        builder.sql(tbl);
        builder
            .sql(" WHERE ")
            .sql(col)
            .sql(" > ")
            .param()
            .bind::<sql_types::Uuid, _>(marker.into_untyped_uuid());
        builder.sql(" ORDER BY ");
        builder.sql(col);
        builder
            .sql(" LIMIT ")
            .param()
            .bind::<sql_types::Int8, _>(i64::from(batch_size.get()));

        // deleted: delete deeply-orphaned rows in this batch
        builder.sql("), deleted AS (DELETE FROM omicron.public.");
        builder.sql(tbl);
        builder.sql(" WHERE ");
        builder.sql(col);
        builder.sql(
            " IN (\
                SELECT b.",
        );
        builder.sql(col);
        builder.sql(
            " FROM batch b \
            LEFT JOIN omicron.public.fm_sitrep s ON s.id = b.",
        );
        builder.sql(col);
        builder.sql(" WHERE s.id IS NULL) RETURNING ");
        builder.sql(col);

        // Final SELECT: return deleted count + next marker
        builder.sql(
            ") SELECT \
                (SELECT COUNT(*) FROM deleted) AS rows_deleted, \
                CASE WHEN (SELECT COUNT(*) FROM batch) >= ",
        );
        builder.param().bind::<sql_types::Int8, _>(i64::from(batch_size.get()));
        builder.sql(" THEN (SELECT MAX(");
        builder.sql(col);
        builder.sql(") FROM batch) ELSE NULL END AS next_marker");
        builder.query()
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
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::QueryBuilder;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use chrono::Utc;
    use diesel::pg::Pg;
    use ereport_types;
    use nexus_types::alert::AlertClass;
    use nexus_types::fm;
    use nexus_types::fm::ereport::{EreportData, Reporter};
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use std::collections::BTreeMap;
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

    #[tokio::test]
    async fn expectorate_deeply_orphaned_batch_query() {
        let query = DataStore::deeply_orphaned_batch_query(
            SitrepChildTable::CaseEreport,
            SitrepUuid::nil(),
            SQL_BATCH_SIZE,
        );
        expectorate_query_contents(
            &query,
            "tests/output/deeply_orphaned_batch_query.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_deeply_orphaned_batch_query() {
        let logctx = dev::test_setup_log("explain_deeply_orphaned_batch_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::deeply_orphaned_batch_query(
            SitrepChildTable::CaseEreport,
            SitrepUuid::nil(),
            SQL_BATCH_SIZE,
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
    async fn expectorate_delete_orphaned_sitrep_metadata() {
        let query = DataStore::delete_orphaned_sitrep_metadata_query(
            SitrepUuid::nil(),
            SQL_BATCH_SIZE,
        );
        expectorate_query_contents(
            &query,
            "tests/output/delete_orphaned_sitrep_metadata.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_delete_orphaned_sitrep_metadata_query() {
        let logctx = dev::test_setup_log(
            "explain_delete_orphaned_sitrep_metadata_query",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::delete_orphaned_sitrep_metadata_query(
            SitrepUuid::nil(),
            SQL_BATCH_SIZE,
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
            ereports_by_id: Default::default(),
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
            ereports_by_id: Default::default(),
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
            ereports_by_id: Default::default(),
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
            ereports_by_id: Default::default(),
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
            ereports_by_id: Default::default(),
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
            ereports_by_id: Default::default(),
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
            ereports_by_id: Default::default(),
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
            ereports_by_id: Default::default(),
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

    /// Assert that two sitreps are equal, skipping timestamp fields in ereports
    /// and sitrep metadata. These timestamps may lose precision when
    /// round-tripping through cockroachdb and may no longer be "equal".
    ///
    /// NOTE FOR FUTURE GENERATIONS: If we add other top-level child records
    /// other than cases, we should also assert that they match here.
    #[track_caller]
    fn assert_sitreps_eq(this: &Sitrep, that: &Sitrep) {
        // Verify the sitrep metadata matches --- ignore the timestamp.
        assert_eq!(this.id(), that.id());
        assert_eq!(this.metadata.creator_id, that.metadata.creator_id);
        assert_eq!(this.metadata.comment, that.metadata.comment);
        assert_eq!(
            this.metadata.parent_sitrep_id,
            that.metadata.parent_sitrep_id
        );

        // Verify all the expected cases exist in both sitreps
        assert_eq!(this.cases.len(), that.cases.len());
        for case in &that.cases {
            let fm::Case {
                id,
                created_sitrep_id,
                closed_sitrep_id,
                comment,
                de,
                ereports,
                alerts_requested,
                support_bundles_requested: _,
            } = case;
            let case_id = id;
            let Some(expected) = this.cases.get(&case_id) else {
                panic!(
                    "assertion failed: left == right\n  \
                    right sitrep contained case {case_id}\n  \
                    left sitrep contains only cases {:?}\n",
                    this.cases.iter().map(|case| case.id).collect::<Vec<_>>(),
                )
            };
            // N.B.: we must assert each bit of the case manually, as ereports
            // contain `time_collected` timestamps which will lose a bit of
            // precision when roundtripped through the database.
            // :(
            assert_eq!(&expected.id, id, "while checking case {case_id}");
            assert_eq!(
                &expected.created_sitrep_id, created_sitrep_id,
                "while checking case {case_id}"
            );
            assert_eq!(
                &expected.closed_sitrep_id, closed_sitrep_id,
                "while checking case {case_id}"
            );
            assert_eq!(
                &expected.comment, comment,
                "while checking case {case_id}"
            );
            assert_eq!(&expected.de, de, "while checking case {case_id}");

            // Now, check that all the ereports are present in both cases.
            assert_eq!(ereports.len(), expected.ereports.len());
            for expected in &expected.ereports {
                let ereport_id = expected.ereport.id();
                let Some(ereport) = ereports.get(&ereport_id) else {
                    panic!(
                        "assertion failed: left == right (while checking case {case_id})\n  \
                        case in right sitrep did not contain ereport {ereport_id}\n  \
                        it contains only these ereports: {:?}\n",
                        ereports
                            .iter()
                            .map(|e| e.ereport.id().to_string())
                            .collect::<Vec<_>>(),
                    )
                };
                let fm::case::CaseEreport {
                    id,
                    ereport,
                    assigned_sitrep_id,
                    comment,
                } = ereport;
                assert_eq!(
                    &expected.id, id,
                    "ereport assignment IDs should be equal \
                     (while checking ereport {ereport_id} in case {case_id})",
                );
                // This is where we go out of our way to avoid the timestamp,
                // btw.
                assert_eq!(
                    expected.ereport.id(),
                    ereport.id(),
                    "while checking ereport {ereport_id} in case {case_id}",
                );
                assert_eq!(
                    &expected.assigned_sitrep_id, assigned_sitrep_id,
                    "while checking ereport {ereport_id} in case {case_id}",
                );
                assert_eq!(
                    &expected.comment, comment,
                    "while checking ereport {ereport_id} in case {case_id}",
                );
            }

            // Since these don't have any timestamps in them, we can just assert
            // the whole map is the same.
            assert_eq!(
                alerts_requested, &expected.alerts_requested,
                "while checking case {case_id}"
            );
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
                    ereport: Arc::new(fm::Ereport::new(ereport1, reporter)),
                    assigned_sitrep_id: sitrep_id,
                    comment: "this has something to do with case 1".to_string(),
                })
                .unwrap();

            let mut alerts_requested = iddqd::IdOrdMap::new();
            alerts_requested
                .insert_unique(fm::case::AlertRequest {
                    id: AlertUuid::new_v4(),
                    class: AlertClass::TestFoo,
                    payload: serde_json::json!({}),
                    requested_sitrep_id: sitrep_id,
                })
                .unwrap();
            alerts_requested
                .insert_unique(fm::case::AlertRequest {
                    id: AlertUuid::new_v4(),
                    class: AlertClass::TestFooBar,
                    payload: serde_json::json!({}),
                    requested_sitrep_id: sitrep_id,
                })
                .unwrap();

            fm::Case {
                id: omicron_uuid_kinds::CaseUuid::new_v4(),
                created_sitrep_id: sitrep_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                ereports,
                alerts_requested,
                support_bundles_requested: iddqd::IdOrdMap::new(),
                comment: "my cool case".to_string(),
            }
        };

        let case2 = {
            let mut ereports = iddqd::IdOrdMap::new();
            ereports
                .insert_unique(fm::case::CaseEreport {
                    id: omicron_uuid_kinds::CaseEreportUuid::new_v4(),
                    ereport: Arc::new(fm::Ereport::new(ereport2, reporter)),
                    assigned_sitrep_id: sitrep_id,
                    comment: "this has something to do with case 2".to_string(),
                })
                .unwrap();

            let mut alerts_requested = iddqd::IdOrdMap::new();
            alerts_requested
                .insert_unique(fm::case::AlertRequest {
                    id: AlertUuid::new_v4(),
                    class: AlertClass::TestQuuxBar,
                    payload: serde_json::json!({}),
                    requested_sitrep_id: sitrep_id,
                })
                .unwrap();

            fm::Case {
                id: omicron_uuid_kinds::CaseUuid::new_v4(),
                created_sitrep_id: sitrep_id,
                closed_sitrep_id: None,
                de: fm::DiagnosisEngineKind::PowerShelf,
                ereports,
                alerts_requested,
                support_bundles_requested: iddqd::IdOrdMap::new(),
                comment: "break in case of emergency".to_string(),
            }
        };

        let mut cases = iddqd::IdOrdMap::new();
        cases.insert_unique(case1.clone()).expect("failed to insert case 1");
        cases.insert_unique(case2.clone()).expect("failed to insert case 2");
        let mut ereports_by_id = iddqd::IdOrdMap::new();
        for case in cases.iter() {
            ereports_by_id
                .extend(case.ereports.iter().map(|ce| ce.ereport.clone()));
        }
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
            ereports_by_id,
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

        assert_sitreps_eq(&sitrep, &read_sitrep);

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

        // Note that we must also insert a second sitrep which is a child of the
        // sitrep we intend to delete, as the sitrep insert operation makes a
        // sitrep the current sitrep, and a sitrep cannot be deleted if it is
        // current.
        datastore
            .fm_sitrep_insert(
                opctx,
                fm::Sitrep {
                    metadata: fm::SitrepMetadata {
                        parent_sitrep_id: Some(sitrep_id),
                        id: SitrepUuid::new_v4(),
                        time_created: Utc::now(),
                        creator_id: OmicronZoneUuid::new_v4(),
                        comment: "my cool sitrep".to_string(),
                        inv_collection_id: CollectionUuid::new_v4(),
                    },
                    cases: Default::default(),
                    ereports_by_id: Default::default(),
                },
            )
            .await
            .expect("failed to insert second sitrep");

        // Verify the sitrep, cases, and ereport assignments exist
        let conn = datastore.pool_connection_for_tests().await.unwrap();

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

        let alert_requests_before: i64 = alert_req_dsl::fm_alert_request
            .filter(alert_req_dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .expect("failed to count alert requests before deletion");
        assert_eq!(
            alert_requests_before, 3,
            "three alert requests should exist before deletion"
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

        let alert_requests_after: i64 = alert_req_dsl::fm_alert_request
            .filter(alert_req_dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .expect("failed to count alert requests before deletion");
        assert_eq!(
            alert_requests_after, 0,
            "alert requests should not exist after deletion"
        );

        // Clean up
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Test that concurrent read and delete operations on sitreps collections do
    // not result in torn reads. With the fix for issue #9594, fm_sitrep_read
    // checks for the top-level sitrep record at the END of reading, so if a
    // concurrent delete has started (which deletes the top-level record first),
    // the read will fail rather than returning partial data.
    //
    // This test spawns concurrent readers and a deleter to exercise the race
    // condition. Readers should either get the complete original collection
    // OR an error - never partial/torn data.
    #[tokio::test]
    async fn test_concurrent_sitrep_read_delete() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        const TEST_NAME: &str = "test_concurrent_sitrep_read_delete";
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a sitrep and insert it.
        let sitrep = make_sitrep_with_cases(opctx, datastore).await;
        let sitrep_id = sitrep.metadata.id;
        datastore
            .fm_sitrep_insert(opctx, sitrep.clone())
            .await
            .expect("failed to insert sitrep");

        // Verify we can read it back correctly
        let read_back = datastore
            .fm_sitrep_read(&opctx, sitrep_id)
            .await
            .expect("failed to read sitrep");
        assert_sitreps_eq(&sitrep, &read_back);

        // Note that we must also insert a second sitrep which is a child of the
        // sitrep we intend to delete, as the sitrep insert operation makes a
        // sitrep the current sitrep, and a sitrep cannot be deleted if it is
        // current.
        datastore
            .fm_sitrep_insert(
                opctx,
                fm::Sitrep {
                    metadata: fm::SitrepMetadata {
                        parent_sitrep_id: Some(sitrep_id),
                        id: SitrepUuid::new_v4(),
                        time_created: Utc::now(),
                        creator_id: OmicronZoneUuid::new_v4(),
                        comment: "my cool sitrep".to_string(),
                        inv_collection_id: CollectionUuid::new_v4(),
                    },
                    cases: Default::default(),
                    ereports_by_id: Default::default(),
                },
            )
            .await
            .expect("failed to insert second sitrep");

        // Track results from concurrent readers
        let successful_reads = Arc::new(AtomicUsize::new(0));
        let error_count = Arc::new(AtomicUsize::new(0));
        let delete_completed = Arc::new(AtomicBool::new(false));

        // Signal when at least one read has completed, so we know readers are
        // running before we start deleting
        let (first_read_tx, first_read_rx) =
            tokio::sync::oneshot::channel::<()>();
        let first_read_tx =
            Arc::new(std::sync::Mutex::new(Some(first_read_tx)));

        // Spawn reader tasks that loop until deletion completes
        const NUM_READERS: usize = 10;
        let mut reader_handles = Vec::new();

        for n in 0..NUM_READERS {
            let datastore = datastore.clone();
            let opctx = opctx.child(
                std::iter::once(("reader".to_string(), n.to_string()))
                    .collect(),
            );
            let sitrep = sitrep.clone();
            let successful_reads = successful_reads.clone();
            let error_count = error_count.clone();
            let delete_completed = delete_completed.clone();
            let first_read_tx = first_read_tx.clone();

            reader_handles.push(tokio::spawn(async move {
                loop {
                    match datastore.fm_sitrep_read(&opctx, sitrep_id).await {
                        Ok(read_sitrep) => {
                            // If the read sitrep is not equal to the original,
                            // this indicates a torn read!
                            assert_sitreps_eq(&sitrep, &read_sitrep);
                            successful_reads.fetch_add(1, Ordering::Relaxed);

                            // Signal that at least one read completed (only
                            // the first sender to take the channel will send)
                            if let Some(tx) =
                                first_read_tx.lock().unwrap().take()
                            {
                                let _ = tx.send(());
                            }
                        }
                        Err(_) => {
                            // Errors are expected after deletion - the
                            // collection no longer exists. The specific error
                            // varies depending on which query fails first.
                            error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // Stop reading after delete completes
                    if delete_completed.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }));
        }

        // Wait for at least one successful read before deleting, so we know
        // the reader tasks have started
        first_read_rx.await.expect("no reader completed a read");

        // Delete the sitrep while readers are running
        datastore
            .fm_sitrep_delete_all(&opctx, vec![sitrep_id])
            .await
            .expect("failed to delete sitrep");
        delete_completed.store(true, Ordering::Relaxed);

        // Wait for all readers to complete
        for handle in reader_handles {
            handle.await.expect("reader task panicked");
        }

        // Log results for debugging
        let successful = successful_reads.load(Ordering::Relaxed);
        let errors = error_count.load(Ordering::Relaxed);
        eprintln!(
            "Results: {} successful reads, {} errors",
            successful, errors
        );

        // Key invariant: at least one successful read (we waited for this
        // before deleting). Successful reads are validated inside the reader
        // loop - they must match the original sitrep exactly, or the assert_eq!
        // fails indicating a torn read. Errors after deletion are expected and
        // don't need to be categorized.
        assert!(
            successful > 0,
            "Expected at least one successful read (we wait for this)"
        );

        // Verify the sitrep is fully deleted
        match datastore.fm_sitrep_read(&opctx, sitrep_id).await {
            Ok(_sitrep) => panic!("sitrep not deleted"),
            Err(Error::NotFound { message: _ }) => {}
            Err(e) => panic!("unexpected error: {e}"),
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
    /// row doesn't exist) are cleaned up by `fm_sitrep_gc_orphans`.
    #[tokio::test]
    async fn test_gc_deeply_orphaned_children() {
        let logctx = dev::test_setup_log("test_gc_deeply_orphaned_children");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // We need at least one sitrep in history so the GC doesn't think
        // everything is orphaned in a vacuously-true way.
        let sitrep1 = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
            cases: Default::default(),
            ereports_by_id: Default::default(),
        };
        datastore
            .fm_sitrep_insert(opctx, sitrep1)
            .await
            .expect("inserting initial sitrep should succeed");

        // Insert child rows with a sitrep_id that does NOT exist in
        // fm_sitrep. This simulates the race described in issue #10131:
        // the metadata row was GC'd, but the inserter created children
        // after that.
        let ghost_sitrep_id = SitrepUuid::new_v4();
        let ghost_case_id = CaseUuid::new_v4();

        // Insert a deeply-orphaned case.
        diesel::insert_into(case_dsl::fm_case)
            .values(model::fm::CaseMetadata {
                id: ghost_case_id.into(),
                sitrep_id: ghost_sitrep_id.into(),
                de: nexus_types::fm::DiagnosisEngineKind::PowerShelf.into(),
                created_sitrep_id: ghost_sitrep_id.into(),
                closed_sitrep_id: None,
                comment: "deeply orphaned case".to_string(),
            })
            .execute_async(&*conn)
            .await
            .expect("inserting deeply orphaned case");

        // Insert a deeply-orphaned alert request.
        diesel::insert_into(alert_req_dsl::fm_alert_request)
            .values(model::fm::AlertRequest {
                id: AlertUuid::new_v4().into(),
                sitrep_id: ghost_sitrep_id.into(),
                requested_sitrep_id: ghost_sitrep_id.into(),
                case_id: ghost_case_id.into(),
                class: AlertClass::Probe.into(),
                payload: serde_json::json!({}),
            })
            .execute_async(&*conn)
            .await
            .expect("inserting deeply orphaned alert request");

        // Verify the rows exist.
        let cases_before: i64 = case_dsl::fm_case
            .filter(case_dsl::sitrep_id.eq(ghost_sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .unwrap();
        assert_eq!(cases_before, 1, "case should exist before GC");

        let alerts_before: i64 = alert_req_dsl::fm_alert_request
            .filter(
                alert_req_dsl::sitrep_id
                    .eq(ghost_sitrep_id.into_untyped_uuid()),
            )
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .unwrap();
        assert_eq!(alerts_before, 1, "alert request should exist before GC");

        // Verify the ghost sitrep does NOT exist in fm_sitrep.
        let sitrep_exists: i64 = sitrep_dsl::fm_sitrep
            .filter(sitrep_dsl::id.eq(ghost_sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .unwrap();
        assert_eq!(sitrep_exists, 0, "ghost sitrep should NOT exist");

        // Run the unified GC, which should clean up the deeply-orphaned
        // children (and any normal orphans too).
        let result =
            datastore.fm_sitrep_gc_orphans(opctx).await.expect("GC orphans");
        assert_eq!(
            result.child_tables[&SitrepChildTable::Case].rows_deleted,
            1
        );
        assert_eq!(
            result.child_tables[&SitrepChildTable::AlertRequest].rows_deleted,
            1
        );
        assert_eq!(
            result.child_tables[&SitrepChildTable::CaseEreport].rows_deleted,
            0, // we didn't insert any
        );

        // Verify the rows are gone.
        let cases_after: i64 = case_dsl::fm_case
            .filter(case_dsl::sitrep_id.eq(ghost_sitrep_id.into_untyped_uuid()))
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .unwrap();
        assert_eq!(cases_after, 0, "case should be gone after GC");

        let alerts_after: i64 = alert_req_dsl::fm_alert_request
            .filter(
                alert_req_dsl::sitrep_id
                    .eq(ghost_sitrep_id.into_untyped_uuid()),
            )
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .unwrap();
        assert_eq!(alerts_after, 0, "alert request should be gone after GC");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Test that the deeply-orphaned cleanup correctly paginates across
    /// multiple batches when there are more distinct sitrep_ids than the
    /// batch size.
    #[tokio::test]
    async fn test_gc_deeply_orphaned_pagination() {
        let logctx = dev::test_setup_log("test_gc_deeply_orphaned_pagination");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Insert an initial sitrep so the system isn't empty.
        let sitrep1 = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
            cases: Default::default(),
            ereports_by_id: Default::default(),
        };
        datastore
            .fm_sitrep_insert(opctx, sitrep1)
            .await
            .expect("inserting initial sitrep should succeed");

        // Insert deeply-orphaned cases across 5 distinct sitrep_ids.
        // We'll use a batch size of 2, forcing at least 3 batches.
        let num_ghost_sitreps = 5;
        let mut ghost_sitrep_ids = Vec::new();
        for _ in 0..num_ghost_sitreps {
            let ghost_sitrep_id = SitrepUuid::new_v4();
            ghost_sitrep_ids.push(ghost_sitrep_id);
            diesel::insert_into(case_dsl::fm_case)
                .values(model::fm::CaseMetadata {
                    id: CaseUuid::new_v4().into(),
                    sitrep_id: ghost_sitrep_id.into(),
                    de: nexus_types::fm::DiagnosisEngineKind::PowerShelf.into(),
                    created_sitrep_id: ghost_sitrep_id.into(),
                    closed_sitrep_id: None,
                    comment: "deeply orphaned".to_string(),
                })
                .execute_async(&*conn)
                .await
                .expect("inserting deeply orphaned case");
        }

        // Verify all rows exist.
        let cases_before: i64 = case_dsl::fm_case
            .filter(
                case_dsl::sitrep_id.eq_any(
                    ghost_sitrep_ids
                        .iter()
                        .map(|id| id.into_untyped_uuid())
                        .collect::<Vec<_>>(),
                ),
            )
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .unwrap();
        assert_eq!(
            cases_before, num_ghost_sitreps as i64,
            "all ghost cases should exist"
        );

        // Run the deeply-orphaned batch query manually with a small
        // batch size (2) to force pagination across multiple batches.
        let batch_size = std::num::NonZeroU32::new(2).unwrap();
        let mut total_deleted = 0usize;
        let mut marker = SitrepUuid::nil();
        let mut iterations = 0;

        loop {
            let result = DataStore::deeply_orphaned_batch_query(
                SitrepChildTable::Case,
                marker,
                batch_size,
            )
            .get_result_async::<(i64, Option<Uuid>)>(&*conn)
            .await
            .expect("batch query should succeed");

            let (rows_deleted, next_marker) = result;
            total_deleted += rows_deleted as usize;
            iterations += 1;

            match next_marker {
                Some(m) => marker = SitrepUuid::from_untyped_uuid(m),
                None => break,
            }
        }

        assert_eq!(
            total_deleted, num_ghost_sitreps,
            "should have deleted all {num_ghost_sitreps} deeply orphaned cases"
        );
        assert!(
            iterations >= 3,
            "with batch_size=2 and {num_ghost_sitreps} sitreps, \
             should need at least 3 iterations, got {iterations}"
        );

        // Verify all rows are gone.
        let cases_after: i64 = case_dsl::fm_case
            .filter(
                case_dsl::sitrep_id.eq_any(
                    ghost_sitrep_ids
                        .iter()
                        .map(|id| id.into_untyped_uuid())
                        .collect::<Vec<_>>(),
                ),
            )
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .unwrap();
        assert_eq!(cases_after, 0, "all ghost cases should be deleted");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Regression test: with batch_size=1, the old `>=` marker would
    /// infinite-loop on a non-orphaned sitrep_id. With `>`, the loop
    /// must terminate.
    #[tokio::test]
    async fn test_gc_deeply_orphaned_batch_size_one() {
        let logctx =
            dev::test_setup_log("test_gc_deeply_orphaned_batch_size_one");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Insert an initial sitrep so the system isn't empty.
        let sitrep1 = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "test sitrep".to_string(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
            },
            cases: Default::default(),
            ereports_by_id: Default::default(),
        };
        datastore
            .fm_sitrep_insert(opctx, sitrep1)
            .await
            .expect("inserting initial sitrep should succeed");

        // Insert deeply-orphaned cases across 3 distinct sitrep_ids.
        let num_ghost_sitreps = 3;
        let mut ghost_sitrep_ids = Vec::new();
        for _ in 0..num_ghost_sitreps {
            let ghost_sitrep_id = SitrepUuid::new_v4();
            ghost_sitrep_ids.push(ghost_sitrep_id);
            diesel::insert_into(case_dsl::fm_case)
                .values(model::fm::CaseMetadata {
                    id: CaseUuid::new_v4().into(),
                    sitrep_id: ghost_sitrep_id.into(),
                    de: nexus_types::fm::DiagnosisEngineKind::PowerShelf.into(),
                    created_sitrep_id: ghost_sitrep_id.into(),
                    closed_sitrep_id: None,
                    comment: "deeply orphaned".to_string(),
                })
                .execute_async(&*conn)
                .await
                .expect("inserting deeply orphaned case");
        }

        // Run the deeply-orphaned batch query with batch_size=1.
        // This would infinite-loop with the old `>=` marker.
        let batch_size = std::num::NonZeroU32::new(1).unwrap();
        let mut total_deleted = 0usize;
        let mut marker = SitrepUuid::nil();
        let mut iterations = 0;

        loop {
            let result = DataStore::deeply_orphaned_batch_query(
                SitrepChildTable::Case,
                marker,
                batch_size,
            )
            .get_result_async::<(i64, Option<Uuid>)>(&*conn)
            .await
            .expect("batch query should succeed");

            let (rows_deleted, next_marker) = result;
            total_deleted += rows_deleted as usize;
            iterations += 1;

            match next_marker {
                Some(m) => marker = SitrepUuid::from_untyped_uuid(m),
                None => break,
            }
        }

        assert_eq!(
            total_deleted, num_ghost_sitreps,
            "should have deleted all {num_ghost_sitreps} deeply orphaned cases"
        );
        assert_eq!(
            iterations,
            num_ghost_sitreps + 1,
            "with batch_size=1 and {num_ghost_sitreps} ghost sitreps, \
             should need exactly {n} iterations (one per ghost + one \
             final empty batch)",
            n = num_ghost_sitreps + 1,
        );

        // Verify all rows are gone.
        let cases_after: i64 = case_dsl::fm_case
            .filter(
                case_dsl::sitrep_id.eq_any(
                    ghost_sitrep_ids
                        .iter()
                        .map(|id| id.into_untyped_uuid())
                        .collect::<Vec<_>>(),
                ),
            )
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .unwrap();
        assert_eq!(cases_after, 0, "all ghost cases should be deleted");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // SitrepChildTableCounts: queries each table listed in
    // `SitrepChildTable::ALL` to count the number of rows matching a
    // given `sitrep_id`. Used by tests to verify that orphan GC
    // actually cleaned up the expected rows.
    //
    // Mirrors `BlueprintTableCounts` from deployment.rs; the
    // completeness test below ensures every `fm_*` child table with
    // a `sitrep_id` column is covered by `SitrepChildTable`.
    // ---------------------------------------------------------------

    struct SitrepChildTableCounts {
        counts: BTreeMap<String, i64>,
    }

    impl SitrepChildTableCounts {
        /// Query row counts for each child table tracked by
        /// `SitrepChildTable`, for the given `sitrep_id`.
        async fn new(
            datastore: &DataStore,
            sitrep_id: SitrepUuid,
        ) -> SitrepChildTableCounts {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let mut counts = BTreeMap::new();

            for &table in SitrepChildTable::ALL {
                let mut query = QueryBuilder::new();
                query.sql("SELECT COUNT(*) FROM ");
                query.sql(table.table_name());
                query.sql(" WHERE ");
                query.sql(table.sitrep_id_column());
                query.sql(" = ");
                query.param().bind::<diesel::sql_types::Uuid, _>(
                    sitrep_id.into_untyped_uuid(),
                );

                let count: i64 = query
                    .query::<diesel::sql_types::BigInt>()
                    .get_result_async(&*conn)
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to count rows in {}: {e}",
                            table.table_name()
                        )
                    });
                counts.insert(table.table_name().to_string(), count);
            }

            let table_counts = SitrepChildTableCounts { counts };

            // Verify no new fm_* child tables were added without
            // updating SitrepChildTable.
            if let Err(msg) =
                table_counts.verify_all_tables_covered(datastore).await
            {
                panic!("{msg}");
            }

            table_counts
        }

        fn all_empty(&self) -> bool {
            self.counts.values().all(|&count| count == 0)
        }

        fn empty_tables(&self) -> Vec<String> {
            self.counts
                .iter()
                .filter_map(
                    |(table, &count)| {
                        if count == 0 { Some(table.clone()) } else { None }
                    },
                )
                .collect()
        }

        fn non_empty_tables(&self) -> Vec<String> {
            self.counts
                .iter()
                .filter_map(
                    |(table, &count)| {
                        if count > 0 { Some(table.clone()) } else { None }
                    },
                )
                .collect()
        }

        fn tables_checked(&self) -> BTreeSet<&str> {
            self.counts.keys().map(|s| s.as_str()).collect()
        }

        /// Verify no new fm_* tables were added without updating
        /// `SitrepChildTable`.
        async fn verify_all_tables_covered(
            &self,
            datastore: &DataStore,
        ) -> Result<(), String> {
            let conn = datastore.pool_connection_for_tests().await.unwrap();

            // fm_* tables that are NOT children of fm_sitrep and are
            // intentionally ignored.
            let tables_ignored: BTreeSet<&str> =
                ["fm_sitrep", "fm_sitrep_history"].into_iter().collect();
            let tables_checked = self.tables_checked();

            let mut query = QueryBuilder::new();
            query.sql(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_name LIKE 'fm\\_%'",
            );
            let tables_unchecked: Vec<String> = query
                .query::<diesel::sql_types::Text>()
                .load_async(&*conn)
                .await
                .expect("failed to query information_schema for fm_* tables")
                .into_iter()
                .filter(|f: &String| {
                    let t = f.as_str();
                    !tables_ignored.contains(t) && !tables_checked.contains(t)
                })
                .collect();

            if !tables_unchecked.is_empty() {
                Err(format!(
                    "found fm_* child table(s) not covered by \
                     `SitrepChildTable`: {}\n\n\
                     If you added a new fm_* child table, add a variant \
                     to `SitrepChildTable` and update the orphan GC code \
                     in `fm_sitrep_gc_orphans`.\n\n\
                     If your new table should NOT be covered by orphan GC, \
                     either drop the `fm_` prefix or add it to \
                     `tables_ignored` in this test.",
                    tables_unchecked.join(", ")
                ))
            } else {
                Ok(())
            }
        }
    }

    async fn ensure_sitrep_fully_populated(
        datastore: &DataStore,
        sitrep_id: SitrepUuid,
    ) {
        let counts = SitrepChildTableCounts::new(datastore, sitrep_id).await;

        let empty = counts.empty_tables();
        if !empty.is_empty() {
            panic!(
                "expected all fm_* child tables to be populated for \
                 sitrep {sitrep_id}, but these were empty: {empty:?}\n\n\
                 If every sitrep should have rows in this table, this \
                 is a bug in the test fixture. Otherwise, add an \
                 exception to ensure_sitrep_fully_populated()."
            );
        }
    }

    async fn ensure_sitrep_children_fully_deleted(
        datastore: &DataStore,
        sitrep_id: SitrepUuid,
    ) {
        let counts = SitrepChildTableCounts::new(datastore, sitrep_id).await;

        assert!(
            counts.all_empty(),
            "sitrep {sitrep_id} not fully deleted. Non-empty tables: {:?}",
            counts.non_empty_tables()
        );
    }

    #[tokio::test]
    async fn test_representative_sitrep_child_tables() {
        let logctx =
            dev::test_setup_log("test_representative_sitrep_child_tables");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Build a representative sitrep that populates every child table.
        let sitrep = make_sitrep_with_cases(&opctx, &datastore).await;
        let sitrep_id = sitrep.id();

        datastore
            .fm_sitrep_insert(&opctx, sitrep.clone())
            .await
            .expect("failed to insert sitrep");

        // Verify every child table has ≥1 row, AND that every fm_*
        // child table in the schema is covered by SitrepChildTable.
        ensure_sitrep_fully_populated(&datastore, sitrep_id).await;

        // Insert a child sitrep so the original is no longer current
        // and can be deleted.
        datastore
            .fm_sitrep_insert(
                opctx,
                fm::Sitrep {
                    metadata: fm::SitrepMetadata {
                        parent_sitrep_id: Some(sitrep_id),
                        id: SitrepUuid::new_v4(),
                        time_created: Utc::now(),
                        creator_id: OmicronZoneUuid::new_v4(),
                        comment: "child sitrep".to_string(),
                        inv_collection_id: CollectionUuid::new_v4(),
                    },
                    cases: Default::default(),
                    ereports_by_id: Default::default(),
                },
            )
            .await
            .expect("failed to insert child sitrep");

        // Delete the original sitrep.
        let deleted = datastore
            .fm_sitrep_delete_all(&opctx, vec![sitrep_id])
            .await
            .expect("failed to delete sitrep");
        assert_eq!(deleted, 1);

        // Verify all child rows are gone.
        ensure_sitrep_children_fully_deleted(&datastore, sitrep_id).await;

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Stress test that concurrently inserts, reads, and garbage-collects
    /// sitreps. The key invariant: a successful read must always return a
    /// *complete* sitrep (no torn reads). Errors (e.g. `NotFound`) are
    /// expected and fine — partial data is not.
    ///
    /// Writers race with each other, causing `ParentNotCurrent` failures.
    /// Those failed inserts leave orphaned metadata + child rows (not in
    /// history, stale parent) that the GC tasks find and delete. Readers
    /// concurrently read sitreps that may be mid-GC.
    #[tokio::test]
    async fn test_concurrent_sitrep_insert_read_gc() {
        use rand::Rng;
        use rand::SeedableRng;
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;
        use tokio::sync::Mutex;

        const TEST_NAME: &str = "test_concurrent_sitrep_insert_read_gc";
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Shared state: sitreps available for reading.
        let live_sitreps: Arc<Mutex<Vec<fm::Sitrep>>> =
            Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        // Shared counters for all tasks, with Display for panic messages.
        #[derive(Clone)]
        struct Stats {
            reads_ok: Arc<AtomicUsize>,
            reads_err: Arc<AtomicUsize>,
            inserts: Arc<AtomicUsize>,
            gc_runs: Arc<AtomicUsize>,
        }

        impl std::fmt::Display for Stats {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "reads_ok={}, reads_err={}, inserts={}, \
                     gc_runs={}",
                    self.reads_ok.load(Ordering::Relaxed),
                    self.reads_err.load(Ordering::Relaxed),
                    self.inserts.load(Ordering::Relaxed),
                    self.gc_runs.load(Ordering::Relaxed),
                )
            }
        }

        let stats = Stats {
            reads_ok: Arc::new(AtomicUsize::new(0)),
            reads_err: Arc::new(AtomicUsize::new(0)),
            inserts: Arc::new(AtomicUsize::new(0)),
            gc_runs: Arc::new(AtomicUsize::new(0)),
        };

        const NUM_WRITERS: usize = 3;
        const NUM_READERS: usize = 10;
        const NUM_GC: usize = 2;

        let mut handles = Vec::new();

        // --- Writer tasks ---
        for n in 0..NUM_WRITERS {
            let task_name = format!("writer-{n}");
            let datastore = datastore.clone();
            let opctx = opctx.child(
                std::iter::once(("task".to_string(), task_name.clone()))
                    .collect(),
            );
            let log = opctx.log.clone();
            let live_sitreps = live_sitreps.clone();
            let stop = stop.clone();
            let stats = stats.clone();
            handles.push(tokio::spawn(async move {
                // Track the most recent child ID so we can chain
                // sitreps correctly (each new sitrep's parent must
                // be the current sitrep). Multiple writers will race
                // on this — losers reset and retry.
                let mut last_child_id: Option<SitrepUuid> = None;
                while !stop.load(Ordering::Relaxed) {
                    let mut sitrep =
                        make_sitrep_with_cases(&opctx, &datastore).await;
                    sitrep.metadata.parent_sitrep_id = last_child_id;
                    let sitrep_id = sitrep.id();
                    match datastore
                        .fm_sitrep_insert(&opctx, sitrep.clone())
                        .await
                    {
                        Ok(()) => {}
                        Err(InsertSitrepError::ParentNotCurrent(_)) => {
                            slog::info!(
                                &log,
                                "parent not current, resetting chain";
                                "sitrep_id" => %sitrep_id,
                                "parent" => ?last_child_id,
                            );
                            last_child_id = None;
                            continue;
                        }
                        Err(other) => {
                            panic!(
                                "[{task_name}] unexpected insert error \
                                 for sitrep {sitrep_id}: {other} ({stats})"
                            );
                        }
                    }

                    // Insert an empty child so the original isn't
                    // current and can be deleted.
                    let child_id = SitrepUuid::new_v4();
                    let child = fm::Sitrep {
                        metadata: fm::SitrepMetadata {
                            id: child_id,
                            parent_sitrep_id: Some(sitrep_id),
                            time_created: Utc::now(),
                            creator_id: OmicronZoneUuid::new_v4(),
                            comment: "child".to_string(),
                            inv_collection_id: CollectionUuid::new_v4(),
                        },
                        cases: Default::default(),
                        ereports_by_id: Default::default(),
                    };
                    match datastore.fm_sitrep_insert(&opctx, child).await {
                        Ok(()) => {}
                        Err(InsertSitrepError::ParentNotCurrent(_)) => {
                            slog::info!(
                                &log,
                                "child insert: parent not current, \
                                 resetting chain";
                                "sitrep_id" => %sitrep_id,
                                "child_id" => %child_id,
                            );
                            last_child_id = None;
                            continue;
                        }
                        Err(other) => {
                            panic!(
                                "[{task_name}] unexpected child insert \
                                 error for sitrep {sitrep_id}, child \
                                 {child_id}: {other} ({stats})"
                            );
                        }
                    }

                    slog::info!(
                        &log,
                        "inserted sitrep pair";
                        "sitrep_id" => %sitrep_id,
                        "child_id" => %child_id,
                    );
                    last_child_id = Some(child_id);
                    live_sitreps.lock().await.push(sitrep);
                    stats.inserts.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        // --- Reader tasks ---
        for n in 0..NUM_READERS {
            let task_name = format!("reader-{n}");
            let datastore = datastore.clone();
            let opctx = opctx.child(
                std::iter::once(("task".to_string(), task_name.clone()))
                    .collect(),
            );
            let log = opctx.log.clone();
            let live_sitreps = live_sitreps.clone();
            let stop = stop.clone();
            let stats = stats.clone();
            handles.push(tokio::spawn(async move {
                let mut rng = rand::rngs::StdRng::from_os_rng();
                while !stop.load(Ordering::Relaxed) {
                    // Pick a random sitrep to read.
                    let entry = {
                        let guard = live_sitreps.lock().await;
                        if guard.is_empty() {
                            drop(guard);
                            tokio::task::yield_now().await;
                            continue;
                        }
                        let idx = rng.random_range(0..guard.len());
                        guard[idx].clone()
                    };
                    let sitrep_id = entry.id();
                    match datastore.fm_sitrep_read(&opctx, sitrep_id).await {
                        Ok(read_sitrep) => {
                            slog::debug!(
                                &log, "read ok";
                                "sitrep_id" => %sitrep_id,
                            );
                            // If this doesn't match, we have a torn read!
                            assert_sitreps_eq(&entry, &read_sitrep);
                            stats.reads_ok.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(Error::ObjectNotFound { .. }) => {
                            slog::debug!(
                                &log, "read not found (expected)";
                                "sitrep_id" => %sitrep_id,
                            );
                            stats.reads_err.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(other) => {
                            panic!(
                                "[{task_name}] unexpected read error for \
                                 sitrep {sitrep_id}: {other} ({stats})"
                            );
                        }
                    }
                }
            }));
        }

        // --- GC tasks ---
        for n in 0..NUM_GC {
            let task_name = format!("gc-{n}");
            let datastore = datastore.clone();
            let opctx = opctx.child(
                std::iter::once(("task".to_string(), task_name.clone()))
                    .collect(),
            );
            let log = opctx.log.clone();
            let stop = stop.clone();
            let stats = stats.clone();
            handles.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let result = datastore
                        .fm_sitrep_gc_orphans(&opctx)
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "[{task_name}] unexpected GC error: \
                                 {e} ({stats})"
                            )
                        });
                    slog::info!(
                        &log, "GC pass complete";
                        "sitreps_deleted" => result.sitreps_deleted,
                        "child_tables" => ?result.child_tables,
                    );
                    stats.gc_runs.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        // Wait until we've exercised enough of each operation, then stop.
        const MIN_READS: usize = 200;
        const MIN_INSERTS: usize = 20;
        const MIN_GC_RUNS: usize = 10;
        loop {
            let r = stats.reads_ok.load(Ordering::Relaxed);
            let i = stats.inserts.load(Ordering::Relaxed);
            let g = stats.gc_runs.load(Ordering::Relaxed);
            if r >= MIN_READS && i >= MIN_INSERTS && g >= MIN_GC_RUNS {
                break;
            }
            tokio::task::yield_now().await;
        }
        stop.store(true, Ordering::Relaxed);

        // Join all tasks — a panic in any task (from assert_sitreps_eq)
        // means we detected a torn read.
        for handle in handles {
            handle.await.expect("task panicked");
        }

        eprintln!("Stress test results: {stats}");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
