// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on problems and problem revisions.
//!
//! See [RFD 699](https://rfd.shared.oxide.computer/rfd/0699) for details on
//! problems.

use super::DataStore;
use super::RunnableQuery;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db::model::DbTypedGeneration;
use crate::db::model::Problem;
use crate::db::model::ProblemIdentity;
use crate::db::model::ProblemRevision;
use crate::db::model::ProblemView;
use crate::db::pagination::paginated_multicolumn;
use crate::db::sitrep_guard::SitrepGuardedInsert;
use crate::db::sitrep_guard::SitrepGuardedInsertOutcome;
use crate::db::update_and_check::UpdateAndCheck;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::problem::dsl as problem_dsl;
use nexus_db_schema::schema::problem_revision::dsl as rev_dsl;
use nexus_types::external_api::problem as api;
use nexus_types::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_generation_kinds::ProblemRevNum;
use omicron_generation_kinds::SitrepProblemGeneration;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::ProblemUuid;
use std::cmp::Ordering;
use uuid::Uuid;

/// Error returned by [`DataStore::problem_create`].
#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    /// A problem with this ID was already created by an earlier rendezvous
    /// activation; the `rendezvous_problem_created` marker short-circuited the
    /// insert.
    ///
    /// This does *not* imply that the problem still exists. It may have been
    /// deleted since it was created.
    #[error("problem was already created by a previous rendezvous activation")]
    AlreadyCreated,
    /// The sitrep being executed is stale: its `problem_generation` is behind
    /// the current sitrep in the database, so no problem was created.
    #[error("cannot create problem for a stale sitrep")]
    StaleSitrep,
    /// An error occurred while accessing the database.
    #[error(transparent)]
    Database(#[from] Error),
}

impl DataStore {
    /// Fetches the current revision of an already-fetched problem record,
    /// returning a view of the problem at that revision.
    ///
    /// Since revisions are immutable once written, the returned
    /// [`ProblemView`] is consistent as of the time `problem` was
    /// fetched, even if a newer revision has been created since then.
    ///
    /// The `problem` and `authz_problem` arguments must represent the same
    /// problem (i.e. because they were both returned by a call to
    /// `lookup::Problem::fetch_for`). If they do not, this method returns an
    /// internal error.
    pub async fn problem_fetch_current_revision(
        &self,
        opctx: &OpContext,
        authz_problem: &authz::Problem,
        problem: Problem,
    ) -> LookupResult<ProblemView> {
        // Make sure the authz problem and the problem model refer to the same
        // problem ID. We don't *strictly* need to check this before hitting
        // the DB, since the returned revision would not match the problem, and
        // `ProblemView::try_from` will fail, but this saves us a database
        // query and lets us return a slightly more descriptive error message.
        if problem.id() != authz_problem.id() {
            return Err(Error::InternalError {
                internal_message: format!(
                    "attempted to fetch the current revision of problem {} \
                     using an `authz::Problem` for problem {}. this is a bug!",
                    problem.id(),
                    authz_problem.id(),
                ),
            });
        }

        opctx.authorize(authz::Action::Read, authz_problem).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let revision = rev_dsl::problem_revision
            .filter(
                rev_dsl::problem_id.eq(authz_problem.id().into_untyped_uuid()),
            )
            .filter(rev_dsl::revision.eq(problem.current_revision))
            .select(ProblemRevision::as_select())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_problem),
                )
            })?;
        ProblemView::try_from((problem, revision))
    }

    /// List problems matching the provided set of `filters`, paginated by
    /// `time_created` and `ID`.
    pub async fn problem_list(
        &self,
        opctx: &OpContext,
        filter: &api::ProblemListParams,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<ProblemView> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let problems = Self::problem_list_query(filter, pagparams)
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        problems
            .into_iter()
            .map(ProblemView::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    fn problem_list_query(
        filter: &api::ProblemListParams,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> impl RunnableQuery<(Problem, ProblemRevision)> + use<> {
        let mut query = paginated_multicolumn(
            problem_dsl::problem,
            (problem_dsl::time_created, problem_dsl::id),
            pagparams,
        )
        // Exclude problems for which the first revision has not yet been
        // inserted. This is not strictly necessary, since the `INNER JOIN` also
        // ensures we will not return any rows for problems which do not have a
        // revision, but it probably helps CRDB to avoid hitting the JOINed
        // index in that case?
        .filter(problem_dsl::current_revision.gt(0))
        .filter(problem_dsl::time_deleted.is_null())
        .inner_join(
            rev_dsl::problem_revision.on(rev_dsl::problem_id
                .eq(problem_dsl::id)
                .and(rev_dsl::revision.eq(problem_dsl::current_revision))),
        )
        .select((Problem::as_select(), ProblemRevision::as_select()));
        match filter.state {
            Some(api::ProblemState::Active) => {
                query = query.filter(rev_dsl::time_resolved.is_null())
            }
            Some(api::ProblemState::Resolved) => {
                query = query.filter(rev_dsl::time_resolved.is_not_null())
            }
            None => {}
        }
        query
    }

    /// Creates the metadata record for a new problem, returning the inserted
    /// record.
    ///
    /// The inserted problem is created *without* a current revision (i.e. at
    /// revision 0). Problems without a current revision are excluded from
    /// [`Self::problem_list`], and [`Self::problem_fetch_current_revision`]
    /// returns a not-found error for them, until a revision has been inserted
    /// with [`DataStore::problem_revision_insert`] and set as the current
    /// revision by [`DataStore::problem_set_current_revision`]. Until then,
    /// the problem will not be visible in the external API.
    ///
    /// Problems are created during FM rendezvous, based on requested problems
    /// in the sitrep being executed. Therefore, this insertion is a
    /// [`SitrepGuardedInsert`], which succeeds if and only if the
    /// `expected_generation` (the problem generation number of the sitrep
    /// being executed) is equal to that of the current sitrep in the database,
    /// and no `rendezvous_problem_created` marker already exists for this
    /// problem. This way, previously created problems are not re-created, even
    /// if they have since been hard-deleted.
    ///
    /// ## Atomicity and Idempotency
    ///
    /// Creating a problem, creating its first revision, and setting the first
    /// revision as the current one are all separate steps, so this process is
    /// not atomic. This is fine, because the creation of problems and
    /// revisions is performed by the `fm_rendezvous` background task, with
    /// revision numbers generated by the sitrep. Therefore, if one Nexus only
    /// partially creates a problem, other rendezvous tasks will continue
    /// ensuring all necessary records exist. The step of making the revision
    /// current is performed last, and the new revision does not become
    /// visible to the outside world until then.
    ///
    /// Because creating problems and revisions may be performed by multiple
    /// rendezvous tasks, creating a problem is idempotent: if this problem was
    /// already created, the existing record is left unchanged and this returns
    /// [`CreateError::AlreadyCreated`]. A rendezvous task should treat that
    /// error as permission to proceed to inserting revisions, bearing in mind
    /// that the problem may since have been deleted, in which case those
    /// operations fail with a not-found error.
    ///
    /// # Errors
    ///
    /// - [`CreateError::StaleSitrep`] if the latest sitrep's
    ///   `problem_generation` is not `expected_generation`.
    /// - [`CreateError::AlreadyCreated`] if a problem with this ID was already
    ///   created.
    /// - [`CreateError::Database`] if authorization fails or the database
    ///   cannot be accessed.
    pub async fn problem_create(
        &self,
        opctx: &OpContext,
        identity: ProblemIdentity,
        expected_generation: SitrepProblemGeneration,
    ) -> Result<Problem, CreateError> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        // Taking the guard's resource ID from the inserted record ensures that
        // the marker is written for the same ID as the problem itself.
        let id = ProblemUuid::from(identity.id);
        let insert = diesel::insert_into(problem_dsl::problem)
            .values(Problem {
                identity,
                time_deleted: None,
                current_revision: ProblemRevNum::ZERO.into(),
            })
            .on_conflict(problem_dsl::id)
            .do_nothing()
            .returning(Problem::as_returning());
        let guarded = SitrepGuardedInsert::<Problem, _>::new(
            id.into_untyped_uuid(),
            expected_generation,
            insert,
        );
        let problem =
            match guarded.execute_async(&conn).await.map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
            })? {
                SitrepGuardedInsertOutcome::Created(problem) => problem,
                SitrepGuardedInsertOutcome::AlreadyExists => {
                    return Err(CreateError::AlreadyCreated);
                }
                SitrepGuardedInsertOutcome::StaleSitrep => {
                    return Err(CreateError::StaleSitrep);
                }
            };

        slog::debug!(
            &opctx.log,
            "created problem";
            "problem_id" => %id,
            "time_created" => ?problem.identity.time_created,
            "problem_generation" => %expected_generation,
        );

        Ok(problem)
    }

    /// Inserts a revision of a problem.
    ///
    /// Inserting a revision does not make it the problem's current revision.
    /// Use [`Self::problem_set_current_revision`] to update the problem record
    /// to point to that revision.
    ///
    /// Creating a revision and making it the current revision is therefore
    /// non-atomic. This is acceptable because creating revisions and marking
    /// them as the current revision is performed by the `fm_rendezvous`
    /// background task. Should one Nexus replica die while in the midst of
    /// performing rendezvous, another Nexus will finish creating the problem.
    /// Revisions which have not yet been made current are not visible in the
    /// external API until they are made current, and creating a revision is
    /// idempotent: if this problem already has a revision with the same
    /// number, the existing revision is left unchanged, and this returns
    /// `Ok(false)`. Otherwise, this returns `Ok(true)`.
    ///
    /// This does not check that the problem exists. Callers must create it
    /// with [`Self::problem_create`] first.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `revision` is a revision of a different
    /// problem than `authz_problem`.
    pub async fn problem_revision_insert(
        &self,
        opctx: &OpContext,
        authz_problem: &authz::Problem,
        revision: ProblemRevision,
    ) -> CreateResult<bool> {
        opctx.authorize(authz::Action::Modify, authz_problem).await?;
        let problem_id = ProblemUuid::from(revision.problem_id);
        if problem_id != authz_problem.id() {
            return Err(Error::InternalError {
                internal_message: format!(
                    "attempted to insert a revision of problem {problem_id} \
                    as a revision of problem {}",
                    authz_problem.id(),
                ),
            });
        }
        let conn = self.pool_connection_authorized(opctx).await?;
        let inserted = diesel::insert_into(rev_dsl::problem_revision)
            .values(revision)
            .on_conflict((rev_dsl::problem_id, rev_dsl::revision))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(inserted > 0)
    }

    /// Makes an existing revision the current revision of a problem.
    ///
    /// The problem's `time_modified` is set to the new revision's
    /// `time_created`, if the new revision is made current.
    ///
    /// A problem's current revision number only ever increases monotonically.
    /// If the `new_revision` is already the problem's current revision, or the
    /// current revision number is greater than `new_revision`, the record is
    /// not modified. This makes the operation idempotent, and prevents a Nexus
    /// performing rendezvous on an out-of-date sitrep from rolling a problem
    /// back to an earlier revision. The returned [`SetCurrentRevisionOutcome`]
    /// indicates whether or not the record was actually updated.
    ///
    /// # Errors
    ///
    /// - Returns a not-found error for `authz_problem` if the problem does not
    ///   exist or has been deleted.
    /// - Returns an internal error if the revision number is greater than the
    ///   current revision, but no revision of this problem with that number
    ///   actually exists in the database.
    pub async fn problem_set_current_revision(
        &self,
        opctx: &OpContext,
        authz_problem: &authz::Problem,
        new_revision: ProblemRevNum,
    ) -> UpdateResult<SetCurrentRevisionOutcome> {
        opctx.authorize(authz::Action::Modify, authz_problem).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let problem_id = authz_problem.id();
        // If no problem with this ID exists at all, the query returns no rows,
        // which is reported as a not-found error.
        let (_, updated_id, problem) =
            Self::problem_set_current_revision_query(problem_id, new_revision)
                .get_result_async::<(Option<Uuid>, Option<Uuid>, Problem)>(
                    &*conn,
                )
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByResource(authz_problem),
                    )
                })?;

        // Did we update the problem record?
        if updated_id.is_some() {
            return Ok(SetCurrentRevisionOutcome::Updated);
        }

        // ...I guess not. Okay, let's figure out why by inspecting the current
        // state of the problem record we found.
        if problem.time_deleted.is_some() {
            return Err(authz_problem.not_found());
        }

        // The problem record hasn't been deleted, so compare the current
        // revision number with the one we're trying to set to determine what's
        // going on here.
        let current_revision = problem.current_revision();
        match current_revision.cmp(&new_revision) {
            Ordering::Equal => Ok(SetCurrentRevisionOutcome::AlreadyCurrent),
            Ordering::Greater => {
                Ok(SetCurrentRevisionOutcome::Superseded { current_revision })
            }
            // The problem exists, is not deleted, and is at an earlier
            // revision, so the only condition that can have failed is the one
            // requiring the revision to exist.
            Ordering::Less => Err(Error::InternalError {
                internal_message: format!(
                    "problem {problem_id} revision {new_revision} does not \
                    exist (the current revision is {current_revision})",
                ),
            }),
        }
    }

    /// Returns the query run by [`Self::problem_set_current_revision`], which
    /// makes `new_revision` the current revision of a problem, and sets the
    /// problem's `time_modified` to the revision's `time_created`.
    ///
    /// The problem is only updated if it exists and is not deleted, a revision
    /// record for the new revision number exists, and the problem's current
    /// revision is earlier than `new_revision`.
    ///
    /// The problem's ID, the ID of the updated row (or `NULL` if the problem
    /// was not updated), and the problem's record as it was before the update,
    /// as returned by [`UpdateAndCheck::check_if_exists`]
    ///
    /// The SQL generated by this query is output to
    /// `tests/output/problem_set_current_revision.sql`.
    fn problem_set_current_revision_query(
        problem_id: ProblemUuid,
        new_revision: ProblemRevNum,
    ) -> impl RunnableQuery<(Option<Uuid>, Option<Uuid>, Problem)> {
        let revision_num = DbTypedGeneration::from(new_revision);
        let revision_row = rev_dsl::problem_revision
            .filter(rev_dsl::problem_id.eq(problem_id.into_untyped_uuid()))
            .filter(rev_dsl::revision.eq(revision_num));
        diesel::update(problem_dsl::problem)
            .filter(problem_dsl::id.eq(problem_id.into_untyped_uuid()))
            .filter(problem_dsl::time_deleted.is_null())
            .filter(problem_dsl::current_revision.lt(revision_num))
            .filter(diesel::dsl::exists(revision_row))
            .set((
                problem_dsl::current_revision.eq(revision_num),
                // The `EXISTS` filter above guarantees that this subquery
                // returns a row, so it is never `NULL`.
                problem_dsl::time_modified.eq(revision_row
                    .select(rev_dsl::time_created)
                    .single_value()
                    .assume_not_null()),
            ))
            .check_if_exists::<Problem>(problem_id.into_untyped_uuid())
    }
}

/// The outcome of [`DataStore::problem_set_current_revision`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetCurrentRevisionOutcome {
    /// The revision was made the problem's current revision.
    Updated,
    /// The revision was already the problem's current revision, so nothing
    /// was changed.
    AlreadyCurrent,
    /// The problem was already at a later revision, so nothing was changed.
    Superseded {
        /// The problem's current revision.
        current_revision: ProblemRevNum,
    },
}

#[cfg(test)]
mod revision_tests {
    use super::*;
    use crate::db::model::SqlU32;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use assert_matches::assert_matches;
    use dropshot::PaginationOrder;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::fm::RendezvousProblemCreated;
    use nexus_db_schema::schema::rendezvous_problem_created::dsl as marker_dsl;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use nexus_types::fm::problem::ProblemCode;
    use nexus_types::identity::Asset;
    use omicron_common::api::external::LookupType;
    use omicron_generation_kinds::SitrepAlertGeneration;
    use omicron_generation_kinds::SitrepSupportBundleGeneration;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use std::num::NonZeroU32;

    fn rev(revision: u32) -> ProblemRevNum {
        ProblemRevNum::from_u32(revision)
    }

    /// Inserts a parentless sitrep with the given `problem_generation`, which
    /// becomes the current sitrep if no sitrep is current yet.
    ///
    /// Creating a problem requires a current sitrep whose `problem_generation`
    /// matches the one the creator expects.
    async fn insert_current_sitrep(
        opctx: &OpContext,
        datastore: &DataStore,
        problem_generation: SitrepProblemGeneration,
    ) {
        let sitrep = Sitrep {
            metadata: SitrepMetadata {
                id: SitrepUuid::new_v4(),
                parent_sitrep_id: None,
                inv_collection_id: CollectionUuid::new_v4(),
                next_inv_min_time_started: Utc::now(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "problem test sitrep".to_string(),
                time_created: Utc::now(),
                alert_generation: SitrepAlertGeneration::new(),
                support_bundle_generation: SitrepSupportBundleGeneration::new(),
                problem_generation,
            },
            cases: Default::default(),
            ereports_by_id: Default::default(),
            problems: Default::default(),
        };
        datastore
            .fm_sitrep_insert(opctx, sitrep, None)
            .await
            .expect("inserting a sitrep should succeed");
    }

    /// Returns the `created_at_generation` of the creation marker for the
    /// problem `id`, or `None` if there is no marker.
    async fn load_marker_generation(
        datastore: &DataStore,
        id: ProblemUuid,
    ) -> Option<SitrepProblemGeneration> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        marker_dsl::rendezvous_problem_created
            .filter(marker_dsl::problem_id.eq(id.into_untyped_uuid()))
            .select(RendezvousProblemCreated::as_select())
            .first_async::<RendezvousProblemCreated>(&*conn)
            .await
            .optional()
            .expect("loading the creation marker should succeed")
            .map(|marker| marker.created_at_generation())
    }

    /// Returns revision `revision` of the problem `id`.
    ///
    /// Each revision's `time_created` is `revision` seconds after
    /// `time_base`, so that revisions are distinguishable by timestamp.
    fn make_revision(
        id: ProblemUuid,
        revision: u32,
        time_base: DateTime<Utc>,
    ) -> ProblemRevision {
        ProblemRevision {
            problem_id: id.into(),
            revision: rev(revision).into(),
            time_created: time_base
                + chrono::TimeDelta::seconds(i64::from(revision)),
            time_resolved: None,
            code: SqlU32::new(ProblemCode::NotARealProblem.into()),
            summary: format!("My Cool Problem (rev {revision})"),
            description: format!(
                "This is a bunch of interesting information about revision \
                 {revision} of my super cool and interesting problem. The \
                 problem is definitely real and exists."
            ),
            action_required: format!("fix the problem (rev {revision})"),
        }
    }

    /// Loads the `problem` record, including if it is deleted or still at
    /// revision zero.
    async fn load_problem(
        datastore: &DataStore,
        authz_problem: &authz::Problem,
    ) -> Problem {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        problem_dsl::problem
            .filter(problem_dsl::id.eq(authz_problem.id().into_untyped_uuid()))
            .select(Problem::as_select())
            .get_result_async(&*conn)
            .await
            .expect("problem record should exist")
    }

    /// Looks up a problem and fetches a view of it at its current revision,
    /// the same way the external API does.
    async fn fetch_current(
        opctx: &OpContext,
        datastore: &DataStore,
        id: ProblemUuid,
    ) -> LookupResult<ProblemView> {
        let (authz_problem, problem) =
            LookupPath::new(opctx, datastore).problem_id(id).fetch().await?;
        datastore
            .problem_fetch_current_revision(opctx, &authz_problem, problem)
            .await
    }

    /// Loads every revision of a problem, in revision order.
    // TODO(eliza): this should eventually grow into a real API when we want to
    // start exposing the revision history for export...
    async fn load_revisions(
        datastore: &DataStore,
        authz_problem: &authz::Problem,
    ) -> Vec<ProblemRevision> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        rev_dsl::problem_revision
            .filter(
                rev_dsl::problem_id.eq(authz_problem.id().into_untyped_uuid()),
            )
            .order_by(rev_dsl::revision.asc())
            .select(ProblemRevision::as_select())
            .load_async(&*conn)
            .await
            .expect("loading revisions should succeed")
    }

    /// Creates a problem and inserts `revisions` new revisions of it (starting
    /// at revision 1), without making any of them current.
    async fn create_with_revisions(
        opctx: &OpContext,
        datastore: &DataStore,
        revisions: u32,
    ) -> (authz::Problem, Vec<ProblemRevision>) {
        let id = ProblemUuid::new_v4();
        datastore
            .problem_create(
                opctx,
                ProblemIdentity::new(id),
                SitrepProblemGeneration::new(),
            )
            .await
            .expect("creating a problem should succeed");

        let time_base = omicron_common::now_db_precision();
        let (authz_problem,) = LookupPath::new(opctx, datastore)
            .problem_id(id)
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();
        let mut inserted = Vec::new();
        for revision in 1..=revisions {
            let revision = make_revision(id, revision, time_base);
            let was_inserted = datastore
                .problem_revision_insert(
                    opctx,
                    &authz_problem,
                    revision.clone(),
                )
                .await
                .expect("inserting a revision should succeed");
            assert!(was_inserted, "a new revision should be inserted");
            inserted.push(revision);
        }
        (authz_problem, inserted)
    }

    #[tokio::test]
    async fn expectorate_problem_set_current_revision() {
        let id = "e2c7ba5e-2b1d-4c62-9d0b-6a2f8f4c1e01".parse().unwrap();
        let query = DataStore::problem_set_current_revision_query(id, rev(2));
        expectorate_query_contents(
            &query,
            "tests/output/problem_set_current_revision.sql",
        )
        .await;
    }

    /// Creating a problem is idempotent, and a problem with no current
    /// revision does not appear in the problem list and cannot be fetched.
    #[tokio::test]
    async fn test_problem_create() {
        let logctx = dev::test_setup_log("test_problem_create");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        let id = ProblemUuid::new_v4();
        let identity = ProblemIdentity::new(id);
        let created = datastore
            .problem_create(
                opctx,
                identity.clone(),
                SitrepProblemGeneration::new(),
            )
            .await
            .expect("creating a problem should succeed");
        assert_eq!(created.identity, identity);
        assert_eq!(created.current_revision(), ProblemRevNum::ZERO);
        assert_eq!(
            load_marker_generation(datastore, id).await,
            Some(SitrepProblemGeneration::new()),
            "creating a problem should write a creation marker at the \
             expected generation"
        );

        let err = datastore
            .problem_create(
                opctx,
                ProblemIdentity::new(id),
                SitrepProblemGeneration::new(),
            )
            .await
            .expect_err("creating a problem again should not create it");
        assert_matches!(err, CreateError::AlreadyCreated);

        let (authz_problem, problem) = LookupPath::new(opctx, datastore)
            .problem_id(id)
            .fetch()
            .await
            .unwrap();
        assert_eq!(
            problem.identity, identity,
            "creating a problem a second time should not overwrite the \
             existing record's `time_created`"
        );
        assert_eq!(problem.current_revision(), ProblemRevNum::ZERO);

        let err = datastore
            .problem_fetch_current_revision(opctx, &authz_problem, problem)
            .await
            .expect_err("a problem at revision zero should not be fetchable");
        assert_matches!(err, Error::ObjectNotFound { .. });

        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };
        let listed = datastore
            .problem_list(
                opctx,
                &api::ProblemListParams { state: None },
                &pagparams,
            )
            .await
            .expect("listing problems should succeed");
        assert!(listed.is_empty(), "listed: {listed:#?}");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// A rendezvous task executing a sitrep whose `problem_generation` is
    /// behind the current sitrep's cannot create a problem.
    #[tokio::test]
    async fn test_problem_create_stale_sitrep() {
        let logctx = dev::test_setup_log("test_problem_create_stale_sitrep");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let current_generation = SitrepProblemGeneration::new().next();
        insert_current_sitrep(opctx, datastore, current_generation).await;

        let id = ProblemUuid::new_v4();
        let err = datastore
            .problem_create(
                opctx,
                ProblemIdentity::new(id),
                SitrepProblemGeneration::new(),
            )
            .await
            .expect_err("creating a problem from a stale sitrep fails");
        assert_matches!(err, CreateError::StaleSitrep);

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let problems: i64 = problem_dsl::problem
            .filter(problem_dsl::id.eq(id.into_untyped_uuid()))
            .count()
            .get_result_async(&*conn)
            .await
            .unwrap();
        assert_eq!(problems, 0, "a stale sitrep should not create a problem");
        assert_eq!(
            load_marker_generation(datastore, id).await,
            None,
            "a stale sitrep should not write a creation marker"
        );

        // At the current generation, the same problem can be created.
        datastore
            .problem_create(opctx, ProblemIdentity::new(id), current_generation)
            .await
            .expect("creating a problem at the current generation");
        assert_eq!(
            load_marker_generation(datastore, id).await,
            Some(current_generation),
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Once a problem has been created and then deleted, it is not re-created
    /// by a sitrep that still requests it.
    #[tokio::test]
    async fn test_problem_create_does_not_resurrect() {
        let logctx =
            dev::test_setup_log("test_problem_create_does_not_resurrect");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        let id = ProblemUuid::new_v4();
        datastore
            .problem_create(
                opctx,
                ProblemIdentity::new(id),
                SitrepProblemGeneration::new(),
            )
            .await
            .expect("creating a problem should succeed");

        // Hard-delete the record, so that only the creation marker remains to
        // prevent the problem from being re-created.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        diesel::delete(problem_dsl::problem)
            .filter(problem_dsl::id.eq(id.into_untyped_uuid()))
            .execute_async(&*conn)
            .await
            .expect("deleting the problem should succeed");

        let err = datastore
            .problem_create(
                opctx,
                ProblemIdentity::new(id),
                SitrepProblemGeneration::new(),
            )
            .await
            .expect_err("a deleted problem should not be re-created");
        assert_matches!(err, CreateError::AlreadyCreated);

        let problems: i64 = problem_dsl::problem
            .filter(problem_dsl::id.eq(id.into_untyped_uuid()))
            .count()
            .get_result_async(&*conn)
            .await
            .unwrap();
        assert_eq!(problems, 0, "the deleted problem should not be re-created");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests that making a new revision current updates the problem record,
    /// and that doing it a second time is idempotent.
    #[tokio::test]
    async fn test_problem_set_current_revision() {
        let logctx = dev::test_setup_log("test_problem_set_current_revision");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        let (authz_problem, revisions) =
            create_with_revisions(opctx, datastore, 2).await;

        for revision in &revisions {
            let number = ProblemRevNum::from(revision.revision);
            let outcome = datastore
                .problem_set_current_revision(opctx, &authz_problem, number)
                .await
                .expect("making a revision current should succeed");
            assert_eq!(outcome, SetCurrentRevisionOutcome::Updated);

            let fetched = fetch_current(opctx, datastore, authz_problem.id())
                .await
                .expect("fetching the problem should succeed");
            assert_eq!(fetched.revision, number);
            assert_eq!(fetched.summary, revision.summary);
            assert_eq!(fetched.identity.time_modified, revision.time_created);

            let outcome = datastore
                .problem_set_current_revision(opctx, &authz_problem, number)
                .await
                .expect(
                    "making a revision current again should succeed \
                     idempotently",
                );
            assert_eq!(outcome, SetCurrentRevisionOutcome::AlreadyCurrent);
            let problem = load_problem(datastore, &authz_problem).await;
            assert_eq!(problem.current_revision(), number);
            assert_eq!(
                problem.identity.time_modified, revision.time_created,
                "a subsequent call to set current revision should not change \
                time_modified"
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests that trying to make an earlier revision current does not change
    /// the problem's current revision or time_modified.
    #[tokio::test]
    async fn test_problem_current_revision_monotonicity() {
        let logctx =
            dev::test_setup_log("test_problem_current_revision_monotonicity");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        let (authz_problem, revisions) =
            create_with_revisions(opctx, datastore, 2).await;

        let outcome = datastore
            .problem_set_current_revision(opctx, &authz_problem, rev(2))
            .await
            .expect("making revision 2 current should succeed");
        assert_eq!(outcome, SetCurrentRevisionOutcome::Updated);

        let outcome = datastore
            .problem_set_current_revision(opctx, &authz_problem, rev(1))
            .await
            .expect("making an earlier revision current should succeed");
        assert_eq!(
            outcome,
            SetCurrentRevisionOutcome::Superseded { current_revision: rev(2) }
        );

        let problem = load_problem(datastore, &authz_problem).await;
        assert_eq!(problem.current_revision().as_u64(), 2);
        assert_eq!(problem.identity.time_modified, revisions[1].time_created);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// A revision that has not been inserted cannot be made current.
    #[tokio::test]
    async fn test_problem_set_current_revision_requires_revision() {
        let logctx = dev::test_setup_log(
            "test_problem_set_current_revision_requires_revision",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        let (authz_problem, _) =
            create_with_revisions(opctx, datastore, 1).await;
        let err = datastore
            .problem_set_current_revision(opctx, &authz_problem, rev(2))
            .await
            .expect_err("making a missing revision current should fail");
        assert_matches!(err, Error::InternalError { .. });

        let problem = load_problem(datastore, &authz_problem).await;
        assert_eq!(problem.current_revision(), ProblemRevNum::ZERO);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Making a revision current fails if the problem does not exist or has
    /// been deleted.
    #[tokio::test]
    async fn test_problem_set_current_revision_deleted_or_missing() {
        let logctx = dev::test_setup_log(
            "test_problem_set_current_revision_deleted_or_missing",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        let authz_problem = {
            // Cheat a little bit here: `LookupPath` would return `NotFound` for
            // a problem that doesn't actually exist, so (in order to simulate a
            // situation where the problem has been hard-deleted) we shall
            // construct one by hand.
            let id = ProblemUuid::new_v4();
            authz::Problem::new(authz::FLEET, id, LookupType::by_id(id))
        };
        let err = datastore
            .problem_set_current_revision(opctx, &authz_problem, rev(1))
            .await
            .expect_err("updating a nonexistent problem should fail");
        assert_matches!(err, Error::ObjectNotFound { .. });

        let (authz_problem, _) =
            create_with_revisions(opctx, datastore, 1).await;
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::update(problem_dsl::problem)
                .filter(
                    problem_dsl::id.eq(authz_problem.id().into_untyped_uuid()),
                )
                .set(problem_dsl::time_deleted.eq(Utc::now()))
                .execute_async(&*conn)
                .await
                .expect("deleting the problem should succeed");
        }
        let err = datastore
            .problem_set_current_revision(opctx, &authz_problem, rev(1))
            .await
            .expect_err("updating a deleted problem should fail");
        assert_matches!(err, Error::ObjectNotFound { .. });
        assert_eq!(
            load_problem(datastore, &authz_problem,)
                .await
                .current_revision()
                .as_u64(),
            0
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Inserting a revision that already exists leaves the existing revision
    /// unchanged, and a revision of one problem cannot be inserted as a
    /// revision of another.
    #[tokio::test]
    async fn test_problem_revision_insert_is_idempotent() {
        let logctx =
            dev::test_setup_log("test_problem_revision_insert_is_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        let (authz_problem, revisions) =
            create_with_revisions(opctx, datastore, 1).await;

        let mut different = revisions[0].clone();
        different.summary = "a different revision 1".to_string();
        let inserted = datastore
            .problem_revision_insert(opctx, &authz_problem, different)
            .await
            .expect("inserting an existing revision should succeed");
        assert!(!inserted);
        assert_eq!(load_revisions(datastore, &authz_problem).await, revisions);

        let other = make_revision(
            ProblemUuid::new_v4(),
            2,
            omicron_common::now_db_precision(),
        );
        let err = datastore
            .problem_revision_insert(opctx, &authz_problem, other)
            .await
            .expect_err("inserting another problem's revision should fail");
        assert_matches!(err, Error::InternalError { .. });
        assert_eq!(load_revisions(datastore, &authz_problem).await, revisions);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// When several revisions are made current concurrently, the problem ends
    /// up at the latest of them.
    #[tokio::test]
    async fn test_problem_concurrent_set_current_revision() {
        const REVISIONS: u32 = 8;

        let logctx =
            dev::test_setup_log("test_problem_concurrent_set_current_revision");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        let (authz_problem, revisions) =
            create_with_revisions(opctx, datastore, REVISIONS).await;
        let results =
            futures::future::join_all((1..=REVISIONS).map(|revision| {
                datastore.problem_set_current_revision(
                    opctx,
                    &authz_problem,
                    rev(revision),
                )
            }))
            .await;
        for result in results {
            result.expect("making a revision current should succeed");
        }

        let problem = load_problem(datastore, &authz_problem).await;
        assert_eq!(
            problem.current_revision(),
            ProblemRevNum::from_u32(REVISIONS)
        );
        assert_eq!(
            problem.identity.time_modified,
            revisions.last().unwrap().time_created,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Creates a problem with the provided `time_created`, and inserts
    /// and makes current its first revision.
    ///
    /// If `resolved` is true, the first revision is resolved.
    async fn create_current_problem(
        opctx: &OpContext,
        datastore: &DataStore,
        time_created: DateTime<Utc>,
        resolved: bool,
    ) -> authz::Problem {
        let id = ProblemUuid::new_v4();
        let identity = ProblemIdentity {
            id: id.into(),
            time_created,
            time_modified: time_created,
        };
        datastore
            .problem_create(opctx, identity, SitrepProblemGeneration::new())
            .await
            .expect("creating a problem should succeed");
        let (authz_problem,) = LookupPath::new(opctx, datastore)
            .problem_id(id)
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        let mut revision = make_revision(id, 1, time_created);
        if resolved {
            revision.time_resolved = Some(revision.time_created);
        }
        datastore
            .problem_revision_insert(opctx, &authz_problem, revision)
            .await
            .expect("inserting a revision should succeed");
        datastore
            .problem_set_current_revision(opctx, &authz_problem, rev(1))
            .await
            .expect("making a revision current should succeed");
        authz_problem
    }

    /// Lists every problem matching `filter`, fetching `page_size`
    /// problems at a time.
    async fn list_all(
        opctx: &OpContext,
        datastore: &DataStore,
        filter: api::ProblemListParams,
        direction: PaginationOrder,
        page_size: u32,
    ) -> Vec<ProblemView> {
        // Guards against a pagination bug that never returns an empty page.
        const MAX_PAGES: usize = 100;

        let limit = NonZeroU32::new(page_size).unwrap();
        let mut marker: Option<(DateTime<Utc>, Uuid)> = None;
        let mut all = Vec::new();
        for _ in 0..MAX_PAGES {
            let pagparams =
                DataPageParams { marker: marker.as_ref(), direction, limit };
            let page = datastore
                .problem_list(opctx, &filter, &pagparams)
                .await
                .expect("listing active problems should succeed");
            assert!(page.len() <= page_size as usize, "page: {page:#?}");
            let Some(last) = page.last() else {
                return all;
            };
            marker = Some((last.time_created(), last.id().into_untyped_uuid()));
            all.extend(page);
        }
        panic!("listing active problems did not finish in {MAX_PAGES} pages");
    }

    fn ids(problems: &[ProblemView]) -> Vec<ProblemUuid> {
        problems.iter().map(|problem| problem.id()).collect()
    }

    /// The state filter selects problems by the state of their
    /// *current* revision, and problems which are deleted or have no
    /// current revision are never listed.
    #[tokio::test]
    async fn test_problem_list_state_filter() {
        let logctx = dev::test_setup_log("test_problem_list_state_filter");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;
        let now = omicron_common::now_db_precision();

        let active = create_current_problem(opctx, datastore, now, false).await;
        let resolved =
            create_current_problem(opctx, datastore, now, true).await;

        // A problem whose current revision is active, but which also has a
        // later revision that is resolved and not yet current.
        let pending_resolution =
            create_current_problem(opctx, datastore, now, false).await;
        let mut resolving_revision =
            make_revision(pending_resolution.id(), 2, now);
        resolving_revision.time_resolved =
            Some(resolving_revision.time_created);
        datastore
            .problem_revision_insert(
                opctx,
                &pending_resolution,
                resolving_revision,
            )
            .await
            .expect("inserting a revision should succeed");

        // A problem with no current revision.
        datastore
            .problem_create(
                opctx,
                ProblemIdentity::new(ProblemUuid::new_v4()),
                SitrepProblemGeneration::new(),
            )
            .await
            .expect("creating a problem should succeed");

        // A deleted problem.
        let deleted =
            create_current_problem(opctx, datastore, now, false).await;
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::update(problem_dsl::problem)
                .filter(problem_dsl::id.eq(deleted.id().into_untyped_uuid()))
                .set(problem_dsl::time_deleted.eq(Utc::now()))
                .execute_async(&*conn)
                .await
                .expect("deleting the active problem should succeed");
        }

        let list = |state| {
            list_all(
                opctx,
                datastore,
                api::ProblemListParams { state },
                PaginationOrder::Ascending,
                100,
            )
        };
        let sorted_ids = |problems: Vec<ProblemView>| {
            let mut ids = ids(&problems);
            ids.sort_unstable();
            ids
        };
        let sorted = |mut ids: Vec<ProblemUuid>| {
            ids.sort_unstable();
            ids
        };

        let all = list(None).await;
        let listed_pending = all
            .iter()
            .find(|problem| problem.id() == pending_resolution.id())
            .expect("the problem pending resolution should be listed");
        assert_eq!(listed_pending.revision, rev(1));
        assert_eq!(listed_pending.time_resolved, None);
        assert_eq!(
            sorted_ids(all),
            sorted(vec![active.id(), resolved.id(), pending_resolution.id()]),
        );

        assert_eq!(
            sorted_ids(list(Some(api::ProblemState::Active)).await),
            sorted(vec![active.id(), pending_resolution.id()]),
        );

        let listed_resolved = list(Some(api::ProblemState::Resolved)).await;
        assert_eq!(ids(&listed_resolved), vec![resolved.id()]);
        assert!(listed_resolved[0].time_resolved.is_some());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Paginating through the Active Problem list returns every problem
    /// exactly once, ordered by `time_created` and then by ID, in either
    /// direction and with or without a state filter.
    #[tokio::test]
    async fn test_active_problem_list_pagination() {
        let logctx = dev::test_setup_log("test_active_problem_list_pagination");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        insert_current_sitrep(opctx, datastore, SitrepProblemGeneration::new())
            .await;

        // Several problems share a `time_created`, so that their order is
        // decided by their IDs. Every third problem is resolved.
        let base = omicron_common::now_db_precision();
        let offsets = [0, 1, 2, 2, 2, 3, 4];
        let mut expected = Vec::new();
        for (i, offset) in offsets.into_iter().enumerate() {
            let time_created = base + chrono::TimeDelta::seconds(offset);
            let resolved = i % 3 == 2;
            let problem = create_current_problem(
                opctx,
                datastore,
                time_created,
                resolved,
            )
            .await;
            expected.push((time_created, problem.id(), resolved));
        }
        expected.sort_by_key(|&(time_created, id, _)| {
            (time_created, id.into_untyped_uuid())
        });
        let expected_all: Vec<_> =
            expected.iter().map(|&(_, id, _)| id).collect();
        let expected_active: Vec<_> = expected
            .iter()
            .filter(|&&(_, _, resolved)| !resolved)
            .map(|&(_, id, _)| id)
            .collect();

        for page_size in [1, 2, 100] {
            let ascending = list_all(
                opctx,
                datastore,
                api::ProblemListParams { state: None },
                PaginationOrder::Ascending,
                page_size,
            )
            .await;
            assert_eq!(ids(&ascending), expected_all, "page size {page_size}");

            let descending = list_all(
                opctx,
                datastore,
                api::ProblemListParams { state: None },
                PaginationOrder::Descending,
                page_size,
            )
            .await;
            let mut expected_descending = expected_all.clone();
            expected_descending.reverse();
            assert_eq!(
                ids(&descending),
                expected_descending,
                "page size {page_size}"
            );

            let active = list_all(
                opctx,
                datastore,
                api::ProblemListParams {
                    state: Some(api::ProblemState::Active),
                },
                PaginationOrder::Ascending,
                page_size,
            )
            .await;
            assert_eq!(ids(&active), expected_active, "page size {page_size}");
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    //     use super::*;
    //     use crate::db::explain::ExplainableAsync;
    //     use crate::db::pub_test_utils::TestDatabase;
    //     use dropshot::PaginationOrder;
    //     use omicron_test_utils::dev;
    //     use std::num::NonZeroU32;

    //     #[tokio::test]
    //     async fn explain_problem_list_all_states_query() {
    //         explain_list_query(
    //             "explain_problem_list_all_states_query",
    //             api::ProblemListParams { state: None },
    //         )
    //         .await;
    //     }

    //     #[tokio::test]
    //     async fn explain_problem_list_active_query() {
    //         explain_list_query(
    //             "explain_problem_list_active_query",
    //             api::ProblemListParams {
    //                 state: Some(api::ProblemState::Active),
    //             },
    //         )
    //         .await;
    //     }

    //     #[tokio::test]
    //     async fn explain_problem_list_resolved_query() {
    //         explain_list_query(
    //             "explain_problem_list_resolved_query",
    //             api::ProblemListParams {
    //                 state: Some(api::ProblemState::Resolved),
    //             },
    //         )
    //         .await;
    //     }

    //     async fn explain_list_query(
    //         test_name: &str,
    //         filters: api::ProblemListParams,
    //     ) {
    //         let logctx = dev::test_setup_log(test_name);
    //         let db = TestDatabase::new_with_pool(&logctx.log).await;
    //         let pool = db.pool();
    //         let conn = pool.claim().await.unwrap();

    //         let pagparams = DataPageParams::<'_, (DateTime<Utc>, Uuid)> {
    //             marker: None,
    //             direction: PaginationOrder::Descending,
    //             limit: NonZeroU32::new(100).unwrap(),
    //         };
    //         eprintln!("--- filters: {filters:#?}\n");

    //         let query = DataStore::problem_list_query(&filters, &pagparams);

    //         let explanation = query
    //             .explain_async(&conn)
    //             .await
    //             .expect("Failed to explain query - is it valid SQL?");

    //         eprintln!("--- explanation: {explanation}");

    //         assert!(
    //             !explanation.contains("FULL SCAN"),
    //             "Found an unexpected FULL SCAN: {explanation}",
    //         );

    //         db.terminate().await;
    //         logctx.cleanup_successful();
    //     }
}
