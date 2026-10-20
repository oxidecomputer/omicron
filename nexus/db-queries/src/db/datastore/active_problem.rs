// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on Active Problems.
//!
//! See [RFD 699](https://rfd.shared.oxide.computer/rfd/0699) for details on
//! Active Problems.

use super::DataStore;
use super::RunnableQuery;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db::model::ActiveProblem;
use crate::db::model::ActiveProblemIdentity;
use crate::db::model::ActiveProblemRevision;
use crate::db::model::ActiveProblemView;
use crate::db::model::DbTypedGeneration;
use crate::db::pagination::paginated_multicolumn;
use crate::db::update_and_check::UpdateAndCheck;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::active_problem::dsl as problem_dsl;
use nexus_db_schema::schema::active_problem_revision::dsl as rev_dsl;
use nexus_types::external_api::active_problem as api;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_generation_kinds::ActiveProblemRevNum;
use omicron_uuid_kinds::ActiveProblemUuid;
use omicron_uuid_kinds::GenericUuid;
use std::cmp::Ordering;
use uuid::Uuid;

impl DataStore {
    pub async fn active_problem_fetch(
        &self,
        opctx: &OpContext,
        authz_problem: &authz::ActiveProblem,
    ) -> LookupResult<ActiveProblemView> {
        opctx.authorize(authz::Action::Read, authz_problem).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let (problem, revision) =
            Self::active_problem_fetch_query(authz_problem.id())
                .get_result_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByResource(authz_problem),
                    )
                })?;
        ActiveProblemView::try_from((problem, revision))
    }

    /// Selects a non-deleted Active Problem together with the revision that
    /// its `current_revision` column refers to.
    fn active_problem_fetch_query(
        id: ActiveProblemUuid,
    ) -> impl RunnableQuery<(ActiveProblem, ActiveProblemRevision)> + use<>
    {
        problem_dsl::active_problem
            .filter(problem_dsl::id.eq(id.into_untyped_uuid()))
            .filter(problem_dsl::time_deleted.is_null())
            .inner_join(
                rev_dsl::active_problem_revision.on(rev_dsl::problem_id
                    .eq(problem_dsl::id)
                    .and(rev_dsl::revision.eq(problem_dsl::current_revision))),
            )
            .select((
                ActiveProblem::as_select(),
                ActiveProblemRevision::as_select(),
            ))
    }

    /// List Active Problems matching the provided set of `filters`, paginated
    /// by `time_created` and `ID`.
    pub async fn active_problem_list(
        &self,
        opctx: &OpContext,
        filter: &api::ActiveProblemListParams,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<ActiveProblemView> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let problems = Self::active_problem_list_query(filter, pagparams)
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        problems
            .into_iter()
            .map(ActiveProblemView::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    fn active_problem_list_query(
        filter: &api::ActiveProblemListParams,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> impl RunnableQuery<(ActiveProblem, ActiveProblemRevision)> + use<>
    {
        let mut query = paginated_multicolumn(
            problem_dsl::active_problem,
            (problem_dsl::time_created, problem_dsl::id),
            pagparams,
        )
        // Exclude Active Problems for which the first revision has not yet been
        // inserted. This is not strictly necessary, since the `INNER JOIN` also
        // ensures we will not return any rows for Active Problems which do not
        // have a revision, but it probably helps CRDB to avoid hitting the
        // JOINed index in that case?
        .filter(problem_dsl::current_revision.gt(0))
        .filter(problem_dsl::time_deleted.is_null())
        .inner_join(
            rev_dsl::active_problem_revision.on(rev_dsl::problem_id
                .eq(problem_dsl::id)
                .and(rev_dsl::revision.eq(problem_dsl::current_revision))),
        )
        .select((
            ActiveProblem::as_select(),
            ActiveProblemRevision::as_select(),
        ));
        match filter.state {
            Some(api::ActiveProblemState::Active) => {
                query = query.filter(rev_dsl::time_resolved.is_null())
            }
            Some(api::ActiveProblemState::Resolved) => {
                query = query.filter(rev_dsl::time_resolved.is_not_null())
            }
            None => {}
        }
        query
    }

    /// Creates the metadata record for a new Active Problem.
    ///
    /// The inserted Active Problem is created *without* a current revision
    /// (i.e. at revision 0). Active Problems without a current revision are
    /// excluded from [`Self::active_problem_list`] and cannot be fetched until
    /// a revision has been inserted with
    /// [`DataStore::active_problem_revision_insert`] and set as the current
    /// revision by [`DataStore::active_problem_set_current_revision`]. Until
    /// then, the Active Problem will not be visible in the external API.
    ///
    /// ## Atomicity and Idempotency
    ///
    /// Creating an Active Problem, creating its first revision, and setting the
    /// first revision as the current one are all separate steps, so this
    /// process is not atomic. This is fine, because the creation of Active
    /// Problems and revisions is performed by the `fm_rendezvous` background
    /// task, with revision numbers generated by the sitrep. Therefore, if one
    /// Nexus only partially creates an Active Problem, other rendezvous tasks
    /// will continue ensuring all necessary records exist. The step of making
    /// the revision current is performed last, and the new revision does not
    /// become visible to the outside world until then.
    ///
    /// Because creating Active Problems and revisions may be performed by
    /// multiple rendezvous tasks, creating an Active Problem *is* idempotent:
    /// if an Active Problem with the provided ID already exists, it is left
    /// unchanged and this function returns `Ok(false)`.
    pub async fn active_problem_create(
        &self,
        opctx: &OpContext,
        identity: ActiveProblemIdentity,
    ) -> CreateResult<bool> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let inserted = diesel::insert_into(problem_dsl::active_problem)
            .values(ActiveProblem {
                identity,
                time_deleted: None,
                current_revision: ActiveProblemRevNum::from_u32(0).into(),
            })
            .on_conflict(problem_dsl::id)
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(inserted > 0)
    }

    /// Inserts a revision of an Active Problem.
    ///
    /// Inserting a revision does not make it the problem's current revision.
    /// Use [`Self::active_problem_set_current_revision`] to update the Active
    /// Problem record to point to that revision.
    ///
    /// Creating a revision and making it the current revision is therefore
    /// non-atomic. This is acceptable because creating revisions and marking
    /// them as the current revision is performed by the `fm_rendezvous`
    /// background task. Should one Nexus replica die while in the midst of
    /// performing rendezvous, another Nexus will finish creating the Active
    /// Problem. Revisions which have not yet been made current are not visible
    /// in the external API until they are made current, and creating a revision
    /// is idempotent: if this problem already has a revision with the same
    /// number, the existing revision is left unchanged, and this returns
    /// `Ok(false)`. Otherwise, this returns `Ok(true)`.
    ///
    /// This does not check that the Active Problem exists. Callers must create
    /// it with [`Self::active_problem_create`] first.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `revision` is a revision of a different
    /// Active Problem than `authz_problem`.
    pub async fn active_problem_revision_insert(
        &self,
        opctx: &OpContext,
        authz_problem: &authz::ActiveProblem,
        revision: ActiveProblemRevision,
    ) -> CreateResult<bool> {
        opctx.authorize(authz::Action::Modify, authz_problem).await?;
        let problem_id = ActiveProblemUuid::from(revision.problem_id);
        if problem_id != authz_problem.id() {
            return Err(Error::InternalError {
                internal_message: format!(
                    "attempted to insert a revision of Active Problem \
                    {problem_id} as a revision of Active Problem {}",
                    authz_problem.id(),
                ),
            });
        }
        let conn = self.pool_connection_authorized(opctx).await?;
        let inserted = diesel::insert_into(rev_dsl::active_problem_revision)
            .values(revision)
            .on_conflict((rev_dsl::problem_id, rev_dsl::revision))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(inserted > 0)
    }

    /// Makes an existing revision the current revision of an Active Problem.
    ///
    /// The problem's `time_modified` is set to the new revision's
    /// `time_created`, if the new revision is made current.
    ///
    /// An Active Problem's current revision number only ever increases
    /// monotonically. If the `new_revision` is already the Active Problem's
    /// current revision, or the current revision number is greater than
    /// `new_revision`, the record is not modified. This makes the operation
    /// idempotent, and prevents a Nexus performing rendezvous on an out-of-date
    /// sitrep from rolling a problem back to an earlier revision. The returned
    /// [`SetCurrentRevisionOutcome`] indicates whether or not the record was
    /// actually updated.
    ///
    /// # Errors
    ///
    /// - Returns a not-found error for `authz_problem` if the Active Problem
    ///   does not exist or has been deleted.
    /// - Returns an internal error if the revision number is greater than the
    ///   current revision, but no revision of this problem with that number
    ///   actually exists in the database.
    pub async fn active_problem_set_current_revision(
        &self,
        opctx: &OpContext,
        authz_problem: &authz::ActiveProblem,
        new_revision: ActiveProblemRevNum,
    ) -> UpdateResult<SetCurrentRevisionOutcome> {
        opctx.authorize(authz::Action::Modify, authz_problem).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let problem_id = authz_problem.id();
        // If no Active Problem with this ID exists at all, the query returns
        // no rows, which is reported as a not-found error.
        let (_, updated_id, problem) =
            Self::active_problem_set_current_revision_query(
                problem_id,
                new_revision,
            )
            .get_result_async::<(Option<Uuid>, Option<Uuid>, ActiveProblem)>(
                &*conn,
            )
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_problem),
                )
            })?;

        // Did we update the Active Problem record?
        if updated_id.is_some() {
            return Ok(SetCurrentRevisionOutcome::Updated);
        }

        // ...I guess not. Okay, let's figure out why by inspecting the current
        // state of the Active Problem record we found.
        if problem.time_deleted.is_some() {
            return Err(authz_problem.not_found());
        }

        // The Active Problem record hasn't been deleted, so compare the current
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
                    "Active Problem {problem_id} revision {new_revision} does \
                    not exist (the current revision is {current_revision})",
                ),
            }),
        }
    }

    /// Returns the query run by [`Self::active_problem_set_current_revision`],
    /// which makes `new_revision` the current revision of an Active Problem,
    /// and sets the problem's `time_modified` to the revision's `time_created`.
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
    /// `tests/output/active_problem_set_current_revision.sql`.
    fn active_problem_set_current_revision_query(
        problem_id: ActiveProblemUuid,
        new_revision: ActiveProblemRevNum,
    ) -> impl RunnableQuery<(Option<Uuid>, Option<Uuid>, ActiveProblem)> {
        let revision_num = DbTypedGeneration::from(new_revision);
        let revision_row = rev_dsl::active_problem_revision
            .filter(rev_dsl::problem_id.eq(problem_id.into_untyped_uuid()))
            .filter(rev_dsl::revision.eq(revision_num));
        diesel::update(problem_dsl::active_problem)
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
            .check_if_exists::<ActiveProblem>(problem_id.into_untyped_uuid())
    }
}

/// The outcome of [`DataStore::active_problem_set_current_revision`].
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
        current_revision: ActiveProblemRevNum,
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
    use nexus_types::fm::active_problem::ProblemCode;
    use nexus_types::identity::Asset;
    use omicron_common::api::external::LookupType;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;

    fn rev(revision: u32) -> ActiveProblemRevNum {
        ActiveProblemRevNum::from_u32(revision)
    }

    /// Returns revision `revision` of the problem `id`.
    ///
    /// Each revision's `time_created` is `revision` seconds after
    /// `time_base`, so that revisions are distinguishable by timestamp.
    fn make_revision(
        id: ActiveProblemUuid,
        revision: u32,
        time_base: DateTime<Utc>,
    ) -> ActiveProblemRevision {
        ActiveProblemRevision {
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

    /// Loads the `active_problem` record, including if it is deleted or still
    /// at revision zero.
    async fn load_problem(
        datastore: &DataStore,
        authz_problem: &authz::ActiveProblem,
    ) -> ActiveProblem {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        problem_dsl::active_problem
            .filter(problem_dsl::id.eq(authz_problem.id().into_untyped_uuid()))
            .select(ActiveProblem::as_select())
            .get_result_async(&*conn)
            .await
            .expect("active problem record should exist")
    }

    /// Loads every revision of an Active Problem, in revision order.
    // TODO(eliza): this should eventually grow into a real API when we want to
    // start exposing the revision history for export...
    async fn load_revisions(
        datastore: &DataStore,
        authz_problem: &authz::ActiveProblem,
    ) -> Vec<ActiveProblemRevision> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        rev_dsl::active_problem_revision
            .filter(
                rev_dsl::problem_id.eq(authz_problem.id().into_untyped_uuid()),
            )
            .order_by(rev_dsl::revision.asc())
            .select(ActiveProblemRevision::as_select())
            .load_async(&*conn)
            .await
            .expect("loading revisions should succeed")
    }

    /// Creates an Active Problem and inserts `revisions` new revisions of it
    /// (starting at revision 1), without making any of them current.
    async fn create_with_revisions(
        opctx: &OpContext,
        datastore: &DataStore,
        revisions: u32,
    ) -> (authz::ActiveProblem, Vec<ActiveProblemRevision>) {
        let id = ActiveProblemUuid::new_v4();
        let created = datastore
            .active_problem_create(opctx, ActiveProblemIdentity::new(id))
            .await
            .expect("creating an active problem should succeed");
        assert!(created, "a new active problem should be inserted");

        let time_base = omicron_common::now_db_precision();
        let (authz_problem,) = LookupPath::new(opctx, datastore)
            .active_problem_id(id)
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();
        let mut inserted = Vec::new();
        for revision in 1..=revisions {
            let revision = make_revision(id, revision, time_base);
            let was_inserted = datastore
                .active_problem_revision_insert(
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
    async fn expectorate_active_problem_set_current_revision() {
        let id = "e2c7ba5e-2b1d-4c62-9d0b-6a2f8f4c1e01".parse().unwrap();
        let query =
            DataStore::active_problem_set_current_revision_query(id, rev(2));
        expectorate_query_contents(
            &query,
            "tests/output/active_problem_set_current_revision.sql",
        )
        .await;
    }

    /// Creating an Active Problem is idempotent, and a problem with no current
    /// revision does not appear in the Active Problem list and cannot be
    /// fetched.
    #[tokio::test]
    async fn test_active_problem_create() {
        let logctx = dev::test_setup_log("test_active_problem_create");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let id = ActiveProblemUuid::new_v4();
        let identity = ActiveProblemIdentity::new(id);
        let created = datastore
            .active_problem_create(opctx, identity.clone())
            .await
            .expect("creating an active problem should succeed");
        assert!(created);
        let created_again = datastore
            .active_problem_create(opctx, ActiveProblemIdentity::new(id))
            .await
            .expect("creating an active problem again should succeed");
        assert!(!created_again);

        let (authz_problem, problem) = LookupPath::new(opctx, datastore)
            .active_problem_id(id)
            .fetch()
            .await
            .unwrap();
        assert_eq!(
            problem.identity, identity,
            "creating an active problem a second time should not overwrite the \
             existing record's `time_created`"
        );
        assert_eq!(problem.current_revision(), ActiveProblemRevNum::ZERO);

        let err = datastore
            .active_problem_fetch(opctx, &authz_problem)
            .await
            .expect_err("a problem at revision zero should not be fetchable");
        assert_matches!(err, Error::ObjectNotFound { .. });

        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };
        let listed = datastore
            .active_problem_list(
                opctx,
                &api::ActiveProblemListParams { state: None },
                &pagparams,
            )
            .await
            .expect("listing active problems should succeed");
        assert!(listed.is_empty(), "listed: {listed:#?}");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests that making a new revision current updates the Active Problem record,
    /// and that doing it a second time is idempotent.
    #[tokio::test]
    async fn test_active_problem_set_current_revision() {
        let logctx =
            dev::test_setup_log("test_active_problem_set_current_revision");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_problem, revisions) =
            create_with_revisions(opctx, datastore, 2).await;

        for revision in &revisions {
            let number = ActiveProblemRevNum::from(revision.revision);
            let outcome = datastore
                .active_problem_set_current_revision(
                    opctx,
                    &authz_problem,
                    number,
                )
                .await
                .expect("making a revision current should succeed");
            assert_eq!(outcome, SetCurrentRevisionOutcome::Updated);

            let fetched = datastore
                .active_problem_fetch(opctx, &authz_problem)
                .await
                .expect("fetching the active problem should succeed");
            assert_eq!(fetched.revision, number);
            assert_eq!(fetched.summary, revision.summary);
            assert_eq!(fetched.identity.time_modified, revision.time_created);

            let outcome = datastore
                .active_problem_set_current_revision(
                    opctx,
                    &authz_problem,
                    number,
                )
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
    async fn test_active_problem_current_revision_monotonicity() {
        let logctx = dev::test_setup_log(
            "test_active_problem_current_revision_monotonicity",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_problem, revisions) =
            create_with_revisions(opctx, datastore, 2).await;

        let outcome = datastore
            .active_problem_set_current_revision(opctx, &authz_problem, rev(2))
            .await
            .expect("making revision 2 current should succeed");
        assert_eq!(outcome, SetCurrentRevisionOutcome::Updated);

        let outcome = datastore
            .active_problem_set_current_revision(opctx, &authz_problem, rev(1))
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
    async fn test_active_problem_set_current_revision_requires_revision() {
        let logctx = dev::test_setup_log(
            "test_active_problem_set_current_revision_requires_revision",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_problem, _) =
            create_with_revisions(opctx, datastore, 1).await;
        let err = datastore
            .active_problem_set_current_revision(opctx, &authz_problem, rev(2))
            .await
            .expect_err("making a missing revision current should fail");
        assert_matches!(err, Error::InternalError { .. });

        let problem = load_problem(datastore, &authz_problem).await;
        assert_eq!(problem.current_revision(), ActiveProblemRevNum::ZERO);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Making a revision current fails if the Active Problem does not exist or
    /// has been deleted.
    #[tokio::test]
    async fn test_active_problem_set_current_revision_deleted_or_missing() {
        let logctx = dev::test_setup_log(
            "test_active_problem_set_current_revision_deleted_or_missing",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let authz_problem = {
            // Cheat a little bit here: `LookupPath` would return `NotFound` for
            // a problem that doesn't actually exist, so (in order to simulate a
            // situation where the problem has been hard-deleted) we shall
            // construct one by hand.
            let id = ActiveProblemUuid::new_v4();
            authz::ActiveProblem::new(authz::FLEET, id, LookupType::by_id(id))
        };
        let err = datastore
            .active_problem_set_current_revision(opctx, &authz_problem, rev(1))
            .await
            .expect_err("updating a nonexistent problem should fail");
        assert_matches!(err, Error::ObjectNotFound { .. });

        let (authz_problem, _) =
            create_with_revisions(opctx, datastore, 1).await;
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::update(problem_dsl::active_problem)
                .filter(
                    problem_dsl::id.eq(authz_problem.id().into_untyped_uuid()),
                )
                .set(problem_dsl::time_deleted.eq(Utc::now()))
                .execute_async(&*conn)
                .await
                .expect("deleting the active problem should succeed");
        }
        let err = datastore
            .active_problem_set_current_revision(opctx, &authz_problem, rev(1))
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
    async fn test_active_problem_revision_insert_is_idempotent() {
        let logctx = dev::test_setup_log(
            "test_active_problem_revision_insert_is_idempotent",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_problem, revisions) =
            create_with_revisions(opctx, datastore, 1).await;

        let mut different = revisions[0].clone();
        different.summary = "a different revision 1".to_string();
        let inserted = datastore
            .active_problem_revision_insert(opctx, &authz_problem, different)
            .await
            .expect("inserting an existing revision should succeed");
        assert!(!inserted);
        assert_eq!(load_revisions(datastore, &authz_problem).await, revisions);

        let other = make_revision(
            ActiveProblemUuid::new_v4(),
            2,
            omicron_common::now_db_precision(),
        );
        let err = datastore
            .active_problem_revision_insert(opctx, &authz_problem, other)
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
    async fn test_active_problem_concurrent_set_current_revision() {
        const REVISIONS: u32 = 8;

        let logctx = dev::test_setup_log(
            "test_active_problem_concurrent_set_current_revision",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_problem, revisions) =
            create_with_revisions(opctx, datastore, REVISIONS).await;
        let results =
            futures::future::join_all((1..=REVISIONS).map(|revision| {
                datastore.active_problem_set_current_revision(
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
            ActiveProblemRevNum::from_u32(REVISIONS)
        );
        assert_eq!(
            problem.identity.time_modified,
            revisions.last().unwrap().time_created,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Creates an Active Problem with the provided `time_created`, and inserts
    /// and makes current its first revision.
    ///
    /// If `resolved` is true, the first revision is resolved.
    async fn create_current_problem(
        opctx: &OpContext,
        datastore: &DataStore,
        time_created: DateTime<Utc>,
        resolved: bool,
    ) -> authz::ActiveProblem {
        let id = ActiveProblemUuid::new_v4();
        let identity = ActiveProblemIdentity {
            id: id.into(),
            time_created,
            time_modified: time_created,
        };
        datastore
            .active_problem_create(opctx, identity)
            .await
            .expect("creating an active problem should succeed");
        let (authz_problem,) = LookupPath::new(opctx, datastore)
            .active_problem_id(id)
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();

        let mut revision = make_revision(id, 1, time_created);
        if resolved {
            revision.time_resolved = Some(revision.time_created);
        }
        datastore
            .active_problem_revision_insert(opctx, &authz_problem, revision)
            .await
            .expect("inserting a revision should succeed");
        datastore
            .active_problem_set_current_revision(opctx, &authz_problem, rev(1))
            .await
            .expect("making a revision current should succeed");
        authz_problem
    }

    /// Lists every Active Problem matching `filter`, fetching `page_size`
    /// problems at a time.
    async fn list_all(
        opctx: &OpContext,
        datastore: &DataStore,
        filter: api::ActiveProblemListParams,
        direction: PaginationOrder,
        page_size: u32,
    ) -> Vec<ActiveProblemView> {
        // Guards against a pagination bug that never returns an empty page.
        const MAX_PAGES: usize = 100;

        let limit = NonZeroU32::new(page_size).unwrap();
        let mut marker: Option<(DateTime<Utc>, Uuid)> = None;
        let mut all = Vec::new();
        for _ in 0..MAX_PAGES {
            let pagparams =
                DataPageParams { marker: marker.as_ref(), direction, limit };
            let page = datastore
                .active_problem_list(opctx, &filter, &pagparams)
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

    fn ids(problems: &[ActiveProblemView]) -> Vec<ActiveProblemUuid> {
        problems.iter().map(|problem| problem.id()).collect()
    }

    /// The state filter selects Active Problems by the state of their
    /// *current* revision, and Active Problems which are deleted or have no
    /// current revision are never listed.
    #[tokio::test]
    async fn test_active_problem_list_state_filter() {
        let logctx =
            dev::test_setup_log("test_active_problem_list_state_filter");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
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
            .active_problem_revision_insert(
                opctx,
                &pending_resolution,
                resolving_revision,
            )
            .await
            .expect("inserting a revision should succeed");

        // A problem with no current revision.
        datastore
            .active_problem_create(
                opctx,
                ActiveProblemIdentity::new(ActiveProblemUuid::new_v4()),
            )
            .await
            .expect("creating an active problem should succeed");

        // A deleted problem.
        let deleted =
            create_current_problem(opctx, datastore, now, false).await;
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::update(problem_dsl::active_problem)
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
                api::ActiveProblemListParams { state },
                PaginationOrder::Ascending,
                100,
            )
        };
        let sorted_ids = |problems: Vec<ActiveProblemView>| {
            let mut ids = ids(&problems);
            ids.sort_unstable();
            ids
        };
        let sorted = |mut ids: Vec<ActiveProblemUuid>| {
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
            sorted_ids(list(Some(api::ActiveProblemState::Active)).await),
            sorted(vec![active.id(), pending_resolution.id()]),
        );

        let listed_resolved =
            list(Some(api::ActiveProblemState::Resolved)).await;
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
                api::ActiveProblemListParams { state: None },
                PaginationOrder::Ascending,
                page_size,
            )
            .await;
            assert_eq!(ids(&ascending), expected_all, "page size {page_size}");

            let descending = list_all(
                opctx,
                datastore,
                api::ActiveProblemListParams { state: None },
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
                api::ActiveProblemListParams {
                    state: Some(api::ActiveProblemState::Active),
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
    //     async fn explain_active_problem_list_all_states_query() {
    //         explain_list_query(
    //             "explain_active_problem_list_all_states_query",
    //             api::ActiveProblemListParams { state: None },
    //         )
    //         .await;
    //     }

    //     #[tokio::test]
    //     async fn explain_active_problem_list_active_query() {
    //         explain_list_query(
    //             "explain_active_problem_list_active_query",
    //             api::ActiveProblemListParams {
    //                 state: Some(api::ActiveProblemState::Active),
    //             },
    //         )
    //         .await;
    //     }

    //     #[tokio::test]
    //     async fn explain_active_problem_list_resolved_query() {
    //         explain_list_query(
    //             "explain_active_problem_list_resolved_query",
    //             api::ActiveProblemListParams {
    //                 state: Some(api::ActiveProblemState::Resolved),
    //             },
    //         )
    //         .await;
    //     }

    //     async fn explain_list_query(
    //         test_name: &str,
    //         filters: api::ActiveProblemListParams,
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

    //         let query = DataStore::active_problem_list_query(&filters, &pagparams);

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
