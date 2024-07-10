// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga recovery
//!
//! ## Review of distributed sagas
//!
//! Nexus uses distributed sagas via [`steno`] to manage multi-step operations
//! that have their own unwinding or cleanup steps.  While executing sagas,
//! critical state is durably stored in the **saga log** such that after a
//! crash, the saga can be resumed while maintaining certain guarantees:
//!
//! - During normal execution, each **action** will be executed at least once.
//! - If an action B depends on action A, then once B has started, A will not
//!   run again.
//! - Once any action has failed, the saga is **unwound**, meaning that the undo
//!   actions are executed for any action that has successfully completed.
//! - The saga will not come to rest until one of these three things has
//!   happened:
//!   1. All actions complete successfully.  This is the normal case of saga
//!      completion.
//!   2. Any number of actions complete successfully, at least one action
//!      failed, and the undo actions complete successfully for any actions that
//!      *did* run.  This is the normal case of clean saga failure where
//!      intuitively the state of the world is unwound to match whatever it was
//!      before the saga ran.
//!   3. Any number of actions complete successfully, at least one action
//!      failed, and at least one undo action also failed.  This is a nebulous
//!      "stuck" state where the world may be partially changed by the saga.
//!
//! There's more to all this (see the Steno docs), but the important thing here
//! is that the persistent state is critical for ensuring these properties
//! across a Nexus crash.  The process of resuming in-progress sagas after a
//! crash is called **saga recovery**.  Fortunately, Steno handles the details
//! of those constraints.  All we have to do is provide Steno with the
//! persistent state of any sagas that it needs to resume.
//!
//!
//! ## Saga recovery and persistent state
//!
//! Everything needed to recover a saga is stored in:
//!
//! 1. a **saga** record, which is mostly immutable
//! 2. the **saga log**, an append-only description of exactly what happened
//!    during execution
//!
//! Responsibility for persisting this state is divided across Steno and Nexus:
//!
//! 1. Steno tells its consumer (Nexus) precisely what information needs to be
//!    stored and when.  It does this by invoking methods on the `SecStore`
//!    trait at key points in the saga's execution.  Steno does not care how
//!    this information is stored or where it is stored.
//!
//! 2. Nexus serializes the given state and stores it into CockroachDB using
//!    the `saga` and `saga_node_event` tables.
//!
//! After a crash, Nexus is then responsible for:
//!
//! 1. Identifying what sagas were in progress before the crash,
//! 2. Loading all the information about them from the database (namely, the
//!    `saga` record and the full saga log in the form of records from the
//!    `saga_node_event` table), and
//! 3. Providing all this information to Steno so that it can resume running the
//!    saga.
//!
//!
//! ## Saga recovery: not just at startup
//!
//! So far, this is fairly straightforward.  What makes it tricky is that there
//! are situations where we want to carry out saga recovery after Nexus has
//! already started and potentially recovered other sagas and started its own
//! sagas.  Specifically, when a Nexus instance gets **expunged** (removed
//! permanently), it may have outstanding sagas that need to be re-assigned to
//! another Nexus instance, which then needs to resume them.  To do this, we run
//! saga recovery in a Nexus background task so that it runs both periodically
//! and on-demand when activated.  (This approach is also useful for other
//! reasons, like retrying recovery for sagas whose recovery failed due to
//! transient errors.)
//!
//! Why does this make things tricky?  When Nexus goes to identify what sagas
//! it needs to recover, it lists sagas that are (1) assigned to it (as opposed
//! to a different Nexus) and (2) not yet finished.  But that could include
//! sagas in one of three groups:
//!
//! 1. Sagas from a previous Nexus [Unix process] lifetime that have not yet
//!    been recovered in this lifetime.  These **should** be recovered.
//! 2. Sagas from a previous Nexus [Unix process] lifetime that have already
//!    been recovered in this lifetime.  These **should not** be recovered.
//! 3. Sagas that were created in this Nexus lifetime.  These **should not** be
//!    recovered.
//!
//! There are a bunch of ways to attack this problem.  We do it by keeping track
//! in-memory of the set of sagas that might be running in the current process
//! and then ignoring those when we do recovery.  Even this is easier said than
//! done!  It's easy enough to insert new sagas into the set whenever a saga is
//! successfully recovered as well as any time a saga is created for the first
//! time (though that requires a structure that's modifiable from multiple
//! different contexts).  But to avoid this set growing unbounded, we should
//! remove entries when a saga finishes running.  When exactly can we do that?
//! We have to be careful of the intrinsic race between when the recovery
//! process queries the database to list candidate sagas for recovery (i.e.,
//! unfinished sagas assigned to this Nexus) and when it checks the set of sagas
//! that should be ignored.  Suppose a saga is running, the recovery process
//! finds it, then the saga finishes, it gets removed from the set, and then the
//! recovery process checks the set.  We'll think it wasn't running and start it
//! again -- very bad.  We can't remove anything from the set until we know that
//! the saga recovery task _doesn't_ have a stale list of candidate sagas to be
//! recovered.
//!
//! This constraint suggests the solution: the set will be owned and managed
//! entirely by the task that's doing saga recovery.  We'll use a channel to
//! trigger inserts when sagas are created elsewhere in Nexus.  What about
//! deletes?  The recovery process can actually figure out on its own when a
//! saga can be removed: if a saga was _not_ in the list of candidates to be
//! recovered, then that means it's finished, and that means it can be deleted
//! from the set.  Care must be taken to process things in the right order, but
//! in the end it's pretty simple: the recovery process first fetches the
//! candidate list of sagas, then processes all insertions that have come in
//! over the channel and adds them to the set, then compares the set against the
//! candidate list.  Sagas in the set and not in the list can be removed.  Sagas
//! in the list and not in the set must be recovered.  Sagas in both the set and
//! the list may or may not still be running, but they were running at some
//! point during this recovery pass and they can be safely ignored until the
//! next pass.

pub use task::SagaRecovery;

mod recovery;
mod status;
mod task;

#[cfg(test)]
mod test {
    use super::*;
    use futures::FutureExt;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeMap;
    use steno::SagaId;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    // Test the following structures used together:
    //
    // - RestState
    // - Plan
    // - ExecutionSummary
    // - Report
    //
    // The first three of these are used in a loop together and their exposed
    // functionality is largely in terms of each other.  That makes them a
    // little harder to test in isolation, but we can test them pretty
    // well together.
    #[tokio::test]
    async fn test_basic() {
        let logctx = test_setup_log("saga_recovery_basic");
        let log = &logctx.log;

        // Start with a blank slate.
        let mut report = status::Report::new();
        let mut rest_state = recovery::RestState::new();
        let initial_rest_state = rest_state.clone();

        // Now, go through a no-op recovery.
        let (plan, summary, last_pass_result) = do_recovery_pass(
            log,
            &mut rest_state,
            Vec::new(),
            Vec::new(),
            &|_| Ok(()),
        );
        assert_eq!(last_pass_result.nfound, 0);
        assert_eq!(last_pass_result.nskipped, 0);
        assert_eq!(last_pass_result.nremoved, 0);
        assert_eq!(rest_state, initial_rest_state);
        report.update_after_pass(&plan, summary);

        // Great.  Now go through a somewhat general case of recovery:
        // - start two sagas normally (i.e., as though they had been started
        //   elsewhere in Nexus)
        //   - one that also appears in the database
        //     (this is the case where Nexus listed sagas after this new saga
        //     started)
        //   - one that does not appear in the database
        //     (this is the case where Nexus listed sagas before this new saga
        //     started)
        // - at the same time, create two sagas that need to be recovered:
        //   - one where recovery succeeds
        //   - one where recovery fails
        let sagas_started = make_saga_ids(2);
        let sagas_to_recover = make_saga_ids(2);
        let db_sagas = {
            let mut db_sagas = sagas_to_recover.clone();
            db_sagas.push(sagas_started[0]);
            db_sagas
        };
        let ndb_sagas = db_sagas.len();
        let (plan, summary, last_pass_result) = do_recovery_pass(
            log,
            &mut rest_state,
            sagas_started,
            db_sagas,
            &|s| {
                if s == sagas_to_recover[1] {
                    Ok(())
                } else {
                    Err(Error::internal_error("test error"))
                }
            },
        );
        assert_eq!(ndb_sagas, last_pass_result.nfound);
        assert_eq!(1, last_pass_result.nskipped);
        assert_eq!(0, last_pass_result.nremoved);
        assert_eq!(1, last_pass_result.nrecovered);
        assert_eq!(1, last_pass_result.nfailed);
        assert_eq!(
            vec![sagas_to_recover[1]],
            summary.sagas_recovered_successfully().collect::<Vec<_>>()
        );
        report.update_after_pass(&plan, summary);
        assert_eq!(report.recent_recoveries.ring.len(), 1);
        assert_eq!(
            report.recent_recoveries.ring[0].saga_id,
            sagas_to_recover[1]
        );
        assert_eq!(report.recent_failures.ring.len(), 1);
        assert_eq!(report.recent_failures.ring[0].saga_id, sagas_to_recover[0]);

        // XXX-dap working on this test -- see comment in the other file for
        // some cases to cover, and also think through it from first principles.
        // e.g., another pass should exercise cases where an ambiguous saga
        // actually does finish and one where it doesn't?
    }

    const FAKE_SEC_ID: &str = "03082281-fb2e-4bfd-bce3-997c89a0db2d";
    pub fn make_fake_saga(saga_id: SagaId) -> nexus_db_model::Saga {
        let sec_id =
            nexus_db_model::SecId::from(FAKE_SEC_ID.parse::<Uuid>().unwrap());
        nexus_db_model::Saga::new(
            sec_id,
            steno::SagaCreateParams {
                id: saga_id,
                name: steno::SagaName::new("dummy"),
                state: steno::SagaCachedState::Running,
                dag: serde_json::Value::Null,
            },
        )
    }

    pub fn make_saga_ids(count: usize) -> Vec<SagaId> {
        let mut rv = Vec::with_capacity(count);
        for _ in 0..count {
            rv.push(SagaId(Uuid::new_v4()));
        }
        // Essentially by coincidence, the values we're checking against
        // are going to be sorted.  Sort this here for convenience.
        rv.sort();
        rv
    }

    /// Simulates one recovery pass
    fn do_recovery_pass(
        log: &slog::Logger,
        rest_state: &mut recovery::RestState,
        new_sagas_started: Vec<SagaId>,
        sagas_found: Vec<SagaId>,
        recovery_result: &dyn Fn(SagaId) -> Result<(), Error>,
    ) -> (recovery::Plan, recovery::ExecutionSummary, status::LastPassSuccess)
    {
        // Simulate processing messages that the `new_sagas_started` sagas just
        // started.
        let (tx, mut rx) = mpsc::unbounded_channel();
        for saga_id in new_sagas_started {
            tx.send(saga_id).unwrap();
        }
        rest_state.update_started_sagas(log, &mut rx);

        // Generate fake database records for the sagas that the caller wants to
        // pretend show up as running in the database.
        let expected_nfound = sagas_found.len();
        let fake_sagas_found: BTreeMap<_, _> = sagas_found
            .into_iter()
            .map(|saga_id| {
                let saga = make_fake_saga(saga_id);
                (saga_id, saga)
            })
            .collect();

        // Start the recovery pass by planning what to do.
        let plan = recovery::Plan::new(log, &rest_state, fake_sagas_found);

        // Simulate execution using the callback to determine whether recovery
        // for each saga succeeds or not.
        //
        // There are a lot of ways we could interleave execution here.  But in
        // practice, the implementation we care about does these all serially.
        // So that's what we test here.
        let mut summary_builder = recovery::ExecutionSummaryBuilder::new();
        let mut nok = 0;
        let mut nerrors = 0;
        for (saga_id, saga) in plan.sagas_needing_recovery() {
            let saga_log = log.new(o!(
                "saga_name" => saga.name.clone(),
                "saga_id" => saga_id.to_string(),
            ));

            summary_builder.saga_recovery_start(*saga_id, saga_log);
            match recovery_result(*saga_id) {
                Ok(()) => {
                    nok += 1;
                    summary_builder.saga_recovery_success(
                        *saga_id,
                        futures::future::ready(Ok(())).boxed(),
                    );
                }
                Err(error) => {
                    nerrors += 1;
                    summary_builder.saga_recovery_failure(*saga_id, &error);
                }
            }
        }

        let summary = summary_builder.build();
        let last_pass = status::LastPassSuccess::new(&plan, &summary);
        assert_eq!(last_pass.nfound, expected_nfound);
        assert_eq!(last_pass.nrecovered, nok);
        assert_eq!(last_pass.nfailed, nerrors);

        // We can't tell from the information we have how many were skipped,
        // removed, or ambiguous.  The caller verifies that.
        (plan, summary, last_pass)
    }
}
