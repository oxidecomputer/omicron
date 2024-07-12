// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga recovery bookkeeping
//!
//! If you're reading this, you first want to read the big block comment in the
//! saga recovery background task.  It explains important background about what
//! we're trying to do here.  The rest of this comment assumes you've read all
//! that.
//!
//! ## Saga recovery passes
//!
//! For the reasons mentioned in that comment, saga recovery is done in
//! **passes**.  The state that's kept between passes (when the task is "at
//! rest") is called the [`RestState`].
//!
//! Each saga recovery pass looks like this:
//!
//! 1. Start with initial [`RestState`] and [`Report`].
//! 2. List in-progress sagas from the database.
//! 3. Collect list of sagas that have been started by the rest of Nexus.
//! 4. Use [`Plan::new()`] to construct a plan.  The plan examines all the sagas
//!    reported by the database as well as all the sagas we knew about before
//!    and determines for each one exactly what's needed.  Each saga falls into
//!    one of a few buckets:
//!    * It's clearly not running, but should be, so it needs to be recovered.
//!    * It's clearly running, so it does not need to be recovered.
//!    * It has finished running and we can stop keeping track of it.
//!    * It _may_ be running but we cannot tell because of the intrinsic race
//!      between steps 2 and 3 above.  We'll keep track of these and resolve the
//!      ambiguity on the next pass.
//! 5. Carry out recovery for whatever sagas need to be recovered.  Use
//!    [`ExecutionBuilder::new()`] to construct a description of what happened.
//! 6. Update the [`RestState`] and [`Report`] to reflect what happened in this
//!    pass.
//!
//! This process can be repeated forever, as often as wanted, but should not be
//! run concurrently.
//!
//! ## Saga recovery task vs. this crate
//!
//! This process is driven by the caller (the saga recovery background task),
//! with helpers provided by this crate:
//!
//! ```text
//!          Saga recovery task        |      nexus-saga-recovery crate
//!     ------------------------------------------------------------------------
//!                                    |
//!      1. initial `RestState` and   ---> provides `RestState`, `Report`
//!         `Report`                   |
//!                                    |
//!      2. list in-progress sagas     |
//!                                    |
//!      3. collect list of sagas  ------> use RestState::update_started_sagas()
//!         started by Nexus           |
//!                                    |
//!      4. make a plan  ----------------> use Plan::new()
//!                                    |   This is where all the decisions
//!                                    |   about saga recovery get made.
//!                                    |
//!      5. follow the plan -------------> use Plan::sagas_needing_recovery()
//!                                    |
//!         fetch details from db      |
//!         load sagas into Steno      |
//!                                    |   use ExecutionBuilder::new() to report
//!                                    |   what's going on
//!                                    |
//!      6. update `RestState` and ----->  use `RestState::update_after_pass()`
//!         `Report`                   |   and `Report::update_after_pass()`
//! ```
//!
//! We do it this way to separate all the tricky planning logic from the
//! mechanics of loading saga state from the database and handing it over to
//! Steno (which is simple by comparison).  This crate handles the planning and
//! reporting.  The saga recovery task handles the database/Steno stuff.  This
//! is an example of the ["plan-execute" pattern][1] and it makes it much easier
//! for us to exercise all the different cases in automated tests.  It also
//! makes it easy to keep status objects for runtime observability and
//! debugging.  These get exposed to `omdb` and should also be visible in core
//! files.
//!
//! [1]: https://mmapped.blog/posts/29-plan-execute

mod recovery;
mod status;

pub use recovery::Execution;
pub use recovery::ExecutionBuilder;
pub use recovery::Plan;
pub use recovery::RestState;
pub use status::DebuggingHistory;
pub use status::LastPass;
pub use status::LastPassSuccess;
pub use status::RecoveryFailure;
pub use status::RecoverySuccess;
pub use status::Report;

#[cfg(test)]
mod test {
    use super::*;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev::test_setup_log;
    use slog::o;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use steno::SagaId;
    use tokio::sync::mpsc;
    use uuid::Uuid;

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

    /// Simple simulator for saga recovery state
    ///
    /// This type exposes functions to simulate things that would happen in
    /// Nexus (e.g., saga started, saga finished, etc.).  It keeps track of
    /// what little simulated database state and in-memory state is required to
    /// exercise all the bookkeeping in this crate.
    struct Simulator {
        log: slog::Logger,
        rest_state: recovery::RestState,
        started_sagas: Vec<SagaId>,
        db_list: BTreeMap<SagaId, nexus_db_model::Saga>,
        snapshot_db_list: Option<BTreeMap<SagaId, nexus_db_model::Saga>>,
        injected_recovery_errors: BTreeSet<SagaId>,
    }

    impl Simulator {
        pub fn new(log: slog::Logger) -> Simulator {
            Simulator {
                log,
                rest_state: recovery::RestState::new(),
                started_sagas: Vec::new(),
                db_list: BTreeMap::new(),
                snapshot_db_list: None,
                injected_recovery_errors: BTreeSet::new(),
            }
        }

        /// Pretend that a particular saga was running in a previous Nexus
        /// lifetime (and so needs to be recovered).
        pub fn sim_previously_running_saga(&mut self) -> SagaId {
            let saga_id = SagaId(Uuid::new_v4());
            self.db_list.insert(saga_id, make_fake_saga(saga_id));
            saga_id
        }

        /// Pretend that Nexus started a new saga (e.g., in response to an API
        /// request)
        pub fn sim_normal_saga_start(&mut self) -> SagaId {
            let saga_id = SagaId(Uuid::new_v4());
            self.db_list.insert(saga_id, make_fake_saga(saga_id));
            self.started_sagas.push(saga_id);
            saga_id
        }

        /// Pretend that Nexus finished running the given saga
        pub fn sim_normal_saga_done(&mut self, saga_id: SagaId) {
            assert!(
                self.db_list.remove(&saga_id).is_some(),
                "simulated saga finished, but it wasn't running"
            );
        }

        /// Configure simulation so that recovery for the specified saga will
        /// succeed or fail, depending on `fail`.  This will affect all recovery
        /// passes until the function is called again with a different value.
        ///
        /// If this function is not called for a saga, the default behavior is
        /// that recovery succeeds.
        pub fn sim_config_recovery_result(
            &mut self,
            saga_id: SagaId,
            fail: bool,
        ) {
            if fail {
                self.injected_recovery_errors.insert(saga_id);
            } else {
                self.injected_recovery_errors.remove(&saga_id);
            }
        }

        /// Snapshot the simulated database state and use that state for the
        /// next recovery pass.
        ///
        /// As an example, this can be used to exercise both sides of the race
        /// between Nexus starting a saga and listing in-progress sagas.  If you
        /// want to test "listing in-progress" happens first, use this function
        /// to snapshot the database state, then start a saga, and then do a
        /// recovery pass.  That recovery pass will act on the snapshotted
        /// database state.
        ///
        /// After the next recovery pass, the snapshotted state will be removed.
        /// The _next_ recovery pass will use the latest database state unless
        /// this function is called again.
        pub fn snapshot_db(&mut self) {
            assert!(
                self.snapshot_db_list.is_none(),
                "multiple snapshots created between recovery passes"
            );
            self.snapshot_db_list = Some(self.db_list.clone());
        }

        /// Simulate a saga recovery pass
        pub fn sim_recovery_pass(
            &mut self,
        ) -> (recovery::Plan, recovery::Execution, status::LastPassSuccess, usize)
        {
            let log = &self.log;

            // Simulate processing messages that the `new_sagas_started` sagas
            // just started.
            let nstarted = self.started_sagas.len();
            let (tx, mut rx) = mpsc::unbounded_channel();
            for saga_id in self.started_sagas.drain(..) {
                tx.send(saga_id).unwrap();
            }
            self.rest_state.update_started_sagas(log, &mut rx);

            // Start the recovery pass by planning what to do.
            let db_sagas = self
                .snapshot_db_list
                .take()
                .unwrap_or_else(|| self.db_list.clone());
            let plan = recovery::Plan::new(log, &self.rest_state, db_sagas);

            // Simulate execution using the callback to determine whether
            // recovery for each saga succeeds or not.
            //
            // There are a lot of ways we could interleave execution here.  But
            // in practice, the implementation we care about does these all
            // serially.  So that's what we test here.
            let mut execution_builder = recovery::ExecutionBuilder::new();
            let mut nok = 0;
            let mut nerrors = 0;
            for (saga_id, saga) in plan.sagas_needing_recovery() {
                let saga_log = log.new(o!(
                    "saga_name" => saga.name.clone(),
                    "saga_id" => saga_id.to_string(),
                ));

                execution_builder.saga_recovery_start(*saga_id, saga_log);
                if self.injected_recovery_errors.contains(saga_id) {
                    nerrors += 1;
                    execution_builder.saga_recovery_failure(
                        *saga_id,
                        &Error::internal_error("test error"),
                    );
                } else {
                    nok += 1;
                    execution_builder.saga_recovery_success(*saga_id);
                }
            }

            let execution = execution_builder.build();
            let last_pass = status::LastPassSuccess::new(&plan, &execution);
            assert_eq!(last_pass.nrecovered, nok);
            assert_eq!(last_pass.nfailed, nerrors);

            self.rest_state.update_after_pass(&plan, &execution);

            // We can't tell from the information we have how many were skipped,
            // removed, or ambiguous.  The caller verifies that.
            (plan, execution, last_pass, nstarted)
        }
    }

    // End-to-end test of the saga recovery bookkeeping, which is basically
    // everything *except* loading the sagas from the database and restoring
    // them in Steno.  See the block comment above -- that stuff lives outside
    // of this crate.
    //
    // Tests the following structures used together:
    //
    // - RestState
    // - Plan
    // - Execution
    // - Report
    //
    // These are hard to test in isolation since they're intended to be used
    // together in a loop (and so don't export public interfaces for mucking
    // with internal).
    #[tokio::test]
    async fn test_basic() {
        let logctx = test_setup_log("saga_recovery_basic");
        let log = &logctx.log;

        // Start with a blank slate.
        let mut sim = Simulator::new(log.clone());
        let initial_rest_state = sim.rest_state.clone();
        let mut report = status::Report::new();

        //
        // Now, go through a no-op recovery.
        //
        let (plan, execution, last_pass_result, nstarted) =
            sim.sim_recovery_pass();
        assert_eq!(last_pass_result.nfound, 0);
        assert_eq!(last_pass_result.nskipped, 0);
        assert_eq!(last_pass_result.nremoved, 0);
        assert_eq!(sim.rest_state, initial_rest_state);
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 0);
        assert_eq!(report.ntotal_failures, 0);
        assert_eq!(report.ntotal_started, 0);
        assert_eq!(report.ntotal_finished, 0);

        //
        // Now, go through a somewhat general case of recovery.
        //
        // First, add a couple of sagas that just showed up in the database.
        // This covers the case of sagas that were either from a previous Nexus
        // lifetime or re-assigned from some other Nexus that has been expunged.
        // We create two so we can exercise success and failure cases for
        // recovery.
        //
        let saga_recover_ok = sim.sim_previously_running_saga();
        let saga_recover_fail = sim.sim_previously_running_saga();
        sim.sim_config_recovery_result(saga_recover_fail, true);

        // Simulate Nexus starting a couple of sagas in the usual way.  This one
        // will appear in the database as well as in our set of sagas started.
        let saga_started_normally_1 = sim.sim_normal_saga_start();
        let saga_started_normally_2 = sim.sim_normal_saga_start();

        // Start a saga and then finish it immediately.  This is a tricky case
        // because the recovery pass will see that it started, but not see in
        // the database, and it won't be able to tell if it finished or just
        // started.
        let saga_started_and_finished = sim.sim_normal_saga_start();
        sim.sim_normal_saga_done(saga_started_and_finished);

        // Take a snapshot.  Subsequent changes will not affect the database
        // state that's used for the next recovery pass.  We'll use this to
        // simulate Nexus having started a saga immediately after the database
        // listing that's used for a recovery pass.
        sim.snapshot_db();
        let saga_started_after_listing = sim.sim_normal_saga_start();

        // We're finally ready to carry out a simulation pass and verify what
        // happened with each of these sagas.
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        // In the end, there should have been four sagas found in the database:
        // all of the above except for the one that finished.
        assert_eq!(4, last_pass_success.nfound);
        // Two of these needed to be recovered (because they had been previously
        // running).  One succeeded.
        assert_eq!(1, last_pass_success.nrecovered);
        assert_eq!(1, execution.succeeded.len());
        assert_eq!(saga_recover_ok, execution.succeeded[0].saga_id);

        assert_eq!(1, last_pass_success.nfailed);
        assert_eq!(1, execution.failed.len());
        assert_eq!(saga_recover_fail, execution.failed[0].saga_id);
        // Two sagas should have been found in the database that corresponded to
        // sagas that had been started normally and did not need to be
        // recovered.  They would have been skipped.
        assert_eq!(2, last_pass_success.nskipped);
        assert_eq!(2, plan.nskipped());
        // No sagas were removed yet -- we can't do that with only one pass.
        assert_eq!(0, last_pass_success.nremoved);
        assert_eq!(0, plan.ninferred_done());
        // From what the pass could tell, two sagas might be done: the one that
        // actually finished and the one that started after the database
        // listing.
        let mut maybe_done = plan.sagas_maybe_done().collect::<Vec<_>>();
        maybe_done.sort();
        let mut expected_maybe_done =
            vec![saga_started_and_finished, saga_started_after_listing];
        expected_maybe_done.sort();
        assert_eq!(maybe_done, expected_maybe_done);
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 1);
        assert_eq!(report.ntotal_failures, 1);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 0);

        //
        // Change nothing and run another pass.
        // This pass allows the system to determine that some sagas are now
        // done.
        //
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        // There's now five sagas in-progress in the database: the same four as
        // above, plus the one that was started after the snapshot.
        assert_eq!(5, last_pass_success.nfound);
        // One of these needs to be recovered because it failed last time.  It
        // fails again this time.
        assert_eq!(0, last_pass_success.nrecovered);
        assert_eq!(0, execution.succeeded.len());
        assert_eq!(1, last_pass_success.nfailed);
        assert_eq!(1, execution.failed.len());
        assert_eq!(saga_recover_fail, execution.failed[0].saga_id);
        // This time, four sagas should have been found in the database that
        // correspond to sagas that were started normally and did not need to be
        // recovered: the two from last time, plus the one that was recovered,
        // plus the one that was started after the previous snapshot.  These
        // would have been skipped.
        assert_eq!(4, last_pass_success.nskipped);
        assert_eq!(4, plan.nskipped());
        // This time, the saga that was actually finished should have been
        // removed.  We could tell this time.
        assert_eq!(1, last_pass_success.nremoved);
        assert_eq!(
            vec![saga_started_and_finished],
            plan.sagas_inferred_done().collect::<Vec<_>>()
        );
        // This time, there are no sagas that might be done.  The one we thought
        // might have been done last time is now clearly running because it
        // appears in this database listing.
        assert_eq!(0, plan.sagas_maybe_done().count());
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 1);
        assert_eq!(report.ntotal_failures, 2);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 1);

        //
        // Again, change nothing and run another pass.  This should be a steady
        // state: if we keep running passes from here, nothing should change.
        //
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        // Same as above.
        assert_eq!(5, last_pass_success.nfound);
        assert_eq!(0, last_pass_success.nrecovered);
        assert_eq!(0, execution.succeeded.len());
        assert_eq!(1, last_pass_success.nfailed);
        assert_eq!(1, execution.failed.len());
        assert_eq!(saga_recover_fail, execution.failed[0].saga_id);
        assert_eq!(4, last_pass_success.nskipped);
        assert_eq!(4, plan.nskipped());
        assert_eq!(0, plan.sagas_maybe_done().count());
        // Here's the only thing that differs from last time.  We removed a saga
        // before, so this time there's nothing to remove.
        // removed.  We could tell this time.
        assert_eq!(0, last_pass_success.nremoved);
        assert_eq!(0, plan.sagas_inferred_done().count());
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 1);
        assert_eq!(report.ntotal_failures, 3);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 1);

        //
        // Once more and make sure nothing changes.
        //
        let previous_rest_state = sim.rest_state.clone();
        let previous_last_pass_success = last_pass_success.clone();
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        assert_eq!(previous_rest_state, sim.rest_state);
        assert_eq!(previous_last_pass_success, last_pass_success);
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 1);
        assert_eq!(report.ntotal_failures, 4);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 1);

        //
        // This time, fix that saga whose recovery has been failing.
        //
        sim.sim_config_recovery_result(saga_recover_fail, false);
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        // Same as above.
        assert_eq!(5, last_pass_success.nfound);
        assert_eq!(4, last_pass_success.nskipped);
        assert_eq!(4, plan.nskipped());
        assert_eq!(0, last_pass_success.nremoved);
        assert_eq!(0, plan.sagas_inferred_done().count());
        assert_eq!(0, plan.sagas_maybe_done().count());
        // Here's what's different from before.
        assert_eq!(1, last_pass_success.nrecovered);
        assert_eq!(1, execution.succeeded.len());
        assert_eq!(saga_recover_fail, execution.succeeded[0].saga_id);
        assert_eq!(0, last_pass_success.nfailed);
        assert_eq!(0, execution.failed.len());
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 2);
        assert_eq!(report.ntotal_failures, 4);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 1);

        //
        // After the next pass, we should have one more saga that seems to be
        // running.
        //
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        // Same as above.
        assert_eq!(5, last_pass_success.nfound);
        assert_eq!(0, last_pass_success.nremoved);
        assert_eq!(0, plan.sagas_inferred_done().count());
        assert_eq!(0, plan.sagas_maybe_done().count());
        assert_eq!(0, last_pass_success.nfailed);
        assert_eq!(0, execution.failed.len());
        // Here's what's different from before.
        assert_eq!(5, last_pass_success.nskipped);
        assert_eq!(5, plan.nskipped());
        assert_eq!(0, last_pass_success.nrecovered);
        assert_eq!(0, execution.succeeded.len());
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 2);
        assert_eq!(report.ntotal_failures, 4);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 1);

        //
        // With another pass, nothing should differ.
        //
        let previous_rest_state = sim.rest_state.clone();
        let previous_last_pass_success = last_pass_success.clone();
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        assert_eq!(previous_rest_state, sim.rest_state);
        assert_eq!(previous_last_pass_success, last_pass_success);
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 2);
        assert_eq!(report.ntotal_failures, 4);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 1);

        //
        // Now let's complete a couple of different sagas.
        // It'll take two passes for the system to be sure they're done.
        //
        sim.sim_normal_saga_done(saga_started_normally_1);
        sim.sim_normal_saga_done(saga_started_after_listing);
        sim.sim_normal_saga_done(saga_recover_fail);
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        assert_eq!(2, last_pass_success.nfound);
        assert_eq!(0, last_pass_success.nremoved);
        assert_eq!(0, plan.sagas_inferred_done().count());
        assert_eq!(3, plan.sagas_maybe_done().count());
        assert_eq!(0, last_pass_success.nfailed);
        assert_eq!(0, execution.failed.len());
        assert_eq!(2, last_pass_success.nskipped);
        assert_eq!(2, plan.nskipped());
        assert_eq!(0, last_pass_success.nrecovered);
        assert_eq!(0, execution.succeeded.len());
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 2);
        assert_eq!(report.ntotal_failures, 4);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 1);

        //
        // With another pass, we can remove those three that finished.
        //
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        assert_eq!(2, last_pass_success.nfound);
        assert_eq!(3, last_pass_success.nremoved);
        assert_eq!(3, plan.sagas_inferred_done().count());
        assert_eq!(0, plan.sagas_maybe_done().count());
        assert_eq!(0, last_pass_success.nfailed);
        assert_eq!(0, execution.failed.len());
        assert_eq!(2, last_pass_success.nskipped);
        assert_eq!(2, plan.nskipped());
        assert_eq!(0, last_pass_success.nrecovered);
        assert_eq!(0, execution.succeeded.len());
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 2);
        assert_eq!(report.ntotal_failures, 4);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 4);

        //
        // Finish the last two sagas.
        //
        sim.sim_normal_saga_done(saga_started_normally_2);
        sim.sim_normal_saga_done(saga_recover_ok);
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        assert_eq!(0, last_pass_success.nfound);
        assert_eq!(0, last_pass_success.nremoved);
        assert_eq!(0, plan.sagas_inferred_done().count());
        assert_eq!(2, plan.sagas_maybe_done().count());
        assert_eq!(0, last_pass_success.nfailed);
        assert_eq!(0, execution.failed.len());
        assert_eq!(0, last_pass_success.nskipped);
        assert_eq!(0, plan.nskipped());
        assert_eq!(0, last_pass_success.nrecovered);
        assert_eq!(0, execution.succeeded.len());
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 2);
        assert_eq!(report.ntotal_failures, 4);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 4);

        //
        // With another pass, remove those last two.
        //
        let (plan, execution, last_pass_success, nstarted) =
            sim.sim_recovery_pass();
        assert_eq!(0, last_pass_success.nfound);
        assert_eq!(2, last_pass_success.nremoved);
        assert_eq!(2, plan.sagas_inferred_done().count());
        assert_eq!(0, plan.sagas_maybe_done().count());
        assert_eq!(0, last_pass_success.nfailed);
        assert_eq!(0, execution.failed.len());
        assert_eq!(0, last_pass_success.nskipped);
        assert_eq!(0, plan.nskipped());
        assert_eq!(0, last_pass_success.nrecovered);
        assert_eq!(0, execution.succeeded.len());
        report.update_after_pass(&plan, execution, nstarted);
        assert_eq!(report.ntotal_recovered, 2);
        assert_eq!(report.ntotal_failures, 4);
        assert_eq!(report.ntotal_started, 4);
        assert_eq!(report.ntotal_finished, 6);

        // At this point, the rest state should match our existing rest state.
        // This is an extra check to make sure we're not leaking memory related
        // to old sagas.
        assert_eq!(sim.rest_state, initial_rest_state);

        //
        // At this point, we've exercised:
        // - recovering a saga that we didn't start
        //   (basic "recovery" path after a crash, plus re-assignment path)
        // - retrying a saga whose recovery failed (multiple times)
        // - *not* trying to recover:
        //   - a newly-started saga
        //   - a saga that was recovered before
        // - not hanging on forever to sagas that have finished
        // - the edge case built into our implementation where we learned that a
        //   saga was started before it appeared in the database
        //
    }
}
