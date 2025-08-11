// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that help manage Nexus quiesce state

use crate::internal_api::views::PendingSagaInfo;
use chrono::Utc;
use futures::FutureExt;
use futures::future::BoxFuture;
use iddqd::IdOrdMap;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use slog::Logger;
use slog::info;
use slog::o;
use slog_error_chain::InlineErrorChain;
use steno::SagaResult;
use thiserror::Error;
use tokio::sync::watch;

/// Policy determining whether new sagas are allowed to be started
///
/// This is used by Nexus quiesce to disallow creation of new sagas when we're
/// trying to quiesce Nexus.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SagasAllowed {
    /// New sagas may be started (normal condition)
    Allowed,
    /// New sagas may not be started (happens during quiesce)
    Disallowed,
}

#[derive(Debug, Error)]
#[error(
    "saga creation and reassignment are disallowed (Nexus quiescing/quiesced)"
)]
pub struct NoSagasAllowedError;
impl From<NoSagasAllowedError> for Error {
    fn from(value: NoSagasAllowedError) -> Self {
        Error::unavail(&value.to_string())
    }
}

/// Describes both the configuration (whether sagas are allowed to be created)
/// and the state (how many sagas are pending) for the purpose of quiescing
/// Nexus.
// Both configuration and state must be combined (under the same watch channel)
// to avoid races in detecting quiesce.  We want the quiescer to be able to say
// that if `sagas_allowed` is `Disallowed` and there are no sagas running, then
// sagas are quiesced.  But there's no way to guarantee that if these are stored
// in separate channels.
//
// This is used by three consumers:
//
// 1. The SagaExecutor uses this in deciding whether to allow creating new
//    sagas and for keeping track of running sagas
//
// 2. The saga recovery background task uses this to track recovered sagas
//
// 3. The blueprint execution background task uses this to decide whether to
//    attempt to re-assign ourselves sagas from expunged Nexus instances.
//
// All of this ensures that once we begin quiescing, we can reliably tell when
// there are no more sagas running *and* that there will never be any more
// sagas running within this process.
#[derive(Debug, Clone)]
pub struct SagaQuiesceHandle {
    log: Logger,

    // As the name implies, the SagaQuiesceHandle itself is a (cloneable) handle
    // to the real underlying state.  The real state is stored in this watch
    // channel.  In practice, we use this more like a Mutex (or even an RwLock).
    // But the `watch` channel gives us two advantages over Mutex or RwLock:
    //
    // (1) It's hard to hang onto the "lock" for too long, at least as a writer,
    //     because you only ever have the write lock inside a closure (e.g.,
    //     `send_modify`).  That closure can't be async.  This is a good thing.
    //     It means we only hold the write lock for very brief periods while we
    //     mutate the data, using it to protect data and not code.
    //
    // (2) `watch::Receiver` provides a really handy `wait_for()` method` that
    //     we use in `wait_for_quiesced()`.  Besides being convenient, this
    //     would be surprisingly hard for us to implement ourselves with a
    //     `Mutex`.  Traditionally, you'd use a combination Mutex/Condvar for
    //     this.  But we'd want to use a `std` Mutex (since tokio Mutex's
    //     cancellation behavior is abysmal), but we don't want to block on a
    //     std `Condvar` in an async thread.  There are options here (e.g.,
    //     `block_on`), but they're not pleasant.
    inner: watch::Sender<SagaQuiesceInner>,
}

#[derive(Debug, Clone)]
struct SagaQuiesceInner {
    /// current policy: are we allowed to *create* new sagas?
    ///
    /// This also affects re-assigning sagas from expunged Nexus instances to
    /// ourselves.  It does **not** affect saga recovery.
    new_sagas_allowed: SagasAllowed,

    /// list of sagas we need to wait to complete before quiescing
    ///
    /// These are basically running sagas.  They may have been created in this
    /// Nexus process lifetime or created in another process and then recovered
    /// in this one.
    sagas_pending: IdOrdMap<PendingSagaInfo>,

    /// whether at least one recovery pass has successfully completed
    ///
    /// We have to track this because we can't quiesce until we know we've
    /// recovered all outstanding sagas.
    first_recovery_complete: bool,

    /// generation number for the saga reassignment
    ///
    /// This gets bumped whenever a saga reassignment operation completes that
    /// may have re-assigned us some sagas.  It's used to keep track of when
    /// we've recovered all sagas that could be assigned to us.
    reassignment_generation: Generation,

    /// whether there is a saga reassignment operation happening
    ///
    /// These operatinos may assign new sagas to Nexus that must be recovered
    /// and completed before quiescing can finish.
    reassignment_pending: bool,

    /// "saga reassignment generation number" that was "caught up to" by the
    /// last recovery pass
    ///
    /// This is used with `reassignment_generation` to help us know when we've
    /// recovered all the sagas that may have been assigned to us during a
    /// given reassignment pass.  See `reassignment_done()` for details.
    recovered_reassignment_generation: Generation,

    /// whether a saga recovery operation is ongoing, and if one is, what
    /// `reassignment_generation` was when it started
    recovery_pending: Option<Generation>,
}

impl SagaQuiesceHandle {
    pub fn new(log: Logger) -> SagaQuiesceHandle {
        let (inner, _) = watch::channel(SagaQuiesceInner {
            new_sagas_allowed: SagasAllowed::Allowed,
            sagas_pending: IdOrdMap::new(),
            first_recovery_complete: false,
            reassignment_generation: Generation::new(),
            reassignment_pending: false,
            recovered_reassignment_generation: Generation::new(),
            recovery_pending: None,
        });
        SagaQuiesceHandle { log, inner }
    }

    /// Disallow new sagas from being started or re-assigned to this Nexus
    ///
    /// This is currently a one-way trip.  Sagas cannot be un-quiesced.
    pub fn quiesce(&self) {
        // Log this before changing the config to make sure this message
        // appears before messages from code paths that saw this change.
        info!(&self.log, "starting saga quiesce");
        self.inner
            .send_modify(|q| q.new_sagas_allowed = SagasAllowed::Disallowed);
    }

    /// Returns whether sagas are fully quiesced
    pub fn is_fully_quiesced(&self) -> bool {
        self.inner.borrow().is_fully_quiesced()
    }

    /// Wait for sagas to be quiesced
    pub async fn wait_for_quiesced(&self) {
        let _ =
            self.inner.subscribe().wait_for(|q| q.is_fully_quiesced()).await;
    }

    /// Returns information about running sagas (involves a clone)
    pub fn sagas_pending(&self) -> IdOrdMap<PendingSagaInfo> {
        self.inner.borrow().sagas_pending.clone()
    }

    /// Record an operation that might assign sagas to this Nexus
    ///
    /// If reassignment is currently allowed, `f` will be invoked to potentially
    /// re-assign sagas.  `f` returns `(T, bool)`, where `T` is whatever value
    /// you want and is returned back from this function.  The boolean indicates
    /// whether any sagas may have been assigned to the current Nexus.
    ///
    /// If reassignment is currently disallowed (because Nexus is quiescing),
    /// `f` is not invoked and an error describing this condition is returned.
    ///
    /// Only one of these may be outstanding at a time.  It should not be called
    /// concurrently.  This is easy today because this is only invoked by a few
    /// callers, all in different programs, and each of these callers is in its
    /// own singleton context in its program (e.g., a Nexus background task).
    ///
    /// This function exists because quiescing must block on:
    ///
    /// - any re-assigned sagas being recovered and then completed (so we need
    ///   to know if a re-assignment may have assigned any sagas to us), and
    /// - there must be no outstanding re-assignment operations (since that
    ///   would create new sagas that we need to wait for)
    // We expose this function and not
    // `reassignment_start()`/`reassignment_done()` because it's harder to
    // mis-use (e.g., by forgetting to call `reassignment_done()`).  But we keep
    // the other two functions around because it's easier to write tests against
    // those.
    pub async fn reassign_if_possible<F, T>(
        &self,
        f: F,
    ) -> Result<T, NoSagasAllowedError>
    where
        F: AsyncFnOnce() -> (T, bool),
    {
        let in_progress = self.reassignment_start()?;
        let (result, maybe_reassigned) = f().await;
        in_progress.reassignment_done(maybe_reassigned);
        Ok(result)
    }

    /// Record that we've begun a re-assignment operation.
    ///
    /// Only one of these may be outstanding at a time.  The caller must call
    /// `reassignment_done()` before starting another one of these.
    fn reassignment_start(
        &self,
    ) -> Result<SagaReassignmentInProgress, NoSagasAllowedError> {
        let okay = self.inner.send_if_modified(|q| {
            assert!(
                !q.reassignment_pending,
                "two calls to reassignment_start() without intervening call \
                 to reassignment_done() (concurrent calls to \
                 reassign_if_possible()?)"
            );

            if q.new_sagas_allowed != SagasAllowed::Allowed {
                return false;
            }

            q.reassignment_pending = true;
            true
        });

        if okay {
            info!(&self.log, "allowing saga re-assignment pass");
            Ok(SagaReassignmentInProgress { q: self.clone() })
        } else {
            info!(&self.log, "disallowing saga re-assignment pass");
            Err(NoSagasAllowedError)
        }
    }

    /// Record that we've finished an operation that might assign new sagas to
    /// ourselves.
    fn reassignment_done(&self, maybe_reassigned: bool) {
        info!(
            &self.log,
            "saga re-assignment pass finished";
            "maybe_reassigned" => maybe_reassigned
        );
        self.inner.send_modify(|q| {
            assert!(q.reassignment_pending);
            q.reassignment_pending = false;

            // If we may have assigned new sagas to ourselves, bump the
            // generation number.  We won't quiesce until a recovery pass has
            // finished that *started* with this generation number.  So this
            // ensures that we won't quiesce until any sagas that may have been
            // assigned to us have been recovered.
            if maybe_reassigned {
                q.reassignment_generation = q.reassignment_generation.next();
            }
        });
    }

    /// Record that we've begun recovering sagas.
    ///
    /// `f(recovery)` will be invoked to proceed with recovery.  For each
    /// recovered saga, the caller should invoke
    /// `recovery.record_saga_recovery()`.  When finished, `f` returns `(T,
    /// bool)`, where `T` is whatever value you want and is returned back from
    /// this function.  The boolean indicates whether saga recovery was fully
    /// successful.  That is, the boolean should be true only if the caller is
    /// sure that any sagas that were eligible for recovery when `f` was invoked
    /// have been recovered.  (It is possible some sagas became newly eligible
    /// for recovery while `f` was running.  That's not the caller's
    /// responsibility to deal with.)
    ///
    /// Only one of these operations may be outstanding at a time.  This
    /// function should not be called concurrently.  (This is easy today because
    /// there's only one caller and it's in a Nexus background task.)
    // We expose this function and not `recovery_start()`/`recovery_done()`
    // because it's harder to mis-use (e.g., by forgetting to call
    // `recovery_done()`).  But we keep the other two functions around because
    // it's easier to write tests against those.
    pub async fn recover<F, T>(&self, f: F) -> T
    where
        F: AsyncFnOnce(&SagaRecoveryInProgress) -> (T, bool),
    {
        let in_progress = self.recovery_start();
        let (result, success) = f(&in_progress).await;
        in_progress.recovery_done(success);
        result
    }

    /// Record that we've begun recovering sagas.
    ///
    /// Only one of these may be outstanding at a time.  The caller must call
    /// `saga_recovery_done()` before starting another one of these.
    fn recovery_start(&self) -> SagaRecoveryInProgress {
        self.inner.send_modify(|q| {
            assert!(
                q.recovery_pending.is_none(),
                "recovery_start() called twice without intervening \
                 recovery_done() (concurrent calls to recover()?)",
            );
            q.recovery_pending = Some(q.reassignment_generation);
        });

        info!(&self.log, "saga recovery pass starting");
        SagaRecoveryInProgress { q: self.clone() }
    }

    /// Record that we've finished recovering sagas.
    fn recovery_done(&self, success: bool) {
        let log = self.log.clone();
        self.inner.send_modify(|q| {
            let Some(generation) = q.recovery_pending.take() else {
                panic!("cannot finish saga recovery when it was not running");
            };

            if success {
                info!(
                    &log,
                    "saga recovery pass finished";
                    "generation" => generation.to_string()
                );
                q.recovered_reassignment_generation = generation;
                q.first_recovery_complete = true;
            } else {
                info!(&log, "saga recovery pass failed");
            }
        });
    }

    /// Report that a saga has started running
    ///
    /// This fails if sagas are quiesced.
    ///
    /// Callers must also call `saga_completion_future()` to make sure it's
    /// recorded when this saga finishes.
    pub fn saga_create(
        &self,
        saga_id: steno::SagaId,
        saga_name: &steno::SagaName,
    ) -> Result<NewlyPendingSagaRef, NoSagasAllowedError> {
        let okay = self.inner.send_if_modified(|q| {
            if q.new_sagas_allowed != SagasAllowed::Allowed {
                return false;
            }

            q.sagas_pending
                .insert_unique(PendingSagaInfo {
                    saga_id,
                    saga_name: saga_name.clone(),
                    time_pending: Utc::now(),
                    recovered: false,
                })
                .expect("created saga should have unique id");
            true
        });

        if okay {
            let log = self.log.new(o!("saga_id" => saga_id.to_string()));
            info!(&log, "tracking newly created saga");
            Ok(NewlyPendingSagaRef {
                log,
                quiesce: self.inner.clone(),
                saga_id,
                init_finished: false,
            })
        } else {
            info!(
                &self.log,
                "disallowing saga creation";
                "saga_id" => saga_id.to_string()
            );
            Err(NoSagasAllowedError)
        }
    }

    /// Report that the given saga is being recovered
    ///
    /// This is analogous to `saga_created()`, but for sagas that were recovered
    /// rather than having been created within the lifetime of this Nexus
    /// process.
    ///
    /// Callers must also call `saga_completion_future()` to make sure it's
    /// recorded when this saga finishes.
    ///
    /// Callers may invoke this function more than once for a saga as long as
    /// the saga has not yet finished.  (This only has one caller, the saga
    /// recovery background task, which aready takes care to avoid recovering
    /// sagas that might possibly have finished already.)
    ///
    /// Unlike `saga_created()`, this cannot fail as a result of sagas being
    /// quiesced.  That's because a saga that *needs* to be recovered is a
    /// blocker for quiesce, whether it's running or not.  So we need to
    /// actually run and finish it.  We do still want to prevent ourselves from
    /// taking on sagas needing recovery -- that's why we fail
    /// `reassign_if_possible()` when saga creation is disallowed.
    fn record_saga_recovery(
        &self,
        saga_id: steno::SagaId,
        saga_name: &steno::SagaName,
    ) -> NewlyPendingSagaRef {
        let log = self.log.new(o!("saga_id" => saga_id.to_string()));
        info!(&log, "tracking newly recovered saga");

        self.inner.send_modify(|q| {
            // It's okay to call this more than once, so we ignore the possible
            // error from `insert_unique()`.  (`insert_overwrite()` would reset
            // the `time_pending`, which we don't want.)
            let _ = q.sagas_pending.insert_unique(PendingSagaInfo {
                saga_id,
                saga_name: saga_name.clone(),
                time_pending: Utc::now(),
                recovered: true,
            });
        });

        NewlyPendingSagaRef {
            log,
            quiesce: self.inner.clone(),
            saga_id,
            init_finished: false,
        }
    }
}

impl SagaQuiesceInner {
    /// Returns whether sagas are fully and permanently quiesced
    pub fn is_fully_quiesced(&self) -> bool {
        // No new sagas may be created
        self.new_sagas_allowed == SagasAllowed::Disallowed
            // and there are none currently running
            && self.sagas_pending.is_empty()
            // and there are none from a previous lifetime that still need to be
            // recovered
            && self.first_recovery_complete
            // and there are none that blueprint execution may have re-assigned
            // to us that have not been recovered
            && self.reassignment_generation
                <= self.recovered_reassignment_generation
            // and blueprint execution is not currently re-assigning stuff to us
            && !self.reassignment_pending
    }
}

/// Handle used to ensure that we clean up records for a pending saga
///
/// This happens in one of two ways:
///
/// 1. Normally, the caller that obtains this handle should call
///    `saga_completion_future()` once they have a future that can be used to
///    wait for the saga to finish.
///
/// 2. If this object is dropped before that happens, it's assumed that the
///    caller failed to start the saga running and the record is cleaned up
///    immediately.
#[derive(Debug)]
#[must_use = "must record the saga completion future once the saga is running"]
pub struct NewlyPendingSagaRef {
    log: Logger,
    quiesce: watch::Sender<SagaQuiesceInner>,
    saga_id: steno::SagaId,
    init_finished: bool,
}

impl NewlyPendingSagaRef {
    /// Provide a `Future` that finishes when the saga has finished running.
    ///
    /// Returns an equivalent Future.
    ///
    /// This must be called exactly once for each saga created or recovered.
    ///
    /// Internally, the returned future wraps the provided future, but allows
    /// this structure to update its state to reflect that the saga is no longer
    /// running.
    pub fn saga_completion_future(
        mut self,
        fut: BoxFuture<'static, SagaResult>,
    ) -> BoxFuture<'static, SagaResult> {
        self.init_finished = true;
        let saga_id = self.saga_id;

        // When the saga finishes, we need to update our state to reflect that
        // it's no longer running (so that if Nexus is quiescing, the quiesce
        // task knows it can proceed to the next step).  We're provided a Future
        // that we can use to wait for the saga to finish.  We also provide an
        // equivalent Future to our caller.  It'd be handy to just hook into
        // that one, but there's a hitch: our consumer is allowed to drop that
        // Future if they don't care when the saga finishes.  But we always
        // care, for the reason mentioned above.  So we need to create our own
        // task to poll the completion future that we were given and pass along
        // the result to the Future that we provide our consumer.

        // This is the task we spawn off to wait for the saga to finish and
        // update our state.  (The state update happens when `saga_ref` is
        // dropped.)
        let qwrap = self.quiesce.clone();
        let completion_task = tokio::spawn(async move {
            let rv = fut.await;
            info!(self.log, "tracked saga has finished");
            qwrap.send_modify(|q| {
                q.sagas_pending
                    .remove(&saga_id)
                    .expect("saga should have been running");
            });
            rv
        });

        // Construct the future for our caller to be notified when the saga
        // finishes.
        async move {
            match completion_task.await {
                Ok(rv) => rv,
                Err(error) => {
                    // This should be basically impossible.  A panic from the
                    // task would be a bug.  It's conceivable that it gets
                    // cancelled if we're in the middle of a shutdown of the
                    // tokio runtime itself.  That wouldn't really happen in the
                    // real Nexus but could happen as part of the test suite.
                    panic!(
                        "saga_completion_future(): failed to wait for \
                         completion task (is tokio runtime shutting down?): {}",
                        InlineErrorChain::new(&error),
                    );
                }
            }
        }
        .boxed()
    }
}

impl Drop for NewlyPendingSagaRef {
    fn drop(&mut self) {
        // If the caller never invoked saga_completion_future(), it must have
        // bailed out before having started running the saga.  Remove it from
        // the list of sagas that we're tracking.
        if !self.init_finished {
            info!(
                self.log,
                "stopping tracking saga that apparently never started"
            );

            self.quiesce.send_modify(|q| {
                q.sagas_pending.remove(&self.saga_id).unwrap_or_else(|| {
                    panic!(
                        "NewlyPendingSagaRef dropped, saga completion future \
                         not recorded, and the saga is not still pending"
                    )
                });
            });
        }
    }
}

/// Token representing that saga recovery is in-progress
///
/// Consumers **must** explicitly call `recovery_done()` before dropping this.
#[must_use = "You must call recovery_done() after recovery_start()"]
pub struct SagaRecoveryInProgress {
    q: SagaQuiesceHandle,
}

impl SagaRecoveryInProgress {
    fn recovery_done(self, success: bool) {
        self.q.recovery_done(success)
    }

    pub fn record_saga_recovery(
        &self,
        saga_id: steno::SagaId,
        saga_name: &steno::SagaName,
    ) -> NewlyPendingSagaRef {
        self.q.record_saga_recovery(saga_id, saga_name)
    }
}

/// Token representing that saga re-assignment is in-progress
///
/// Consumers **must** explicitly call `reassignment_done()` before dropping
/// this.
#[derive(Debug)]
#[must_use = "You must call reassignment_done() after reassignment_start()"]
struct SagaReassignmentInProgress {
    q: SagaQuiesceHandle,
}

impl SagaReassignmentInProgress {
    fn reassignment_done(self, maybe_reassigned: bool) {
        self.q.reassignment_done(maybe_reassigned)
    }
}

#[cfg(test)]
mod test {
    use crate::quiesce::SagaQuiesceHandle;
    use futures::FutureExt;
    use omicron_test_utils::dev::test_setup_log;
    use std::sync::LazyLock;
    use uuid::Uuid;

    static SAGA_ID: LazyLock<steno::SagaId> =
        LazyLock::new(|| steno::SagaId(Uuid::new_v4()));
    static SAGA_NAME: LazyLock<steno::SagaName> =
        LazyLock::new(|| steno::SagaName::new("test-saga"));

    fn saga_result() -> steno::SagaResult {
        let saga_id = *SAGA_ID;
        let saga_log = steno::SagaLog::new_empty(*SAGA_ID);
        steno::SagaResult {
            saga_id,
            saga_log,
            kind: Err(steno::SagaResultErr {
                error_node_name: steno::NodeName::new("dummy"),
                error_source: steno::ActionError::InjectedError,
                undo_failure: None,
            }),
        }
    }

    /// Tests that quiescing when we have never started anything completes
    /// immediately.
    #[tokio::test]
    async fn test_quiesce_noop() {
        let logctx = test_setup_log("test_quiesce_noop");
        let log = &logctx.log;

        // Set up a new handle.  Complete the first saga recovery immediately so
        // that that doesn't block quiescing.
        let qq = SagaQuiesceHandle::new(log.clone());
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);

        // It's still not fully quiesced because we haven't asked it to quiesce
        // yet.
        assert!(qq.sagas_pending().is_empty());
        assert!(!qq.is_fully_quiesced());

        // Now start quiescing.  It should immediately report itself as
        // quiesced.  There's nothing asynchronous in this path.  (It would be
        // okay if there were.)
        qq.quiesce();
        assert!(qq.is_fully_quiesced());

        // It's not allowed to create sagas or begin re-assignment after
        // quiescing has started, let alone finished.
        let _ = qq
            .saga_create(*SAGA_ID, &SAGA_NAME)
            .expect_err("cannot create saga after quiescing started");
        let _ = qq
            .reassignment_start()
            .expect_err("cannot start re-assignment after quiescing started");

        // Waiting for quiesce should complete immediately.
        qq.wait_for_quiesced().await;
        assert!(qq.is_fully_quiesced());

        logctx.cleanup_successful();
    }

    /// Test: quiescing does not block on sagas that never started
    #[tokio::test]
    async fn test_quiesce_no_block_on_sagas_not_started() {
        let logctx =
            test_setup_log("test_quiesce_no_block_on_sagas_not_started");
        let log = &logctx.log;

        // Set up a new handle.  Complete the first saga recovery immediately so
        // that that doesn't block quiescing.
        let qq = SagaQuiesceHandle::new(log.clone());
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);

        // Start recording a new saga being created.
        let started = qq
            .saga_create(*SAGA_ID, &SAGA_NAME)
            .expect("create saga while not quiesced");
        assert!(!qq.sagas_pending().is_empty());

        // Start quiescing.
        qq.quiesce();
        assert!(!qq.is_fully_quiesced());

        // Dropping the returned handle is as good as completing the saga.
        drop(started);
        assert!(qq.sagas_pending().is_empty());
        assert!(qq.is_fully_quiesced());
        qq.wait_for_quiesced().await;
        assert!(qq.is_fully_quiesced());

        logctx.cleanup_successful();
    }

    /// Test: quiescing blocks on sagas created in this process
    #[tokio::test]
    async fn test_quiesce_block_on_saga_started() {
        let logctx = test_setup_log("test_quiesce_block_on_saga_started");
        let log = &logctx.log;

        // We'll use a oneshot channel to emulate the saga completion future.
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Set up a new handle.  Complete the first saga recovery immediately so
        // that that doesn't block quiescing.
        let qq = SagaQuiesceHandle::new(log.clone());
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);

        // Record a new saga being created and provide it with our emulated
        // completion future.
        let pending = qq
            .saga_create(*SAGA_ID, &SAGA_NAME)
            .expect("create saga while not quiesced");
        let consumer_completion = pending.saga_completion_future(
            async { rx.await.expect("cannot drop this before dropping tx") }
                .boxed(),
        );
        assert!(!qq.sagas_pending().is_empty());

        // Quiesce should block on the saga finishing.
        qq.quiesce();
        assert!(!qq.is_fully_quiesced());

        // "Finish" the saga.
        tx.send(saga_result()).unwrap();

        // Since this is a single-threaded executor, it should not have been
        // able to notice that the saga finished yet.  It's not that important
        // to assert this but it emphasizes that it really is waiting for
        // something to happen.
        assert!(!qq.is_fully_quiesced());

        // The consumer's completion future ought to be unblocked now.
        let _ = consumer_completion.await;

        // Wait for quiescing to finish.  This should be immediate.
        qq.wait_for_quiesced().await;
        assert!(qq.sagas_pending().is_empty());
        assert!(qq.is_fully_quiesced());

        logctx.cleanup_successful();
    }

    /// Test: quiescing blocks on first round of saga recovery
    #[tokio::test]
    async fn test_quiesce_block_on_first_recovery() {
        let logctx = test_setup_log("test_quiesce_block_on_first_recovery");
        let log = &logctx.log;

        // Set up a new handle.
        let qq = SagaQuiesceHandle::new(log.clone());

        // Quiesce should block on recovery having completed successfully once.
        qq.quiesce();
        assert!(!qq.is_fully_quiesced());

        // Act like the first recovery failed.  Quiescing should still be
        // blocked.
        let recovery = qq.recovery_start();
        recovery.recovery_done(false);
        assert!(!qq.is_fully_quiesced());

        // Finish a normal saga recovery.  Quiescing should proceed.
        // This happens synchronously (though it doesn't have to).
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);
        assert!(qq.is_fully_quiesced());
        qq.wait_for_quiesced().await;
        assert!(qq.is_fully_quiesced());

        logctx.cleanup_successful();
    }

    /// Test: quiescing blocks on outstanding saga re-assignment
    #[tokio::test]
    async fn test_quiesce_block_on_reassignment() {
        let logctx = test_setup_log("test_quiesce_block_on_reassignment");
        let log = &logctx.log;

        // Set up a new handle.  Complete the first saga recovery immediately so
        // that that doesn't block quiescing.
        let qq = SagaQuiesceHandle::new(log.clone());
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);

        // Begin saga re-assignment.
        let reassignment =
            qq.reassignment_start().expect("can re-assign when not quiescing");

        // Begin quiescing.
        qq.quiesce();

        // Quiescing is blocked.
        assert!(!qq.is_fully_quiesced());

        // When re-assignment finishes *without* having re-assigned anything,
        // then we're immediately all set.
        reassignment.reassignment_done(false);
        assert!(qq.is_fully_quiesced());
        qq.wait_for_quiesced().await;
        assert!(qq.is_fully_quiesced());

        logctx.cleanup_successful();
    }

    /// Test: when sagas get re-assigned, quiescing is blocked not just on the
    /// re-assignment finishing, but also a subsequent successful saga recovery.
    #[tokio::test]
    async fn test_quiesce_block_on_reassigned_recovery() {
        let logctx =
            test_setup_log("test_quiesce_block_on_reassigned_recovery");
        let log = &logctx.log;

        // Set up a new handle.  Complete the first saga recovery immediately so
        // that that doesn't block quiescing.
        let qq = SagaQuiesceHandle::new(log.clone());
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);

        // Begin saga re-assignment.
        let reassignment =
            qq.reassignment_start().expect("can re-assign when not quiescing");

        // Begin quiescing.
        qq.quiesce();

        // Quiescing is blocked.
        assert!(!qq.is_fully_quiesced());

        // When re-assignment finishes and re-assigned sagas, we're still
        // blocked.
        reassignment.reassignment_done(true);
        assert!(!qq.is_fully_quiesced());

        // If the next recovery pass fails, we're still blocked.
        let recovery = qq.recovery_start();
        recovery.recovery_done(false);
        assert!(!qq.is_fully_quiesced());

        // Once a recovery pass succeeds, we're good.
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);
        assert!(qq.is_fully_quiesced());

        qq.wait_for_quiesced().await;
        assert!(qq.is_fully_quiesced());

        logctx.cleanup_successful();
    }

    /// Test: very similar to test_quiesce_block_on_reassigned_recovery(), but
    /// it's not enough that saga recovery completes after the re-assignment.
    /// It must be a saga recovery pass that started after the re-assignment
    /// completed.
    #[tokio::test]
    async fn test_quiesce_block_on_reassigned_concurrent_recovery() {
        let logctx = test_setup_log(
            "test_quiesce_block_on_reassigned_concurrent_recovery",
        );
        let log = &logctx.log;

        // Set up a new handle.  Complete the first saga recovery immediately so
        // that that doesn't block quiescing.
        let qq = SagaQuiesceHandle::new(log.clone());
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);

        // Begin saga re-assignment.
        let reassignment =
            qq.reassignment_start().expect("can re-assign when not quiescing");

        // Begin quiescing.
        qq.quiesce();

        // Quiescing is blocked.
        assert!(!qq.is_fully_quiesced());

        // Start a recovery pass.
        let recovery = qq.recovery_start();

        // When re-assignment finishes and re-assigned sagas, we're still
        // blocked.
        reassignment.reassignment_done(true);
        assert!(!qq.is_fully_quiesced());

        // Even if this recovery pass succeeds, we're still blocked, because it
        // started before re-assignment finished and so isn't guaranteed to have
        // seen all the re-assigned sagas.
        recovery.recovery_done(true);
        assert!(!qq.is_fully_quiesced());

        // If the next pass fails, we're still blocked.
        let recovery = qq.recovery_start();
        recovery.recovery_done(false);
        assert!(!qq.is_fully_quiesced());

        // Finally, we have a successful pass that unblocks us.
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);
        assert!(qq.is_fully_quiesced());
        qq.wait_for_quiesced().await;
        assert!(qq.is_fully_quiesced());

        logctx.cleanup_successful();
    }

    /// Test: realistic case of a re-assignment that assigns us a saga that we
    /// need to recover *and* wait for completion.
    #[tokio::test]
    async fn test_quiesce_block_on_reassigned_recovered_saga() {
        let logctx =
            test_setup_log("test_quiesce_block_on_reassigned_recovered_saga");
        let log = &logctx.log;

        // We'll use a oneshot channel to emulate the saga completion future.
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Set up a new handle.  Complete the first saga recovery immediately so
        // that that doesn't block quiescing.
        let qq = SagaQuiesceHandle::new(log.clone());
        let recovery = qq.recovery_start();
        recovery.recovery_done(true);

        // Begin saga re-assignment.
        let reassignment =
            qq.reassignment_start().expect("can re-assign when not quiescing");

        // Begin quiescing.
        qq.quiesce();

        // Quiescing is blocked.
        assert!(!qq.is_fully_quiesced());

        // When re-assignment finishes and re-assigned sagas, we're still
        // blocked because we haven't run recovery.
        reassignment.reassignment_done(true);
        assert!(!qq.is_fully_quiesced());

        // Start a recovery pass.  Pretend like we found something.
        let recovery = qq.recovery_start();
        let pending = recovery.record_saga_recovery(*SAGA_ID, &SAGA_NAME);
        let consumer_completion = pending.saga_completion_future(
            async { rx.await.expect("cannot drop this before dropping tx") }
                .boxed(),
        );
        recovery.recovery_done(true);

        // We're still not quiesced because that saga is still running.
        assert!(!qq.is_fully_quiesced());

        // Finish the recovered saga.  That should unblock quiesce.
        tx.send(saga_result()).unwrap();

        // The consumer's completion future ought to be unblocked now.
        let _ = consumer_completion.await;

        qq.wait_for_quiesced().await;
        assert!(qq.is_fully_quiesced());

        logctx.cleanup_successful();
    }

    /// Test: blocks on recovered sagas
    #[tokio::test]
    async fn test_quiesce_block_on_recovered_sagas() {
        let logctx = test_setup_log("test_quiesce_block_on_recovered_sagas");
        let log = &logctx.log;

        // We'll use a oneshot channel to emulate the saga completion future.
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Set up a new handle.
        let qq = SagaQuiesceHandle::new(log.clone());
        // Start a recovery pass.  Pretend like we found something.
        let recovery = qq.recovery_start();
        let pending = recovery.record_saga_recovery(*SAGA_ID, &SAGA_NAME);
        let consumer_completion = pending.saga_completion_future(
            async { rx.await.expect("cannot drop this before dropping tx") }
                .boxed(),
        );
        recovery.recovery_done(true);

        // Begin quiescing.
        qq.quiesce();

        // Quiescing is blocked.
        assert!(!qq.is_fully_quiesced());

        // Finish the recovered saga.  That should unblock quiesce.
        tx.send(saga_result()).unwrap();

        qq.wait_for_quiesced().await;
        assert!(qq.is_fully_quiesced());

        // The consumer's completion future ought to be unblocked now.
        let _ = consumer_completion.await;

        logctx.cleanup_successful();
    }
}
