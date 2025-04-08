// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report status for the saga recovery background task

use super::recovery;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use serde::{Deserialize, Serialize};
use slog_error_chain::InlineErrorChain;
use std::collections::VecDeque;
use steno::SagaId;

// These values are chosen to be large enough to likely cover the complete
// history of saga recoveries, successful and otherwise.  They just need to be
// finite so that this system doesn't use an unbounded amount of memory.
/// Maximum number of successful recoveries to keep track of for debugging
const N_SUCCESS_SAGA_HISTORY: usize = 128;
/// Maximum number of recent failures to keep track of for debugging
const N_FAILED_SAGA_HISTORY: usize = 128;

/// Summarizes the status of saga recovery for debugging
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Report {
    pub recent_recoveries: DebuggingHistory<RecoverySuccess>,
    pub recent_failures: DebuggingHistory<RecoveryFailure>,
    pub last_pass: LastPass,

    pub ntotal_recovered: usize,
    pub ntotal_failures: usize,
    pub ntotal_started: usize,
    pub ntotal_finished: usize,
    pub ntotal_sec_errors_missing: usize,
    pub ntotal_sec_errors_bad_state: usize,
}

impl Report {
    pub fn new() -> Report {
        Report {
            recent_recoveries: DebuggingHistory::new(N_SUCCESS_SAGA_HISTORY),
            recent_failures: DebuggingHistory::new(N_FAILED_SAGA_HISTORY),
            last_pass: LastPass::NeverStarted,
            ntotal_recovered: 0,
            ntotal_failures: 0,
            ntotal_started: 0,
            ntotal_finished: 0,
            ntotal_sec_errors_missing: 0,
            ntotal_sec_errors_bad_state: 0,
        }
    }

    /// Update the report after a single saga recovery pass where we at least
    /// successfully constructed a plan
    pub fn update_after_pass(
        &mut self,
        plan: &recovery::Plan,
        execution: recovery::Execution,
        nstarted: usize,
    ) {
        self.last_pass =
            LastPass::Success(LastPassSuccess::new(plan, &execution));

        let (succeeded, failed) = execution.into_results();

        for success in succeeded {
            self.recent_recoveries.append(success);
            self.ntotal_recovered += 1;
        }

        for failure in failed {
            self.recent_failures.append(failure);
            self.ntotal_failures += 1;
        }

        self.ntotal_started += nstarted;
        self.ntotal_finished += plan.ninferred_done();
    }

    /// Update the report after a saga recovery pass where we couldn't even
    /// construct a plan (usually because we couldn't load state from the
    /// database)
    pub fn update_after_failure(&mut self, error: &Error, nstarted: usize) {
        self.ntotal_started += nstarted;
        self.last_pass = LastPass::Failed {
            message: InlineErrorChain::new(error).to_string(),
        };
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RecoverySuccess {
    pub time: DateTime<Utc>,
    pub saga_id: SagaId,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RecoveryFailure {
    pub time: DateTime<Utc>,
    pub saga_id: SagaId,
    pub message: String,
}

/// Describes what happened during the last saga recovery pass
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum LastPass {
    /// There has not been a saga recovery pass yet
    NeverStarted,
    /// This pass failed to even construct a plan (usually because we couldn't
    /// load state from the database)
    Failed { message: String },
    /// This pass was at least partially successful
    Success(LastPassSuccess),
}

/// Describes what happened during a saga recovery pass where we at least
/// managed to construct a plan
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct LastPassSuccess {
    pub nfound: usize,
    pub nrecovered: usize,
    pub nfailed: usize,
    pub nskipped: usize,
    pub nremoved: usize,
}

impl LastPassSuccess {
    pub fn new(
        plan: &recovery::Plan,
        execution: &recovery::Execution,
    ) -> LastPassSuccess {
        let nfound = plan.sagas_needing_recovery().count() + plan.nskipped();
        LastPassSuccess {
            nfound,
            nrecovered: execution.succeeded.len(),
            nfailed: execution.failed.len(),
            nskipped: plan.nskipped(),
            nremoved: plan.ninferred_done(),
        }
    }
}

/// Debugging ringbuffer, storing arbitrary objects of type `T`
// There surely exist faster and richer implementations.  At least this one's
// pretty simple.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct DebuggingHistory<T> {
    ring: VecDeque<T>,
}

impl<T> DebuggingHistory<T> {
    pub fn new(size: usize) -> DebuggingHistory<T> {
        DebuggingHistory { ring: VecDeque::with_capacity(size) }
    }

    pub fn append(&mut self, t: T) {
        if self.ring.len() == self.ring.capacity() {
            let _ = self.ring.pop_front();
        }
        self.ring.push_back(t);
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.ring.iter()
    }
}
