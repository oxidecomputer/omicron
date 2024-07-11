// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reporting status for the saga recovery background task

use super::recovery;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Error;
use serde::Serialize;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Report {
    pub recent_recoveries: DebuggingHistory<RecoverySuccess>,
    pub recent_failures: DebuggingHistory<RecoveryFailure>,
    last_pass: LastPass,
}

impl Report {
    pub fn new() -> Report {
        Report {
            recent_recoveries: DebuggingHistory::new(N_SUCCESS_SAGA_HISTORY),
            recent_failures: DebuggingHistory::new(N_FAILED_SAGA_HISTORY),
            last_pass: LastPass::NeverStarted,
        }
    }

    pub fn update_after_pass(
        &mut self,
        plan: &recovery::Plan,
        execution: recovery::ExecutionSummary,
    ) {
        self.last_pass =
            LastPass::Success(LastPassSuccess::new(plan, &execution));

        let (succeeded, failed) = execution.into_results();

        for success in succeeded {
            self.recent_recoveries.append(success);
        }

        for failure in failed {
            self.recent_failures.append(failure);
        }
    }

    pub fn update_after_failure(&mut self, error: &Error) {
        self.last_pass = LastPass::Failed {
            message: InlineErrorChain::new(error).to_string(),
        };
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecoverySuccess {
    pub time: DateTime<Utc>,
    pub saga_id: SagaId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecoveryFailure {
    pub time: DateTime<Utc>,
    pub saga_id: SagaId,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum LastPass {
    NeverStarted,
    Failed { message: String },
    Success(LastPassSuccess),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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
        execution: &recovery::ExecutionSummary,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DebuggingHistory<T> {
    size: usize,
    ring: VecDeque<T>,
}

impl<T> DebuggingHistory<T> {
    pub fn new(size: usize) -> DebuggingHistory<T> {
        DebuggingHistory { size, ring: VecDeque::with_capacity(size) }
    }

    pub fn append(&mut self, t: T) {
        let len = self.ring.len();
        assert!(len <= self.size);
        if len == self.size {
            let _ = self.ring.pop_front();
        }
        self.ring.push_back(t);
    }
}
