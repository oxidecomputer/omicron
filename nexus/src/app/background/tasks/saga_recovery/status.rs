// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reporting status for the saga recovery background task

use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::VecDeque;
use steno::SagaId;

#[derive(Clone, Serialize)]
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

#[derive(Clone, Serialize)]
pub struct SagaRecoveryTaskStatus {
    pub recent_recoveries: DebuggingHistory<RecoverySuccess>,
    pub recent_failures: DebuggingHistory<RecoveryFailure>,
    pub last_pass: LastPass,
}

#[derive(Clone, Serialize)]
pub struct RecoveryFailure {
    pub time: DateTime<Utc>,
    pub saga_id: SagaId,
    pub message: String,
}

#[derive(Clone, Serialize)]
pub struct RecoverySuccess {
    pub time: DateTime<Utc>,
    pub saga_id: SagaId,
}

#[derive(Clone, Serialize)]
pub enum LastPass {
    NeverStarted,
    Failed { message: String },
    Success(LastPassSuccess),
}

#[derive(Clone, Serialize)]
pub struct LastPassSuccess {
    pub nfound: usize,
    pub nrecovered: usize,
    pub nfailed: usize,
    pub nskipped: usize,
    pub nremoved: usize,
}
