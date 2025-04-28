// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for reporting protocol invariant violations
//!
//! Invariant violations should _never_ occur. They represent a critical bug in
//! the implementation of the system. In certain scenarios we can detect these
//! invariant violations and record them. This allows reporting them to higher
//! levels of control plane software so that we can debug them and fix them in
//! future releases, as well as rectify outstanding issues on systems where such
//! an alarm arose.

use crate::{Epoch, PlatformId};
use serde::{Deserialize, Serialize};

/// A critical invariant violation that should never occur.
///
/// Many invariant violations are only possible on receipt of peer messages,
/// and are _not_ a result of API calls. This means that there isn't a good
/// way to directly inform the rest of the control plane. Instead we provide a
/// queryable API for `crate::Node` status that includes alerts.
///
/// If an `Alarm` is ever seen by an operator then support should be contacted
/// immediately.
#[derive(
    Debug, Clone, thiserror::Error, PartialEq, Eq, Serialize, Deserialize,
)]
pub enum Alarm {
    #[error(
        "prepare for a later configuration exists: \
        last_prepared_epoch = {last_prepared_epoch:?}, \
        commit_epoch = {commit_epoch}"
    )]
    OutOfOrderCommit { last_prepared_epoch: Option<Epoch>, commit_epoch: Epoch },

    #[error("commit attempted, but missing prepare message: epoch = {epoch}")]
    MissingPrepare { epoch: Epoch },

    #[error(
        "prepare received with mismatched last_committed_epoch: prepare's last \
        committed epoch = {prepare_last_committed_epoch:?}, persisted \
        prepare's last_committed_epoch = \
        {persisted_prepare_last_committed_epoch:?}"
    )]
    PrepareLastCommittedEpochMismatch {
        prepare_last_committed_epoch: Option<Epoch>,
        persisted_prepare_last_committed_epoch: Option<Epoch>,
    },

    #[error(
        "different nodes coordinating same epoch = {epoch}: \
        them = {them}, us = {us}"
    )]
    DifferentNodesCoordinatingSameEpoch {
        epoch: Epoch,
        them: PlatformId,
        us: PlatformId,
    },
}
