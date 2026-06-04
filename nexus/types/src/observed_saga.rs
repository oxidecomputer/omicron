// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! "Currently non-terminal sagas" — the executed view from the `saga` and
//! `saga_node_event` DB tables, annotated with the state of each saga's
//! owning Nexus.
//!
//! This is the saga diagnosis engine's analog of
//! [`InServiceDisk`](crate::in_service_disk::InServiceDisk): the fault
//! management preparation phase reads it directly from the database, and the
//! saga diagnosis engine consumes it to decide whether a saga is stuck (not
//! making progress) or orphaned (owned by a Nexus that is no longer of the
//! current generation).

use chrono::{DateTime, Utc};
use iddqd::{IdOrdItem, id_upcast};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use serde::{Deserialize, Serialize};

/// The non-terminal execution state of a saga, as recorded in the `saga`
/// table's `saga_state` column. Terminal states (`done`, `abandoned`) are
/// deliberately excluded: a saga that has reached one of those is no longer a
/// candidate for the saga diagnosis engine (its case, if any, is closed).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SagaProgressState {
    /// The saga is executing forward actions.
    Running,
    /// One or more actions failed and the saga is executing undo actions.
    Unwinding,
}

/// The state of a saga's owning Nexus (`saga.current_sec`), classified against
/// `db_metadata_nexus`. Drives the saga diagnosis engine's "orphaned" path.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SagaOwnerState {
    /// The owning Nexus is the current, active generation.
    Active,
    /// The owning Nexus is a newer generation that is not yet active.
    NotYet,
    /// The owning Nexus has quiesced (an older generation handed off).
    Quiesced,
    /// The owning Nexus has no `db_metadata_nexus` record at all (expunged),
    /// or the saga has no `current_sec`.
    Absent,
}

impl SagaOwnerState {
    /// If this owner state means the saga is orphaned — owned by a Nexus that
    /// will not make progress on it — the reason why; otherwise `None`.
    pub fn orphaned_reason(self) -> Option<OrphanedReason> {
        match self {
            SagaOwnerState::Active | SagaOwnerState::NotYet => None,
            SagaOwnerState::Quiesced => Some(OrphanedReason::Quiesced),
            SagaOwnerState::Absent => Some(OrphanedReason::Expunged),
        }
    }
}

/// Why a saga is orphaned: its owning Nexus exists but will not advance it.
/// The reduced, only-ever-stored form of [`SagaOwnerState`] (the `Active` and
/// `NotYet` states never produce a fact).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrphanedReason {
    /// The owning Nexus quiesced (an older generation that handed off).
    Quiesced,
    /// The owning Nexus has no `db_metadata_nexus` record (expunged).
    Expunged,
}

/// One non-terminal saga, joined with the timestamp of its most recent node
/// event and the state of its owning Nexus.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservedSaga {
    pub saga_id: steno::SagaId,
    pub saga_name: String,
    pub saga_state: SagaProgressState,
    /// When the saga was created (`saga.time_created`).
    pub time_created: DateTime<Utc>,
    /// The owning Nexus zone (`saga.current_sec`), or `None` if the saga has no
    /// current SEC.
    pub current_sec: Option<OmicronZoneUuid>,
    /// `saga.adopt_generation`: bumps each time the saga is re-adopted to a
    /// SEC (failover), not on progress.
    pub adopt_generation: Generation,
    /// The latest `saga_node_event.event_time` for this saga, i.e. the last
    /// durably-recorded forward or undo step. `None` if the saga somehow has
    /// no node events yet. This is the "last progress" signal:
    /// `now - last_event_time` is how long the saga has gone without
    /// recording progress.
    pub last_event_time: Option<DateTime<Utc>>,
    /// The classified state of the owning Nexus, or `None` if the saga has no
    /// `current_sec` (it is between adoptions and not classifiable — and not
    /// treated as orphaned). Always `Some` when `current_sec` is `Some`.
    pub owner_state: Option<SagaOwnerState>,
}

impl IdOrdItem for ObservedSaga {
    type Key<'a> = steno::SagaId;
    fn key(&self) -> Self::Key<'_> {
        self.saga_id
    }
    id_upcast!();
}
