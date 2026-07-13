// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of the saga diagnosis engine's facts.
//!
//! Each saga fact is stored as typed columns in the `fm_fact_saga` table. The
//! `kind` discriminant selects which payload columns are populated; per-kind
//! CHECK constraints (e.g. `not_progressing_columns_present`) enforce that
//! the right columns are non-NULL for each kind. See
//! [`nexus_types::fm::SagaFact`] for semantics.

use crate::DbTypedUuid;
use crate::SagaState;
use crate::impl_enum_type;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_fact_saga;
use nexus_types::fm;
use nexus_types::fm::case::FactMetadata;
use nexus_types::fm::{
    FactPayload, SagaAbandonedFactPayload, SagaFact,
    SagaNotProgressingFactPayload, SagaOwnerNotCurrentFactPayload,
};
use nexus_types::observed_saga::{OrphanedReason, SagaProgressState};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{CaseKind, FactKind, OmicronZoneKind, SitrepKind};
use uuid::Uuid;

impl_enum_type!(
    FmFactSagaKindEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Eq)]
    pub enum FmFactSagaKind;

    NotProgressing => b"not_progressing"
    OwnerNotCurrentGeneration => b"owner_not_current_generation"
    Abandoned => b"abandoned"
);

impl_enum_type!(
    FmFactSagaOrphanReasonEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Eq)]
    pub enum FmFactSagaOrphanReason;

    Quiesced => b"quiesced"
    Expunged => b"expunged"
);

impl From<OrphanedReason> for FmFactSagaOrphanReason {
    fn from(reason: OrphanedReason) -> Self {
        match reason {
            OrphanedReason::Quiesced => FmFactSagaOrphanReason::Quiesced,
            OrphanedReason::Expunged => FmFactSagaOrphanReason::Expunged,
        }
    }
}

impl From<FmFactSagaOrphanReason> for OrphanedReason {
    fn from(reason: FmFactSagaOrphanReason) -> Self {
        match reason {
            FmFactSagaOrphanReason::Quiesced => OrphanedReason::Quiesced,
            FmFactSagaOrphanReason::Expunged => OrphanedReason::Expunged,
        }
    }
}

impl From<SagaProgressState> for SagaState {
    fn from(state: SagaProgressState) -> Self {
        match state {
            SagaProgressState::Running => SagaState::Running,
            SagaProgressState::Unwinding => SagaState::Unwinding,
        }
    }
}

/// Convert a DB `saga_state` back into the [`SagaProgressState`] recorded on
/// a `NotProgressing` fact. Only live (running or unwinding) sagas carry that
/// fact: a done saga has no facts at all, and an abandoned saga carries an
/// `Abandoned` fact, whose payload has no saga state. A stored `Done` or
/// `Abandoned` here therefore indicates a corrupt row.
fn saga_progress_state(state: SagaState) -> Result<SagaProgressState, Error> {
    match state {
        SagaState::Running => Ok(SagaProgressState::Running),
        SagaState::Unwinding => Ok(SagaProgressState::Unwinding),
        SagaState::Done | SagaState::Abandoned => {
            Err(Error::internal_error(&format!(
                "fm_fact_saga row has saga_state {state:?}, which is never \
                 recorded on a NotProgressing saga fact"
            )))
        }
    }
}

/// Diesel row for the `fm_fact_saga` table.
///
/// The payload columns are populated according to `kind`: a column is `Some`
/// if it belongs to that `kind`'s payload, and `None` otherwise.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_fact_saga)]
pub struct FmFactSaga {
    pub id: DbTypedUuid<FactKind>,
    /// The sitrep to which this fact belongs.
    ///
    /// This will change as the fact is carried forward from one sitrep to the
    /// next.
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub case_id: DbTypedUuid<CaseKind>,
    /// Sitrep in which this fact was first added.
    ///
    /// Preserved unchanged when the fact is carried forward; debug-only.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    pub comment: String,

    /// The saga this fact is about.
    pub saga_id: Uuid,
    pub kind: FmFactSagaKind,

    // Columns for the `NotProgressing` kind.
    pub saga_state: Option<SagaState>,
    pub last_event_time: Option<DateTime<Utc>>,

    // Columns for the `OwnerNotCurrentGeneration` kind.
    pub current_sec: Option<DbTypedUuid<OmicronZoneKind>>,
    pub orphan_reason: Option<FmFactSagaOrphanReason>,
}

impl FmFactSaga {
    /// Build a row from a fact's shared metadata (`metadata`) and its
    /// already-dispatched saga payload (`saga_fact`).
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        case_id: impl Into<DbTypedUuid<CaseKind>>,
        metadata: &FactMetadata,
        saga_fact: &SagaFact,
    ) -> Self {
        let FactMetadata { id, created_sitrep_id, comment } = metadata;
        let base = Self {
            id: (*id).into(),
            sitrep_id: sitrep_id.into(),
            case_id: case_id.into(),
            created_sitrep_id: (*created_sitrep_id).into(),
            comment: comment.clone(),
            saga_id: saga_fact.saga_id().0,
            kind: FmFactSagaKind::NotProgressing,
            saga_state: None,
            last_event_time: None,
            current_sec: None,
            orphan_reason: None,
        };
        match saga_fact {
            SagaFact::NotProgressing(p) => Self {
                kind: FmFactSagaKind::NotProgressing,
                saga_state: Some(p.saga_state.into()),
                last_event_time: Some(p.last_event_time),
                ..base
            },
            SagaFact::OwnerNotCurrentGeneration(p) => Self {
                kind: FmFactSagaKind::OwnerNotCurrentGeneration,
                current_sec: Some(p.current_sec.into()),
                orphan_reason: Some(p.orphan_reason.into()),
                ..base
            },
            SagaFact::Abandoned(_) => {
                Self { kind: FmFactSagaKind::Abandoned, ..base }
            }
        }
    }

    /// Reconstruct an in-memory fact from a row.
    pub fn into_fact(self) -> Result<fm::case::Fact, Error> {
        let kind = self.kind;
        let saga_id = steno::SagaId(self.saga_id);
        let payload = match kind {
            FmFactSagaKind::NotProgressing => FactPayload::Saga(
                SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                    saga_id,
                    saga_state: saga_progress_state(
                        self.saga_state.ok_or_else(|| {
                            missing_column(kind, "saga_state")
                        })?,
                    )?,
                    last_event_time: self.last_event_time.ok_or_else(|| {
                        missing_column(kind, "last_event_time")
                    })?,
                }),
            ),
            FmFactSagaKind::OwnerNotCurrentGeneration => {
                FactPayload::Saga(SagaFact::OwnerNotCurrentGeneration(
                    SagaOwnerNotCurrentFactPayload {
                        saga_id,
                        current_sec: self
                            .current_sec
                            .ok_or_else(|| missing_column(kind, "current_sec"))?
                            .into(),
                        orphan_reason: self
                            .orphan_reason
                            .ok_or_else(|| {
                                missing_column(kind, "orphan_reason")
                            })?
                            .into(),
                    },
                ))
            }
            FmFactSagaKind::Abandoned => FactPayload::Saga(
                SagaFact::Abandoned(SagaAbandonedFactPayload { saga_id }),
            ),
        };
        Ok(fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id: self.id.into(),
                created_sitrep_id: self.created_sitrep_id.into(),
                comment: self.comment,
            },
            payload,
        })
    }
}

fn missing_column(kind: FmFactSagaKind, column: &str) -> Error {
    Error::internal_error(&format!(
        "fm_fact_saga row of kind {kind:?} has a NULL {column}, violating the \
         CHECK constraint requiring it to be non-NULL for this kind"
    ))
}
