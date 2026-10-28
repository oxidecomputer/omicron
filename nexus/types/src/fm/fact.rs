// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Typed payloads of diagnosis-engine-derived [`Fact`]s.
//!
//! Each diagnosis engine owns one variant of [`FactPayload`]. A [`Case`]
//! belongs to exactly one diagnosis engine (see [`Metadata::de`]), so every
//! fact on a case carries that engine's variant.
//!
//! [`Fact`]: super::case::Fact
//! [`Case`]: super::case::Case
//! [`Metadata::de`]: super::case::Metadata::de

use crate::fm::DiagnosisEngineKind;
use crate::inventory::ZpoolHealth;
use crate::observed_saga::{OrphanedReason, SagaProgressState};
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::{
    CollectionUuid, OmicronZoneUuid, PhysicalDiskUuid, ZpoolUuid,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The typed payload of a [`Fact`](super::case::Fact).
///
/// One variant per diagnosis engine. The variant a fact carries always
/// matches its case's [`de`](super::case::Metadata::de); other engines and
/// shared FM code must not interpret another engine's variant.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "engine", rename_all = "snake_case")]
pub enum FactPayload {
    /// A fact owned by the physical-disk diagnosis engine.
    PhysicalDisk(DiskFact),
    /// A fact owned by the saga diagnosis engine.
    Saga(SagaFact),
    /// A fact owned by the certificate diagnosis engine.
    Certificate(CertificateFact),
}

impl From<DiskFact> for FactPayload {
    fn from(fact: DiskFact) -> Self {
        FactPayload::PhysicalDisk(fact)
    }
}

impl From<SagaFact> for FactPayload {
    fn from(fact: SagaFact) -> Self {
        FactPayload::Saga(fact)
    }
}

impl From<CertificateFact> for FactPayload {
    fn from(fact: CertificateFact) -> Self {
        FactPayload::Certificate(fact)
    }
}

impl FactPayload {
    /// The diagnosis engine that owns this payload's variant.
    pub fn engine(&self) -> DiagnosisEngineKind {
        match self {
            FactPayload::PhysicalDisk(_) => DiagnosisEngineKind::PhysicalDisk,
            FactPayload::Saga(_) => DiagnosisEngineKind::Saga,
            FactPayload::Certificate(_) => DiagnosisEngineKind::Certificate,
        }
    }

    /// The physical-disk payload, or `None` if this fact belongs to a
    /// different diagnosis engine.
    pub fn as_physical_disk(&self) -> Option<&DiskFact> {
        match self {
            FactPayload::PhysicalDisk(fact) => Some(fact),
            _ => None,
        }
    }

    /// The saga payload, or `None` if this fact belongs to a different
    /// diagnosis engine.
    pub fn as_saga(&self) -> Option<&SagaFact> {
        match self {
            FactPayload::Saga(fact) => Some(fact),
            _ => None,
        }
    }

    /// The certificate payload, or `None` if this fact belongs to a
    /// different diagnosis engine.
    pub fn as_certificate(&self) -> Option<&CertificateFact> {
        match self {
            FactPayload::Certificate(fact) => Some(fact),
            _ => None,
        }
    }
}

/// Per-fact state for the physical-disk diagnosis engine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DiskFact {
    /// The zpool's most recently observed health is non-`Online`.
    ZpoolUnhealthy(ZpoolUnhealthyFactPayload),
}

impl DiskFact {
    /// The physical disk this fact is about. Common to every kind of disk
    /// fact: a Disk case is keyed by its physical disk, and every fact on
    /// the case agrees on this value.
    pub fn physical_disk_id(&self) -> PhysicalDiskUuid {
        match self {
            DiskFact::ZpoolUnhealthy(p) => p.physical_disk_id,
        }
    }
}

/// Payload of a [`DiskFact::ZpoolUnhealthy`] fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ZpoolUnhealthyFactPayload {
    /// The physical disk this fact (and its parent case) is about.
    /// Every fact on a Disk case must agree on this value.
    pub physical_disk_id: PhysicalDiskUuid,
    /// The zpool whose health was observed. Kept for provenance: the
    /// case is keyed by `physical_disk_id`, but knowing the exact zpool
    /// makes the fact self-describing when read in isolation.
    pub zpool_id: ZpoolUuid,
    pub last_seen_health: ZpoolHealth,
    /// Inventory collection that produced this observation. Recorded for
    /// provenance only: the diagnosis engine never looks this collection
    /// back up (it may well have been GC'd by the time anyone reads the
    /// fact). If multiple `ZpoolUnhealthy` facts ever end up on the same
    /// case, this lets a human reader see which inventory each came from.
    pub observed_in_inv: CollectionUuid,
    /// `time_done` of `observed_in_inv`.
    pub time_observed: DateTime<Utc>,
}

/// Per-fact state for the saga diagnosis engine.
///
/// A saga case (keyed by `saga_id`) may carry `NotProgressing` and
/// `OwnerNotCurrentGeneration` together (two independent problems with a
/// live saga). `Abandoned` supersedes both: once Nexus has permanently
/// given up on a saga, the live-saga conditions are vacuous, and the case
/// carries the `Abandoned` fact alone.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SagaFact {
    /// The saga is live but has recorded no node event in a long time,
    /// i.e. it is not making durable forward or undo progress.
    NotProgressing(SagaNotProgressingFactPayload),
    /// The saga is owned by a Nexus that is no longer of the current
    /// generation (quiesced or expunged), so that Nexus will not advance it.
    OwnerNotCurrentGeneration(SagaOwnerNotCurrentFactPayload),
    /// Nexus failed to recover the saga for a non-transient reason and has
    /// permanently given up on running it. The saga may be holding
    /// partially-allocated resources; remediation is manual and
    /// saga-specific (see omicron#10581 / RFD 555).
    Abandoned(SagaAbandonedFactPayload),
}

impl SagaFact {
    /// The saga this fact (and its parent case) is about. Common to every
    /// kind of saga fact.
    pub fn saga_id(&self) -> steno::SagaId {
        match self {
            SagaFact::NotProgressing(p) => p.saga_id,
            SagaFact::OwnerNotCurrentGeneration(p) => p.saga_id,
            SagaFact::Abandoned(p) => p.saga_id,
        }
    }
}

/// Payload of a [`SagaFact::NotProgressing`] fact.
///
/// Since we carry the saga ID here, we can infer a lot of extra information
/// from the saga itself (e.g., the name) by looking the saga up by ID. For this
/// fact, we only carry that minimal data, plus "what state has it been observed
/// in" and "when", which are values that might change in subsequent iterations
/// of sitrep generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SagaNotProgressingFactPayload {
    /// The saga this fact (and its parent case) is about.
    pub saga_id: steno::SagaId,
    /// Whether the saga is running forward or unwinding.
    pub saga_state: SagaProgressState,
    /// The last durably-recorded progress: the latest
    /// `saga_node_event.event_time` observed for this saga, or the saga's
    /// creation time if it has recorded no node events at all. The case was
    /// opened because `now - last_event_time` exceeded the staleness
    /// threshold.
    pub last_event_time: DateTime<Utc>,
}

/// Payload of a [`SagaFact::OwnerNotCurrentGeneration`] fact.
///
/// See [`SagaNotProgressingFactPayload`] for why we're storing the saga
/// ID and as little else as possible.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SagaOwnerNotCurrentFactPayload {
    /// The saga this fact (and its parent case) is about.
    pub saga_id: steno::SagaId,
    /// The owning Nexus zone (`saga.current_sec`). This fact only fires when
    /// the saga has a current SEC, so it is always present.
    pub current_sec: OmicronZoneUuid,
    /// Why the owner is not current: quiesced (older generation) or expunged
    /// (no `db_metadata_nexus` record).
    pub orphan_reason: OrphanedReason,
}

/// Payload of a [`SagaFact::Abandoned`] fact.
///
/// The condition is boolean (the saga is abandoned or it isn't), so the
/// payload carries nothing beyond the saga's ID. See
/// [`SagaNotProgressingFactPayload`] for why payloads carry only
/// condition-defining fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SagaAbandonedFactPayload {
    /// The saga this fact (and its parent case) is about.
    pub saga_id: steno::SagaId,
}

/// Per-fact state for the certificate diagnosis engine.
///
/// A certificate case is keyed by silo and carries exactly one fact at a
/// time: the silo's best certificate (the one Nexus serves, chosen by latest
/// leaf `not_after`) is either about to expire or has already expired.
/// `BestCertificateExpired` supersedes `BestCertificateExpiring`: once
/// `not_after` has passed, the expiring fact is replaced rather than kept
/// alongside.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CertificateFact {
    /// The silo's best certificate expires within the configured warning
    /// window, and no later-expiring certificate is installed.
    BestCertificateExpiring(CertificateExpiryFactPayload),
    /// The silo's best certificate has expired, and no later-expiring
    /// certificate is installed. Nexus still serves it, since serving an
    /// expired certificate beats serving none.
    BestCertificateExpired(CertificateExpiryFactPayload),
}

impl CertificateFact {
    /// The silo this fact (and its parent case) is about. Common to every
    /// kind of certificate fact.
    pub fn silo_id(&self) -> Uuid {
        match self {
            CertificateFact::BestCertificateExpiring(p) => p.silo_id,
            CertificateFact::BestCertificateExpired(p) => p.silo_id,
        }
    }

    /// The condition-defining payload, shared by every kind.
    pub fn payload(&self) -> &CertificateExpiryFactPayload {
        match self {
            CertificateFact::BestCertificateExpiring(p) => p,
            CertificateFact::BestCertificateExpired(p) => p,
        }
    }
}

/// Payload of a [`CertificateFact::BestCertificateExpiring`] or
/// [`CertificateFact::BestCertificateExpired`] fact.
///
/// This carries only the fields that define the condition. Descriptive data
/// (the silo and certificate names) is looked up from the analysis input
/// when the case is acted on.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CertificateExpiryFactPayload {
    /// The silo this fact (and its parent case) is about.
    pub silo_id: Uuid,
    /// The silo's best certificate at the time the fact was recorded. If a
    /// different certificate becomes the best one, the fact is replaced.
    pub certificate_id: Uuid,
    /// The leaf `not_after` of that certificate.
    pub not_after: DateTime<Utc>,
}
