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

use crate::inventory::ZpoolHealth;
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::{CollectionUuid, PhysicalDiskUuid, ZpoolUuid};
use serde::{Deserialize, Serialize};

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
}

impl From<DiskFact> for FactPayload {
    fn from(fact: DiskFact) -> Self {
        FactPayload::PhysicalDisk(fact)
    }
}

impl FactPayload {
    /// The physical-disk payload, or `None` if this fact belongs to a
    /// different diagnosis engine.
    pub fn as_physical_disk(&self) -> Option<&DiskFact> {
        #[allow(unreachable_patterns)]
        match self {
            FactPayload::PhysicalDisk(fact) => Some(fact),
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
