// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Behavior shared by the per-diagnosis-engine `fm_fact_*` tables.

use nexus_types::fm;
use nexus_types::fm::case::FactMetadata;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{CaseUuid, FactUuid, SitrepUuid};

/// A row in one of the per-diagnosis-engine `fm_fact_*` tables.
pub trait FmFactRow: Sized {
    /// The diagnosis engine's fact type stored in this table.
    type Payload;

    /// Build a row from a fact's shared metadata and its engine payload.
    fn from_sitrep(
        sitrep_id: SitrepUuid,
        case_id: CaseUuid,
        metadata: &FactMetadata,
        payload: &Self::Payload,
    ) -> Self;

    fn id(&self) -> FactUuid;

    fn case_id(&self) -> CaseUuid;

    /// Reconstruct an in-memory fact from a row.
    fn into_fact(self) -> Result<fm::case::Fact, Error>;
}
