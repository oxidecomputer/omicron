// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Just the facts, ma'am.

use super::DiagnosisEngineKind;
use omicron_uuid_kinds::{CaseUuid, FactUuid, SitrepUuid};

pub trait Fact {
    const DE: DiagnosisEngineKind;

    fn identity(&self) -> FactIdentity;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FactIdentity {
    pub id: FactUuid,
    pub case_id: CaseUuid,
    pub sitrep_id: SitrepUuid,
    pub created_sitrep_id: SitrepUuid,
}
