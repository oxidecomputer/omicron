// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::impl_enum_type;
use nexus_types::fm;
use serde::{Deserialize, Serialize};
use std::fmt;

impl_enum_type!(
    DiagnosisEngineEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Serialize,
        Deserialize,
        AsExpression,
        FromSqlRow,
    )]
    #[serde(rename_all = "snake_case")]
    pub enum DiagnosisEngine;

    PowerShelf => b"power_shelf"

);

impl From<DiagnosisEngine> for fm::DiagnosisEngineKind {
    fn from(de: DiagnosisEngine) -> Self {
        match de {
            DiagnosisEngine::PowerShelf => fm::DiagnosisEngineKind::PowerShelf,
        }
    }
}

impl From<fm::DiagnosisEngineKind> for DiagnosisEngine {
    fn from(fm_de: fm::DiagnosisEngineKind) -> Self {
        match fm_de {
            fm::DiagnosisEngineKind::PowerShelf => DiagnosisEngine::PowerShelf,
        }
    }
}

impl fmt::Display for DiagnosisEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fm::DiagnosisEngineKind::from(*self).fmt(f)
    }
}
