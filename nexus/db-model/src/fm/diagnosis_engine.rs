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

impl From<DiagnosisEngine> for fm::DiagnosisEngine {
    fn from(de: DiagnosisEngine) -> Self {
        match de {
            DiagnosisEngine::PowerShelf => fm::DiagnosisEngine::PowerShelf,
        }
    }
}

impl From<fm::DiagnosisEngine> for DiagnosisEngine {
    fn from(fm_de: fm::DiagnosisEngine) -> Self {
        match fm_de {
            fm::DiagnosisEngine::PowerShelf => DiagnosisEngine::PowerShelf,
        }
    }
}

impl fmt::Display for DiagnosisEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fm::DiagnosisEngine::from(*self).fmt(f)
    }
}
