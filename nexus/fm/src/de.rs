// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnosis engines

use crate::SitrepBuilder;
use nexus_types::fm;
pub mod power_shelf;
use std::sync::Arc;

pub trait DiagnosisEngine {
    fn kind(&self) -> fm::DiagnosisEngineKind;

    fn analyze_ereport(
        &mut self,
        sitrep: &mut SitrepBuilder<'_>,
        ereport: &Arc<fm::Ereport>,
    ) -> anyhow::Result<()>;

    fn process_cases(
        &mut self,
        sitrep: &mut SitrepBuilder<'_>,
    ) -> anyhow::Result<()>;
}
