// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnosis engines

use crate::CaseBuilder;
use crate::SitrepBuilder;
use nexus_types::fm;
use std::sync::Arc;

pub mod power_shelf;

pub trait DiagnosisEngine {
    fn kind(&self) -> fm::DiagnosisEngineKind;

    /// Called for each new ereprot received since the parent sitrep.
    fn analyze_ereport(
        &mut self,
        sitrep: &mut SitrepBuilder<'_>,
        ereport: &Arc<fm::Ereport>,
    ) -> anyhow::Result<()>;

    /// Called for each case belonging to this diagnosis engine opened in the
    /// parent sitrep.
    fn analyze_open_case(
        &mut self,
        sitrep: &mut SitrepBuilder<'_>,
        ereport: &mut CaseBuilder,
    ) -> anyhow::Result<()>;

    /// Complete this diagnosis engine's analysis, making any necessary changes
    /// to the sitrep being constructed.
    fn finish(&mut self, sitrep: &mut SitrepBuilder<'_>) -> anyhow::Result<()>;
}
