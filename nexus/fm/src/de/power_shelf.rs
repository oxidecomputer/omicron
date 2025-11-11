// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Power shelf diagnosis
use crate::SitrepBuilder;
use nexus_types::fm::Ereport;
use nexus_types::fm::ereport;
use nexus_types::inventory::SpType;
use std::sync::Arc;

pub fn diagnose(
    log: &slog::Logger,
    sitrep: &mut SitrepBuilder<'_>,
    new_ereports: &[Arc<Ereport>],
) -> anyhow::Result<()> {
    for ereport in new_ereports {
        // Skip non-power shelf reports
        if !matches!(
            ereport.reporter,
            ereport::Reporter::Sp { sp_type: SpType::Power, .. }
        ) {
            continue;
        }

        // TODO: check for existing cases tracked for this power shelf and see
        // if the ereport is related to them...

        let case = sitrep.open_case(todo!())?;
    }

    Ok(())
}
