// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for structured reports on planning, i.e., Blueprint generation.
//!
//! Most of the important structures (e.g., `PlanningReport`, the step reports)
//! are defined in [`nexus_types::deployment`] so that they may be shared with
//! the `blueprint_planner` background task and `omdb`.

use nexus_types::deployment::{
    PlanningAddStepReport, PlanningCockroachdbSettingsStepReport,
    PlanningDecommissionStepReport, PlanningExpungeStepReport,
    PlanningMgsUpdatesStepReport, PlanningNoopImageSourceStepReport,
    PlanningReport, PlanningZoneUpdatesStepReport,
};
use omicron_uuid_kinds::BlueprintUuid;

/// A blueprint planning report minus the blueprint ID that the
/// report is for. Returned by [`crate::planner::Planner::do_plan`]
/// when all planning steps are complete, but before the blueprint
/// has been built (and so we don't yet know its ID).
#[derive(Debug)]
pub(crate) struct InterimPlanningReport {
    pub expunge: PlanningExpungeStepReport,
    pub decommission: PlanningDecommissionStepReport,
    pub noop_image_source: PlanningNoopImageSourceStepReport,
    pub mgs_updates: PlanningMgsUpdatesStepReport,
    pub add: PlanningAddStepReport,
    pub zone_updates: PlanningZoneUpdatesStepReport,
    pub cockroachdb_settings: PlanningCockroachdbSettingsStepReport,
}

impl InterimPlanningReport {
    /// Attach a blueprint ID to an interim planning report.
    pub(crate) fn finalize(
        self,
        blueprint_id: BlueprintUuid,
    ) -> PlanningReport {
        let Self {
            expunge,
            decommission,
            noop_image_source,
            mgs_updates,
            add,
            zone_updates,
            cockroachdb_settings,
        } = self;
        PlanningReport {
            blueprint_id,
            expunge,
            decommission,
            noop_image_source,
            mgs_updates,
            add,
            zone_updates,
            cockroachdb_settings,
        }
    }
}
