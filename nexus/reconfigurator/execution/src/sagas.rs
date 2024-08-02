// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-assign sagas from expunged Nexus zones

use nexus_db_model::SecId;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use omicron_common::api::external::Error;
use slog::info;
use std::num::NonZeroU32;

/// Reports what happened when we tried to re-assign sagas
///
/// Callers need to know two things:
///
/// 1. Whether any sagas may have been re-assigned
///    (because they'll want to kick off saga recovery for them)
/// 2. Whether any errors were encountered
#[derive(Debug)]
pub(crate) enum Reassigned {
    /// We successfully re-assigned all sagas that needed it
    All { count: u32 },
    /// We encountered an error and cannot tell how many sagas were re-assigned.
    /// It was at least this many.  (The count can be zero, but it's still
    /// possible that some were re-assigned.)
    AtLeast { count: u32, error: Error },
}

/// For each expunged Nexus zone, re-assign sagas owned by that Nexus to the
/// specified nexus (`nexus_id`).
// TODO-dap activate recovery background task
pub(crate) async fn reassign_sagas_from_expunged(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_id: SecId,
) -> Reassigned {
    let log = &opctx.log;

    // Identify any Nexus zones that have been expunged and need to have sagas
    // re-assigned.
    let nexus_zones_ids: Vec<_> = blueprint
        .all_omicron_zones(BlueprintZoneFilter::Expunged)
        .filter_map(|(_, z)| z.zone_type.is_nexus().then(|| z.id))
        .collect();

    debug!(log, "re-assign sagas: found Nexus instances";
        "nexus_zones_ids" => ?nexus_zones_ids);


    let result = datastore.sagas_reassign_sec_batched(opctx, &nexus_zone_ids, nexus_id);
    info!(log, "re-assign sagas";
        "nexus_zones_ids" => ?nexus_zones_ids);

}
