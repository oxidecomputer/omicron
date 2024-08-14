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
use omicron_uuid_kinds::GenericUuid;
use slog::{debug, info, warn};

/// For each expunged Nexus zone, re-assign sagas owned by that Nexus to the
/// specified nexus (`nexus_id`).
pub(crate) async fn reassign_sagas_from_expunged(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_id: SecId,
) -> Result<bool, Error> {
    let log = &opctx.log;

    // Identify any Nexus zones that have been expunged and need to have sagas
    // re-assigned.
    let nexus_zone_ids: Vec<_> = blueprint
        .all_omicron_zones(BlueprintZoneFilter::Expunged)
        .filter_map(|(_, z)| {
            z.zone_type
                .is_nexus()
                .then(|| nexus_db_model::SecId(z.id.into_untyped_uuid()))
        })
        .collect();

    debug!(log, "re-assign sagas: found Nexus instances";
        "nexus_zone_ids" => ?nexus_zone_ids);

    let result =
        datastore.sagas_reassign_sec(opctx, &nexus_zone_ids, nexus_id).await;

    match result {
        Ok(count) => {
            info!(log, "re-assigned sagas";
                "nexus_zone_ids" => ?nexus_zone_ids,
                "count" => count,
            );

            Ok(count != 0)
        }
        Err(error) => {
            warn!(log, "failed to re-assign sagas";
                "nexus_zone_ids" => ?nexus_zone_ids,
                &error,
            );

            Err(error)
        }
    }
}

// We do not have tests at this layer since it's so thin.  The datastore
// operation (which is the main thing above) is tested separately.  There's an
// integration test in Nexus that tests not only that we re-assigned any
// in-progress sagas but that the recovery process picked them up and completed
// them.
