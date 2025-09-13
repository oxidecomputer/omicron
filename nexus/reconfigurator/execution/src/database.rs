// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages deployment of records into the database.

use anyhow::anyhow;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::blueprint_zone_type;
use omicron_uuid_kinds::OmicronZoneUuid;
use nexus_types::deployment::BlueprintZoneType;

/// Idempotently ensure that the Nexus records for the zones are populated
/// in the database.
pub(crate) async fn deploy_db_metadata_nexus_records(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_id: OmicronZoneUuid,
) -> Result<(), anyhow::Error> {
    // To determine what state to use for new records, we need to know which is
    // the currently active Nexus generation.  That is necessarily the
    // generation number of the Nexus instance that's doing the execution.
    let nexus_generation = blueprint
        // XXX-dap add helper for iterating over in-service Nexus zones
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .find_map(|(_sled_id, z)| {
            if z.id != nexus_id {
                return None;
            }

            let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                nexus_generation,
                ..
            }) = &z.zone_type
            else {
                // This should be impossible.
                return None;
            };

            Some(*nexus_generation)
        })
        .ok_or_else(|| {
            anyhow!(
                "did not find nexus generation for current \
                 Nexus zone ({nexus_id})"
            )
        })?;

    datastore
        .database_nexus_access_create(opctx, blueprint, nexus_generation)
        .await
        .map_err(|err| anyhow!(err))?;
    Ok(())
}
