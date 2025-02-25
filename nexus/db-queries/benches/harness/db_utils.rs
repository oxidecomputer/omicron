// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database test helpers
//!
//! These are largely ripped out of "nexus/db-queries/src/db/datastore".
//!
//! Benchmarks are compiled as external binaries from library crates, so we
//! can only access `pub` code.
//!
//! It may be worth refactoring some of these functions to a test utility
//! crate to avoid the de-duplication.

use anyhow::Context;
use anyhow::Result;
use nexus_db_model::Sled;
use nexus_db_model::SledReservationConstraintBuilder;
use nexus_db_model::SledUpdate;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pub_test_utils::helpers::small_resource_request;
use nexus_db_queries::db::pub_test_utils::helpers::SledUpdateBuilder;
use nexus_db_queries::db::DataStore;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use uuid::Uuid;

pub fn rack_id() -> Uuid {
    Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
}

const USABLE_HARDWARE_THREADS: u32 = 32;

pub fn test_new_sled_update() -> SledUpdate {
    let mut sled = SledUpdateBuilder::new();
    sled.rack_id(rack_id())
        .hardware()
        .usable_hardware_threads(USABLE_HARDWARE_THREADS);
    sled.build()
}

pub async fn create_sleds(datastore: &DataStore, count: usize) -> Vec<Sled> {
    let mut sleds = vec![];
    for _ in 0..count {
        let (sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        sleds.push(sled);
    }
    sleds
}

/// Given a `sled_count`, returns the number of times a call to
/// `create_reservation` should succeed.
///
/// This can be used to validate parameters before running benchmarks.
pub fn max_resource_request_count(sled_count: usize) -> usize {
    let threads_per_request: usize =
        small_resource_request().hardware_threads.0.try_into().unwrap();
    let threads_per_sled: usize = USABLE_HARDWARE_THREADS.try_into().unwrap();

    threads_per_sled * sled_count / threads_per_request
}

pub async fn create_reservation(
    opctx: &OpContext,
    db: &DataStore,
    instance_id: InstanceUuid,
) -> Result<PropolisUuid> {
    let vmm_id = PropolisUuid::new_v4();

    loop {
        match db
            .sled_reservation_create(
                &opctx,
                instance_id,
                vmm_id,
                small_resource_request(),
                SledReservationConstraintBuilder::new().build(),
            )
            .await
        {
            Ok(_) => break,
            Err(err) => {
                // This condition is bad - it would result in a user-visible
                // error, in most cases - but it's also an indication of failure
                // due to contention. We normally bubble this out to users,
                // rather than stalling the request, but in this particular
                // case, we choose to retry immediately.
                if err.to_string().contains("restart transaction") {
                    continue;
                }
                return Err(err).context("Failed to create reservation");
            }
        }
    }
    Ok(vmm_id)
}

pub async fn delete_reservation(
    opctx: &OpContext,
    db: &DataStore,
    vmm_id: PropolisUuid,
) -> Result<()> {
    db.sled_reservation_delete(&opctx, vmm_id)
        .await
        .context("Failed to delete reservation")?;
    Ok(())
}
