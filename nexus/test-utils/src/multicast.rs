// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast-specific Nexus integration test helpers.
//!
//! Wraps the long-running sim instances exposed by the test starter
//! (`DdmInstance`) with function-style helpers that synchronize them
//! against state in the datastore.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use nexus_db_queries::context::OpContext;
use nexus_test_interface::NexusServer;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Asset;
use nexus_types::inventory::Collection;
use omicron_test_utils::dev::maghemite::{
    PeerMap, SimPeerStatus, sim_peer_info,
};
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::GenericUuid;
use slog::warn;

use crate::ControlPlaneTestContext;

const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Populate every switch zone's `DdmInstance` peer table from the in-service
/// sleds recorded in the datastore.
///
/// The multicast reconciler prefers DDM peer topology and falls back to
/// inventory only when DDM is empty or unreachable. Production runs the
/// real `ddmd`, which populates peers; tests run the in-process
/// `DdmInstance` simulator, which starts with an empty peer table.
///
/// This util synthesizes the production primary path: it waits for
/// inventory to report SP entries for every in-service sled, looks up
/// each sled's `sp_slot` from inventory the same way the reconciler's
/// fallback does (`find_sp_for_sled` matches by serial number), and
/// injects a peer per switch with the synthetic interface name
/// `tfportrear<sp_slot>_0` matching `parse_ddm_if_name_to_port`'s
/// expected format and the rear port the inventory fallback would
/// resolve to.
///
/// Both paths agree on port info by construction. Deriving `sp_slot`
/// from the same inventory the fallback uses guarantees that toggling
/// between the primary path and the fallback yields an identical
/// sled-to-port mapping.
///
/// `DdmInstance::set_peers` has replace semantics, so calling this
/// multiple times always yields a fresh map. Removed sleds drop and
/// new ones appear.
///
/// Tests that explicitly want to exercise the inventory fallback should
/// follow this call with [`clear_ddm_peers`] (or skip the helper entirely).
pub async fn populate_ddm_peers<N: NexusServer>(
    cptestctx: &ControlPlaneTestContext<N>,
) {
    let log = &cptestctx.logctx.log;
    let datastore = cptestctx.server.datastore();
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());

    let sleds = datastore
        .sled_list_all_batched(&opctx, SledFilter::InService)
        .await
        .expect("failed to list in-service sleds for DDM peer population");
    let current_ids: BTreeSet<uuid::Uuid> =
        sleds.iter().map(|sled| sled.id().into_untyped_uuid()).collect();

    // Snapshot the cache without holding the lock across the inventory
    // wait. On a hit, we're done; on a miss we drop the lock, do the
    // wait + build, then take the lock again to publish. Concurrent
    // misses may each build their own (idempotent) `PeerMap`; last
    // writer wins, which is harmless because builds converge on the
    // same answer for a given sled-set.
    let cached = cptestctx.multicast_ddm_peers.lock().unwrap().clone();
    let peers = match cached {
        Some((ids, peers)) if ids == current_ids => peers,
        _ => {
            // Wait until inventory has both a sled-agent record and an
            // SP entry for every in-service sled, then capture that
            // collection so we can resolve `sp_slot` per sled below.
            let expected_serials: HashSet<String> = sleds
                .iter()
                .map(|sled| sled.serial_number().to_string())
                .collect();
            let expected_sled_ids: HashSet<uuid::Uuid> =
                current_ids.iter().copied().collect();
            let collection = wait_for_inventory_with_sleds(
                cptestctx,
                &expected_sled_ids,
                &expected_serials,
            )
            .await;

            // Build the peer map. Match SPs to sleds by serial number
            // in the same way the reconciler's inventory fallback does.
            // The synthetic interface name `tfportrear<sp_slot>_0`
            // round-trips through our parser to the same rear port the
            // fallback would resolve to.
            let new_peers: PeerMap = sleds
                .iter()
                .map(|sled| {
                    let sp = collection
                        .sps
                        .iter()
                        .find(|(bb, _)| {
                            bb.serial_number == sled.serial_number()
                                && bb.part_number == sled.part_number()
                        })
                        .or_else(|| {
                            collection.sps.iter().find(|(bb, _)| {
                                bb.serial_number == sled.serial_number()
                            })
                        })
                        .map(|(_, sp)| sp)
                        .unwrap_or_else(|| {
                            panic!(
                                "no inventory SP entry for sled {} (serial \
                                 {}); inventory subset check should have \
                                 caught this",
                                sled.id(),
                                sled.serial_number(),
                            )
                        });
                    let host = sled.serial_number().to_string();
                    let if_name = format!("tfportrear{}_0", sp.sp_slot);
                    (
                        host.clone(),
                        sim_peer_info(
                            sled.ip(),
                            &host,
                            &if_name,
                            0, // kind: 0 = server router
                            SimPeerStatus::Active,
                        ),
                    )
                })
                .collect();

            *cptestctx.multicast_ddm_peers.lock().unwrap() =
                Some((current_ids, new_peers.clone()));
            new_peers
        }
    };

    // Iterate switches in `SwitchSlot` order so log output across test
    // passes is deterministic. `set_peers` has replace semantics, so
    // cloning per switch is safe and supports tests that interleave
    // `clear_ddm_peers`.
    let switches: BTreeMap<_, _> = cptestctx.ddm.iter().collect();
    for ddm in switches.values() {
        ddm.set_peers(peers.clone());
    }
}

/// Clear every switch zone's `DdmInstance` peer table.
///
/// Typically, use this in tests that exercise `fetch_sled_mapping_from_inventory`.
///
/// The reconciler treats an empty DDM peer response as having "no live topology",
/// which forces the inventory lookup production uses when DDM is genuinely
/// down.
pub fn clear_ddm_peers<N: NexusServer>(cptestctx: &ControlPlaneTestContext<N>) {
    for ddm in cptestctx.ddm.values() {
        ddm.set_peers(PeerMap::new());
    }
}

/// Wait until inventory contains both a sled-agent record *and* an SP entry
/// for every sled in `expected_sled_ids`, then return the collection.
///
/// Both checks are required: `populate_ddm_peers` synthesizes peers from
/// `collection.sps`, so a sled-agent-only check could let the helper exit
/// early and panic in the SP lookup if MGS hasn't published the SP yet.
async fn wait_for_inventory_with_sleds<N: NexusServer>(
    cptestctx: &ControlPlaneTestContext<N>,
    expected_sled_ids: &HashSet<uuid::Uuid>,
    expected_serials: &HashSet<String>,
) -> Arc<Collection> {
    let log = cptestctx.logctx.log.clone();
    let server = &cptestctx.server;
    wait_for_condition::<_, (), _, _>(
        || async {
            match server.inventory_collect_and_get_latest_collection().await {
                Ok(Some(collection)) => {
                    let inv_sled_ids: HashSet<_> = collection
                        .sled_agents
                        .iter()
                        .map(|sled_agent| {
                            sled_agent.sled_id.into_untyped_uuid()
                        })
                        .collect();
                    let inv_sp_serials: HashSet<_> = collection
                        .sps
                        .keys()
                        .map(|bb| bb.serial_number.to_string())
                        .collect();

                    if expected_sled_ids.is_subset(&inv_sled_ids)
                        && expected_serials.is_subset(&inv_sp_serials)
                    {
                        Ok(Arc::new(collection))
                    } else {
                        Err(CondCheckError::NotYet)
                    }
                }
                Ok(None) => Err(CondCheckError::NotYet),
                Err(e) => {
                    warn!(log, "inventory fetch failed: {e}");
                    Err(CondCheckError::NotYet)
                }
            }
        },
        &READY_POLL_INTERVAL,
        &READY_TIMEOUT,
    )
    .await
    .expect("inventory did not catch up to in-service sleds and SPs")
}
