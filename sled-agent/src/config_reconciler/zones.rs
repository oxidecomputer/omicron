// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use id_map::IdMap;
use illumos_utils::running_zone::RunningZone;
use illumos_utils::zpool::ZpoolName;
use illumos_utils::zpool::ZpoolOrRamdisk;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;

#[derive(Debug, thiserror::Error)]
pub enum ZoneError {
    #[error("bad zone config: zone is running on ramdisk ?!")]
    ZoneIsRunningOnRamdisk,
    #[error(
        "invalid filesystem_pool in new zone config: \
         expected pool {expected_pool} but got {got_pool}"
    )]
    InvalidFilesystemPoolZoneConfig {
        expected_pool: ZpoolName,
        got_pool: ZpoolName,
    },
}

#[derive(Debug)]
pub struct ZoneMap {
    zones: BTreeMap<OmicronZoneUuid, Result<OmicronZone, ZoneError>>,
}

// A running zone and the configuration which started it.
#[derive(Debug)]
struct OmicronZone {
    runtime: RunningZone,
    config: OmicronZoneConfig,
}

#[derive(Debug)]
pub(super) struct ReconciledNewZonesRequest<'a> {
    pub(super) zones_to_be_removed: BTreeSet<OmicronZoneUuid>,
    pub(super) zones_to_be_added: HashSet<&'a OmicronZoneConfig>,
    pub(super) config_errors: BTreeMap<OmicronZoneUuid, ZoneError>,
}

impl<'a> ReconciledNewZonesRequest<'a> {
    pub fn new(
        existing_zones: &mut ZoneMap,
        new_request: &'a IdMap<OmicronZoneConfig>,
        log: &Logger,
    ) -> Self {
        reconcile_running_zones_with_new_request_impl(
            existing_zones.zones.values_mut().filter_map(
                |result| match result {
                    Ok(z) => Some((&mut z.config, z.runtime.root_zpool())),
                    Err(_) => None,
                },
            ),
            new_request,
            log,
        )
    }
}

// Separate helper function for `reconcile_running_zones_with_new_request` that
// allows unit tests to exercise the implementation without having to construct
// a `&mut MutexGuard<'_, ZoneMap>` for `existing_zones`.
fn reconcile_running_zones_with_new_request_impl<'a, 'b>(
    existing_zones_with_runtime_zpool: impl Iterator<
        Item = (&'a mut OmicronZoneConfig, &'a ZpoolOrRamdisk),
    >,
    new_request: &'b IdMap<OmicronZoneConfig>,
    log: &Logger,
) -> ReconciledNewZonesRequest<'b> {
    let mut existing_zones_by_id: BTreeMap<_, _> =
        existing_zones_with_runtime_zpool
            .map(|(zone, zpool)| (zone.id, (zone, zpool)))
            .collect();
    let mut zones_to_be_added = HashSet::new();
    let mut zones_to_be_removed = BTreeSet::new();
    let mut config_errors = BTreeMap::new();
    let mut zones_to_update = Vec::new();

    for zone in new_request.iter() {
        let Some((existing_zone, runtime_zpool)) =
            existing_zones_by_id.remove(&zone.id)
        else {
            // This zone isn't in the existing set; add it.
            zones_to_be_added.insert(zone);
            continue;
        };

        // We're already running this zone. If the config hasn't changed, we
        // have nothing to do.
        if zone == existing_zone {
            continue;
        }

        // Special case for fixing #7229. We have an incoming request for a zone
        // that we're already running except the config has changed; normally,
        // we'd shut the zone down and restart it. However, if we get a new
        // request that is:
        //
        // 1. setting `filesystem_pool`, and
        // 2. the config for this zone is otherwise identical, and
        // 3. the new `filesystem_pool` matches the pool on which the zone is
        //    installed
        //
        // then we don't want to shut the zone down and restart it, because the
        // config hasn't actually changed in any meaningful way; this is just
        // reconfigurator correcting #7229.
        if let Some(new_filesystem_pool) = &zone.filesystem_pool {
            let differs_only_by_filesystem_pool = {
                // Clone `existing_zone` and mutate its `filesystem_pool` to
                // match the new request; if they now match, that's the only
                // field that's different.
                let mut existing = existing_zone.clone();
                existing.filesystem_pool = Some(new_filesystem_pool.clone());
                existing == *zone
            };

            let runtime_zpool = match runtime_zpool {
                ZpoolOrRamdisk::Zpool(zpool_name) => zpool_name,
                ZpoolOrRamdisk::Ramdisk => {
                    // The only zone we run on the ramdisk is the switch
                    // zone, for which it isn't possible to get a zone
                    // request, so it should be fine to put an
                    // `unreachable!()` here. Out of caution for future
                    // changes, we'll instead return an error that the
                    // requested zone is on the ramdisk.
                    error!(
                        log,
                        "fix-7229: unexpectedly received request with a \
                         zone config for a zone running on ramdisk";
                        "new_config" => ?zone,
                        "existing_config" => ?existing_zone,
                    );
                    config_errors
                        .insert(zone.id, ZoneError::ZoneIsRunningOnRamdisk);
                    continue;
                }
            };

            if differs_only_by_filesystem_pool {
                if new_filesystem_pool == runtime_zpool {
                    // Our #7229 special case: the new config is only filling in
                    // the pool, and it does so correctly. Move on to the next
                    // zone in the request without adding this zone to either of
                    // our `zone_to_be_*` sets.
                    info!(
                        log,
                        "fix-7229: accepted new zone config that changes only \
                         filesystem_pool";
                        "new_config" => ?zone,
                    );

                    // We should update this `existing_zone`, but delay doing so
                    // until we've processed all zones (so if there are any
                    // failures later, we don't return having partially-updated
                    // the existing zones).
                    zones_to_update.push((existing_zone, zone));
                    continue;
                } else {
                    error!(
                        log,
                        "fix-7229: rejected new zone config that changes only \
                         filesystem_pool (incorrect pool)";
                        "new_config" => ?zone,
                        "expected_pool" => %runtime_zpool,
                    );
                    config_errors.insert(
                        zone.id,
                        ZoneError::InvalidFilesystemPoolZoneConfig {
                            expected_pool: runtime_zpool.clone(),
                            got_pool: new_filesystem_pool.clone(),
                        },
                    );
                    continue;
                }
            }
        }

        // End of #7229 special case: this zone is already running, but the new
        // request has changed it in some way. We need to shut it down and
        // restart it.
        zones_to_be_removed.insert(existing_zone.id);
        zones_to_be_added.insert(zone);
    }

    // Any remaining entries in `existing_zones_by_id` should be shut down.
    zones_to_be_removed.extend(existing_zones_by_id.into_keys());

    // All zones have been handled successfully; commit any changes to existing
    // zones we found in our "fix 7229" special case above.
    let num_zones_updated = zones_to_update.len();
    for (existing_zone, new_zone) in zones_to_update {
        *existing_zone = new_zone.clone();
    }

    info!(
        log,
        "ensure_all_omicron_zones: request reconciliation done";
        "num_zones_to_be_removed" => zones_to_be_removed.len(),
        "num_zones_to_be_added" => zones_to_be_added.len(),
        "num_zones_updated" => num_zones_updated,
        "num_config_errors" => config_errors.len(),
    );
    ReconciledNewZonesRequest {
        zones_to_be_removed,
        zones_to_be_added,
        config_errors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
    use nexus_sled_agent_shared::inventory::OmicronZoneType;
    use omicron_uuid_kinds::ZpoolUuid;

    #[test]
    fn test_fix_7229_zone_config_reconciliation() {
        fn make_omicron_zone_config(
            filesystem_pool: Option<&ZpoolName>,
        ) -> OmicronZoneConfig {
            OmicronZoneConfig {
                id: OmicronZoneUuid::new_v4(),
                filesystem_pool: filesystem_pool.cloned(),
                zone_type: OmicronZoneType::Oximeter {
                    address: "[::1]:0".parse().unwrap(),
                },
                image_source: OmicronZoneImageSource::InstallDataset,
            }
        }

        let logctx =
            omicron_test_utils::dev::test_setup_log("test_ensure_service");
        let log = &logctx.log;

        let some_zpools = (0..10)
            .map(|_| ZpoolName::new_external(ZpoolUuid::new_v4()))
            .collect::<Vec<_>>();

        // Test 1: We have some zones; the new config makes no changes.
        {
            let mut existing = vec![
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[0].clone()),
                ),
                (
                    make_omicron_zone_config(Some(&some_zpools[1])),
                    ZpoolOrRamdisk::Zpool(some_zpools[1].clone()),
                ),
                (
                    make_omicron_zone_config(Some(&some_zpools[2])),
                    ZpoolOrRamdisk::Zpool(some_zpools[2].clone()),
                ),
            ];
            let new_request =
                existing.iter().map(|(zone, _)| zone.clone()).collect();
            let reconciled = reconcile_running_zones_with_new_request_impl(
                existing.iter_mut().map(|(z, p)| (z, &*p)),
                &new_request,
                log,
            );
            assert_eq!(reconciled.zones_to_be_removed, BTreeSet::new());
            assert_eq!(reconciled.zones_to_be_added, HashSet::new());
            assert_eq!(
                existing.iter().map(|(z, _)| z.clone()).collect::<IdMap<_>>(),
                new_request,
            );
        }

        // Test 2: We have some zones; the new config changes `filesystem_pool`
        // to match our runtime pools (i.e., the #7229 fix).
        {
            let mut existing = vec![
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[0].clone()),
                ),
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[1].clone()),
                ),
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[2].clone()),
                ),
            ];
            let new_request = existing
                .iter()
                .enumerate()
                .map(|(i, (zone, _))| {
                    let mut zone = zone.clone();
                    zone.filesystem_pool = Some(some_zpools[i].clone());
                    zone
                })
                .collect();
            let reconciled = reconcile_running_zones_with_new_request_impl(
                existing.iter_mut().map(|(z, p)| (z, &*p)),
                &new_request,
                log,
            );
            assert_eq!(reconciled.zones_to_be_removed, BTreeSet::new());
            assert_eq!(reconciled.zones_to_be_added, HashSet::new());
            assert_eq!(
                existing.iter().map(|(z, _)| z.clone()).collect::<IdMap<_>>(),
                new_request,
            );
        }

        // Test 3: We have some zones; the new config changes `filesystem_pool`
        // to match our runtime pools (i.e., the #7229 fix) but also changes
        // something else in the config for the final zone; we should attempt to
        // remove and re-add that final zone.
        {
            let mut existing = vec![
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[0].clone()),
                ),
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[1].clone()),
                ),
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[2].clone()),
                ),
            ];
            let new_request = existing
                .iter()
                .enumerate()
                .map(|(i, (zone, _))| {
                    let mut zone = zone.clone();
                    zone.filesystem_pool = Some(some_zpools[i].clone());
                    if i == 2 {
                        zone.zone_type = OmicronZoneType::Oximeter {
                            address: "[::1]:10000".parse().unwrap(),
                        };
                    }
                    zone
                })
                .collect();
            let reconciled = reconcile_running_zones_with_new_request_impl(
                existing.iter_mut().map(|(z, p)| (z, &*p)),
                &new_request,
                log,
            );

            assert_eq!(
                reconciled.zones_to_be_removed,
                BTreeSet::from([existing[2].0.id]),
            );
            assert_eq!(
                reconciled.zones_to_be_added,
                HashSet::from([new_request.get(&existing[2].0.id).unwrap()]),
            );
            // The first two existing zones should have been updated to match
            // the new request.
            for (z, _) in &existing[..2] {
                assert_eq!(Some(z), new_request.get(&z.id));
            }
        }

        // Test 4: We have some zones; the new config changes `filesystem_pool`
        // to match our runtime pools (i.e., the #7229 fix), except the new pool
        // on the final zone is incorrect. We should get an error back.
        {
            let mut existing = vec![
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[0].clone()),
                ),
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[1].clone()),
                ),
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[2].clone()),
                ),
            ];
            let new_request = existing
                .iter()
                .enumerate()
                .map(|(i, (zone, _))| {
                    let mut zone = zone.clone();
                    if i < 2 {
                        zone.filesystem_pool = Some(some_zpools[i].clone());
                    } else {
                        zone.filesystem_pool = Some(some_zpools[4].clone());
                    }
                    zone
                })
                .collect();
            let mut reconciled = reconcile_running_zones_with_new_request_impl(
                existing.iter_mut().map(|(z, p)| (z, &*p)),
                &new_request,
                log,
            );

            let err = reconciled
                .config_errors
                .remove(&existing[2].0.id)
                .expect("zone reported as config error");
            match err {
                ZoneError::InvalidFilesystemPoolZoneConfig {
                    expected_pool,
                    got_pool,
                } => {
                    assert_eq!(expected_pool, some_zpools[2]);
                    assert_eq!(got_pool, some_zpools[4]);
                }
                _ => panic!("unexpected error: {err}"),
            }

            // That should have been the only reconcilation error.
            assert!(
                reconciled.config_errors.is_empty(),
                "unexpectedly have more errors: {:?}",
                reconciled.config_errors
            );

            // The other two zones should have been updated.
            for (z, _) in &existing[..2] {
                assert_eq!(Some(z), new_request.get(&z.id));
            }
        }

        // Test 5: We have some zones. The new config applies #7229 fix to the
        // first zone, doesn't include the remaining zones, and adds some new
        // zones. We should see "the remaining zones" removed, the "new zones"
        // added, and the 7229-fixed zone not in either set.
        {
            let mut existing = vec![
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[0].clone()),
                ),
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[1].clone()),
                ),
                (
                    make_omicron_zone_config(None),
                    ZpoolOrRamdisk::Zpool(some_zpools[2].clone()),
                ),
            ];
            let new_request_raw = [
                {
                    let mut z = existing[0].0.clone();
                    z.filesystem_pool = Some(some_zpools[0].clone());
                    z
                },
                make_omicron_zone_config(None),
                make_omicron_zone_config(None),
            ];
            let new_request = new_request_raw.clone().into_iter().collect();
            let reconciled = reconcile_running_zones_with_new_request_impl(
                existing.iter_mut().map(|(z, p)| (z, &*p)),
                &new_request,
                log,
            );

            assert_eq!(
                reconciled.zones_to_be_removed,
                BTreeSet::from_iter(existing[1..].iter().map(|(z, _)| z.id)),
            );
            assert_eq!(
                reconciled.zones_to_be_added,
                HashSet::from_iter(
                    new_request_raw[1..]
                        .iter()
                        .filter_map(|z| new_request.get(&z.id))
                ),
            );
            // Only the first existing zone is being kept; ensure it matches the
            // new request.
            assert_eq!(
                Some(&existing[0].0),
                new_request.get(&existing[0].0.id)
            );
        }
        logctx.cleanup_successful();
    }
}
