// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blippy::Blippy;
use crate::blippy::Severity;
use crate::blippy::SledKind;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetFilter;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::OmicronZoneExternalIp;
use omicron_common::address::DnsSubnet;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::disk::DatasetKind;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;

pub(crate) fn perform_all_blueprint_only_checks(blippy: &mut Blippy<'_>) {
    check_underlay_ips(blippy);
    check_external_networking(blippy);
    check_dataset_zpool_uniqueness(blippy);
    check_datasets(blippy);
}

fn check_underlay_ips(blippy: &mut Blippy<'_>) {
    let mut underlay_ips: BTreeMap<Ipv6Addr, &BlueprintZoneConfig> =
        BTreeMap::new();
    let mut inferred_sled_subnets_by_sled: BTreeMap<
        SledUuid,
        (Ipv6Subnet<SLED_PREFIX>, &BlueprintZoneConfig),
    > = BTreeMap::new();
    let mut inferred_sled_subnets_by_subnet: BTreeMap<
        Ipv6Subnet<SLED_PREFIX>,
        SledUuid,
    > = BTreeMap::new();
    let mut rack_dns_subnets: BTreeSet<DnsSubnet> = BTreeSet::new();

    for (sled_id, zone) in blippy
        .blueprint()
        .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
    {
        let ip = zone.underlay_ip();

        // There should be no duplicate underlay IPs.
        if let Some(previous) = underlay_ips.insert(ip, zone) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::DuplicateUnderlayIp { zone1: previous, zone2: zone },
            );
        }

        if zone.zone_type.is_internal_dns() {
            // Internal DNS zones should have IPs coming from the reserved rack
            // DNS subnets.
            let subnet = DnsSubnet::from_addr(ip);
            if rack_dns_subnets.is_empty() {
                // The blueprint doesn't store the rack subnet explicitly, so we
                // infer it based on the first internal DNS zone we see.
                rack_dns_subnets.extend(subnet.rack_subnet().get_dns_subnets());
            }
            if !rack_dns_subnets.contains(&subnet) {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::InternalDnsZoneBadSubnet {
                        zone,
                        rack_dns_subnets: rack_dns_subnets.clone(),
                    },
                );
            }
        } else {
            let subnet = Ipv6Subnet::new(ip);

            // Any given subnet should be used by at most one sled.
            match inferred_sled_subnets_by_subnet.entry(subnet) {
                Entry::Vacant(slot) => {
                    slot.insert(sled_id);
                }
                Entry::Occupied(prev) => {
                    if *prev.get() != sled_id {
                        blippy.push_sled_note(
                            sled_id,
                            Severity::Fatal,
                            SledKind::ConflictingSledSubnets {
                                other_sled: *prev.get(),
                                subnet,
                            },
                        );
                    }
                }
            }

            // Any given sled should have IPs within at most one subnet.
            //
            // The blueprint doesn't store sled subnets explicitly, so we can't
            // check that each sled is using the subnet it's supposed to. The
            // best we can do is check that the sleds are internally consistent.
            match inferred_sled_subnets_by_sled.entry(sled_id) {
                Entry::Vacant(slot) => {
                    slot.insert((subnet, zone));
                }
                Entry::Occupied(prev) => {
                    if prev.get().0 != subnet {
                        blippy.push_sled_note(
                            sled_id,
                            Severity::Fatal,
                            SledKind::SledWithMixedUnderlaySubnets {
                                zone1: prev.get().1,
                                zone2: zone,
                            },
                        );
                    }
                }
            }
        }
    }
}

fn check_external_networking(blippy: &mut Blippy<'_>) {
    let mut used_external_ips = BTreeMap::new();
    let mut used_external_floating_ips = BTreeMap::new();
    let mut used_external_snat_ips = BTreeMap::new();

    let mut used_nic_ips = BTreeMap::new();
    let mut used_nic_macs = BTreeMap::new();

    for (sled_id, zone, external_ip, nic) in blippy
        .blueprint()
        .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
        .filter_map(|(sled_id, zone)| {
            zone.zone_type
                .external_networking()
                .map(|(external_ip, nic)| (sled_id, zone, external_ip, nic))
        })
    {
        // There should be no duplicate external IPs.
        if let Some(prev_zone) = used_external_ips.insert(external_ip, zone) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::DuplicateExternalIp {
                    zone1: prev_zone,
                    zone2: zone,
                    ip: external_ip.ip(),
                },
            );
        }

        // See the loop below; we build up separate maps to check for
        // Floating/SNAT overlap that wouldn't be caught by the exact
        // `used_external_ips` map above.
        match external_ip {
            OmicronZoneExternalIp::Floating(floating) => {
                used_external_floating_ips.insert(floating.ip, zone);
            }
            OmicronZoneExternalIp::Snat(snat) => {
                used_external_snat_ips
                    .insert(snat.snat_cfg.ip, (sled_id, zone));
            }
        }

        // There should be no duplicate NIC IPs or MACs.
        if let Some(prev_zone) = used_nic_ips.insert(nic.ip, zone) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::DuplicateNicIp {
                    zone1: prev_zone,
                    zone2: zone,
                    ip: nic.ip,
                },
            );
        }
        if let Some(prev_zone) = used_nic_macs.insert(nic.mac, zone) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::DuplicateNicMac {
                    zone1: prev_zone,
                    zone2: zone,
                    mac: nic.mac,
                },
            );
        }
    }

    // The loop above noted any exact duplicates; we should also check for any
    // SNAT / Floating overlaps. For each SNAT IP, ensure we don't have a
    // floating IP at the same address.
    for (ip, (sled_id, zone2)) in used_external_snat_ips {
        if let Some(zone1) = used_external_floating_ips.get(&ip) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::DuplicateExternalIp { zone1, zone2, ip },
            );
        }
    }
}

fn check_dataset_zpool_uniqueness(blippy: &mut Blippy<'_>) {
    let mut durable_kinds_by_zpool: BTreeMap<ZpoolUuid, BTreeMap<ZoneKind, _>> =
        BTreeMap::new();
    let mut transient_kinds_by_zpool: BTreeMap<
        ZpoolUuid,
        BTreeMap<ZoneKind, _>,
    > = BTreeMap::new();

    // On any given zpool, we should have at most one zone of any given
    // kind.
    for (sled_id, zone) in
        // TODO-john in `verify_blueprint` the filter here was `::All`, but I
        // think that's wrong? It prevents (e.g.) a Nexus from running on a
        // zpool where there was a previously-expunged Nexus. We only care about
        // uniqueness-per-zpool for live zones, right?
        blippy
            .blueprint()
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
    {
        // Check "one kind per zpool" for durable datasets...
        if let Some(dataset) = zone.zone_type.durable_dataset() {
            let kind = zone.zone_type.kind();
            if let Some(previous) = durable_kinds_by_zpool
                .entry(dataset.dataset.pool_name.id())
                .or_default()
                .insert(kind, zone)
            {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::ZoneDurableDatasetCollision {
                        zone1: previous,
                        zone2: zone,
                        zpool: dataset.dataset.pool_name.clone(),
                    },
                );
            }
        }

        // ... and transient datasets.
        if let Some(dataset) = zone.filesystem_dataset() {
            let kind = zone.zone_type.kind();
            if let Some(previous) = transient_kinds_by_zpool
                .entry(dataset.pool().id())
                .or_default()
                .insert(kind, zone)
            {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::ZpoolFilesystemDatasetCollision {
                        zone1: previous,
                        zone2: zone,
                        zpool: dataset.into_parts().0,
                    },
                );
            }
        }

        // If a zone has both durable and transient datasets, they should be on
        // the same pool.
        match (zone.zone_type.durable_zpool(), zone.filesystem_pool.as_ref()) {
            (Some(durable), Some(transient)) if durable != transient => {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::ZoneWithDatasetsOnDifferentZpools {
                        zone,
                        durable_zpool: durable.clone(),
                        transient_zpool: transient.clone(),
                    },
                );
            }
            _ => (),
        }
    }
}

type DatasetByKind<'a> = BTreeMap<DatasetKind, &'a BlueprintDatasetConfig>;
type DatasetsByZpool<'a> = BTreeMap<ZpoolUuid, DatasetByKind<'a>>;

#[derive(Debug)]
struct DatasetsBySled<'a> {
    by_sled: BTreeMap<SledUuid, DatasetsByZpool<'a>>,
    noted_sleds_missing_datasets: BTreeSet<SledUuid>,
}

impl<'a> DatasetsBySled<'a> {
    fn new(blippy: &mut Blippy<'a>) -> Self {
        let mut by_sled = BTreeMap::new();

        for (&sled_id, config) in &blippy.blueprint().blueprint_datasets {
            let by_zpool: &mut BTreeMap<_, _> =
                by_sled.entry(sled_id).or_default();

            for dataset in config.datasets.values() {
                let by_kind: &mut BTreeMap<_, _> =
                    by_zpool.entry(dataset.pool.id()).or_default();

                match by_kind.entry(dataset.kind.clone()) {
                    Entry::Vacant(slot) => {
                        slot.insert(dataset);
                    }
                    Entry::Occupied(prev) => {
                        blippy.push_sled_note(
                            sled_id,
                            Severity::Fatal,
                            SledKind::ZpoolWithDuplicateDatasetKinds {
                                dataset1: prev.get(),
                                dataset2: dataset,
                                zpool: dataset.pool.id(),
                            },
                        );
                    }
                }
            }
        }

        Self { by_sled, noted_sleds_missing_datasets: BTreeSet::new() }
    }

    // Get the datasets for each zpool on a given sled, or add a fatal note to
    // `blippy` that the sled is missing an entry in `blueprint_datasets` for
    // the specified reason `why`.
    fn get_sled_or_note_missing(
        &mut self,
        blippy: &mut Blippy<'_>,
        sled_id: SledUuid,
        why: &'static str,
    ) -> Option<&DatasetsByZpool<'a>> {
        let maybe_datasets = self.by_sled.get(&sled_id);
        if maybe_datasets.is_none()
            && self.noted_sleds_missing_datasets.insert(sled_id)
        {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::SledMissingDatasets { why },
            );
        }
        maybe_datasets
    }
}

fn check_datasets(blippy: &mut Blippy<'_>) {
    let mut datasets = DatasetsBySled::new(blippy);

    // As we loop through all the datasets we expect to see, mark them down.
    // Afterwards, we'll check for any datasets present that we _didn't_ expect
    // to see.
    let mut expected_datasets = BTreeSet::new();

    // All disks should have debug and zone root datasets.
    //
    // TODO-correctness We currently only include in-service disks in the
    // blueprint; once we include expunged or decommissioned disks too, we
    // should filter here to only in-service.
    for (&sled_id, disk_config) in &blippy.blueprint().blueprint_disks {
        let Some(sled_datasets) = datasets.get_sled_or_note_missing(
            blippy,
            sled_id,
            "sled has an entry in blueprint_disks",
        ) else {
            continue;
        };

        for disk in &disk_config.disks {
            let sled_datasets = sled_datasets.get(&disk.pool_id);

            match sled_datasets
                .and_then(|by_zpool| by_zpool.get(&DatasetKind::Debug))
            {
                Some(dataset) => {
                    expected_datasets.insert(dataset.id);
                }
                None => {
                    blippy.push_sled_note(
                        sled_id,
                        Severity::Fatal,
                        SledKind::ZpoolMissingDebugDataset {
                            zpool: disk.pool_id,
                        },
                    );
                }
            }

            match sled_datasets.and_then(|by_zpool| {
                by_zpool.get(&DatasetKind::TransientZoneRoot)
            }) {
                Some(dataset) => {
                    expected_datasets.insert(dataset.id);
                }
                None => {
                    blippy.push_sled_note(
                        sled_id,
                        Severity::Fatal,
                        SledKind::ZpoolMissingZoneRootDataset {
                            zpool: disk.pool_id,
                        },
                    );
                }
            }
        }
    }

    // There should be a dataset for every dataset referenced by a running zone
    // (filesystem or durable).
    for (sled_id, zone_config) in blippy
        .blueprint()
        .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
    {
        let Some(sled_datasets) = datasets.get_sled_or_note_missing(
            blippy,
            sled_id,
            "sled has running zones",
        ) else {
            continue;
        };

        match &zone_config.filesystem_dataset() {
            Some(dataset) => {
                match sled_datasets
                    .get(&dataset.pool().id())
                    .and_then(|by_zpool| by_zpool.get(dataset.dataset()))
                {
                    Some(dataset) => {
                        expected_datasets.insert(dataset.id);
                    }
                    None => {
                        blippy.push_sled_note(
                            sled_id,
                            Severity::Fatal,
                            SledKind::ZoneMissingFilesystemDataset {
                                zone: zone_config,
                            },
                        );
                    }
                }
            }
            None => {
                // TODO-john Add a Severity::BackwardsCompatibility and note the
                // missing filesystem pool
            }
        }

        if let Some(dataset) = zone_config.zone_type.durable_dataset() {
            match sled_datasets
                .get(&dataset.dataset.pool_name.id())
                .and_then(|by_zpool| by_zpool.get(&dataset.kind))
            {
                Some(dataset) => {
                    expected_datasets.insert(dataset.id);
                }
                None => {
                    blippy.push_sled_note(
                        sled_id,
                        Severity::Fatal,
                        SledKind::ZoneMissingDurableDataset {
                            zone: zone_config,
                        },
                    );
                }
            }
        }
    }

    // TODO-correctness We currently only include in-service disks in the
    // blueprint; once we include expunged or decommissioned disks too, we
    // should filter here to only in-service.
    let in_service_sled_zpools = blippy
        .blueprint()
        .blueprint_disks
        .iter()
        .map(|(sled_id, disk_config)| {
            (
                sled_id,
                disk_config
                    .disks
                    .iter()
                    .map(|disk| disk.pool_id)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut noted_sleds_without_disks = BTreeSet::new();

    // All datasets should be on zpools that have disk records, and all datasets
    // should have been referenced by either a zone or a disk above.
    for (sled_id, dataset) in blippy
        .blueprint()
        .all_omicron_datasets(BlueprintDatasetFilter::InService)
    {
        if !expected_datasets.contains(&dataset.id) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::OrphanedDataset { dataset },
            );
            continue;
        }

        let Some(sled_zpools) = in_service_sled_zpools.get(&sled_id) else {
            if noted_sleds_without_disks.insert(sled_id) {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::SledMissingDisks {
                        why: "sled has in-service datasets",
                    },
                );
            }
            continue;
        };

        if !sled_zpools.contains(&dataset.pool.id()) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::DatasetOnNonexistentZpool { dataset },
            );
            continue;
        }
    }
}
