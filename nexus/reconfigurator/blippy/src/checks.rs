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
                SledKind::DuplicateUnderlayIp {
                    zone1: previous.clone(),
                    zone2: zone.clone(),
                },
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
                        zone: zone.clone(),
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
                                zone1: prev.get().1.clone(),
                                zone2: zone.clone(),
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
                    zone1: prev_zone.clone(),
                    zone2: zone.clone(),
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
                    zone1: prev_zone.clone(),
                    zone2: zone.clone(),
                    ip: nic.ip,
                },
            );
        }
        if let Some(prev_zone) = used_nic_macs.insert(nic.mac, zone) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::DuplicateNicMac {
                    zone1: prev_zone.clone(),
                    zone2: zone.clone(),
                    mac: nic.mac,
                },
            );
        }
    }

    // The loop above noted any exact duplicates; we should also check for any
    // SNAT / Floating overlaps. For each SNAT IP, ensure we don't have a
    // floating IP at the same address.
    for (ip, (sled_id, zone2)) in used_external_snat_ips {
        if let Some(&zone1) = used_external_floating_ips.get(&ip) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::DuplicateExternalIp {
                    zone1: zone1.clone(),
                    zone2: zone2.clone(),
                    ip,
                },
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
                        zone1: previous.clone(),
                        zone2: zone.clone(),
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
                        zone1: previous.clone(),
                        zone2: zone.clone(),
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
                        zone: zone.clone(),
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
                                dataset1: (*prev.get()).clone(),
                                dataset2: dataset.clone(),
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
                                zone: zone_config.clone(),
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
                            zone: zone_config.clone(),
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
                SledKind::OrphanedDataset { dataset: dataset.clone() },
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
                SledKind::DatasetOnNonexistentZpool {
                    dataset: dataset.clone(),
                },
            );
            continue;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blippy::Kind;
    use crate::blippy::Note;
    use crate::BlippyReportSortKey;
    use nexus_reconfigurator_planning::example::example;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::deployment::BlueprintZoneType;
    use omicron_test_utils::dev::test_setup_log;

    // The tests below all take the example blueprint, mutate in some invalid
    // way, and confirm that blippy reports the invalidity. This test confirms
    // the unmutated blueprint has no blippy notes.
    #[test]
    fn test_example_blueprint_is_blippy_clean() {
        static TEST_NAME: &str = "test_example_blueprint_is_blippy_clean";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, blueprint) = example(&logctx.log, TEST_NAME);

        let report =
            Blippy::new(&blueprint).into_report(BlippyReportSortKey::Kind);
        if !report.notes().is_empty() {
            eprintln!("{}", report.display());
            panic!("example blueprint should have no blippy notes");
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_duplicate_underlay_ips() {
        static TEST_NAME: &str = "test_duplicate_underlay_ips";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Copy the underlay IP from one Nexus to another.
        let mut nexus_iter = blueprint.blueprint_zones.iter_mut().flat_map(
            |(sled_id, zones_config)| {
                zones_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_nexus() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            },
        );
        let (nexus0_sled_id, nexus0) =
            nexus_iter.next().expect("at least one Nexus zone");
        let (nexus1_sled_id, nexus1) =
            nexus_iter.next().expect("at least two Nexus zones");
        assert_ne!(nexus0_sled_id, nexus1_sled_id);

        let dup_ip = nexus0.underlay_ip();
        match &mut nexus1.zone_type {
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address,
                ..
            }) => {
                internal_address.set_ip(dup_ip);
            }
            _ => unreachable!("this is a Nexus zone"),
        };

        // This illegal modification should result in at least three notes: a
        // duplicate underlay IP, duplicate sled subnets, and sled1 having mixed
        // underlay subnets (the details of which depend on the ordering of
        // zones, so we'll sort that out here).
        let nexus0 = nexus0.clone();
        let nexus1 = nexus1.clone();
        let (mixed_underlay_zone1, mixed_underlay_zone2) = {
            let mut sled1_zones = blueprint
                .blueprint_zones
                .get(&nexus1_sled_id)
                .unwrap()
                .zones
                .iter();
            let sled1_zone1 = sled1_zones.next().expect("at least one zone");
            let sled1_zone2 = sled1_zones.next().expect("at least two zones");
            if sled1_zone1.id == nexus1.id {
                (nexus1.clone(), sled1_zone2.clone())
            } else {
                (sled1_zone1.clone(), nexus1.clone())
            }
        };
        let expected_notes = [
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: nexus1_sled_id,
                    kind: SledKind::DuplicateUnderlayIp {
                        zone1: nexus0.clone(),
                        zone2: nexus1.clone(),
                    },
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: nexus1_sled_id,
                    kind: SledKind::SledWithMixedUnderlaySubnets {
                        zone1: mixed_underlay_zone1,
                        zone2: mixed_underlay_zone2,
                    },
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: nexus1_sled_id,
                    kind: SledKind::ConflictingSledSubnets {
                        other_sled: nexus0_sled_id,
                        subnet: Ipv6Subnet::new(dup_ip),
                    },
                },
            },
        ];

        let report =
            Blippy::new(&blueprint).into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        for note in expected_notes {
            assert!(
                report.notes().contains(&note),
                "did not find expected note {note:?}"
            );
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_bad_internnal_dns_subnet() {
        static TEST_NAME: &str = "test_bad_internnal_dns_subnet";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Change the second internal DNS zone to be from a different rack
        // subnet.
        let mut internal_dns_iter = blueprint
            .blueprint_zones
            .iter_mut()
            .flat_map(|(sled_id, zones_config)| {
                zones_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_internal_dns() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            });
        let (dns0_sled_id, dns0) =
            internal_dns_iter.next().expect("at least one internal DNS zone");
        let (dns1_sled_id, dns1) =
            internal_dns_iter.next().expect("at least two internal DNS zones");
        assert_ne!(dns0_sled_id, dns1_sled_id);

        let dns0_ip = dns0.underlay_ip();
        let rack_subnet = DnsSubnet::from_addr(dns0_ip).rack_subnet();
        let different_rack_subnet = {
            // Flip the high bit of the existing underlay IP to guarantee a
            // different rack subnet
            let hi_bit = 1_u128 << 127;
            let lo_bits = !hi_bit;
            let hi_bit_ip = Ipv6Addr::from(hi_bit);
            let lo_bits_ip = Ipv6Addr::from(lo_bits);
            // Build XOR out of the operations we have...
            let flipped_ip = if hi_bit_ip & dns0_ip == hi_bit_ip {
                dns0_ip & lo_bits_ip
            } else {
                dns0_ip | hi_bit_ip
            };
            DnsSubnet::from_addr(flipped_ip).rack_subnet()
        };
        let different_dns_subnet = different_rack_subnet.get_dns_subnet(0);

        match &mut dns1.zone_type {
            BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    http_address,
                    dns_address,
                    ..
                },
            ) => {
                http_address.set_ip(different_dns_subnet.dns_address());
                dns_address.set_ip(different_dns_subnet.dns_address());
            }
            _ => unreachable!("this is an internal DNS zone"),
        };

        let expected_note = Note {
            severity: Severity::Fatal,
            kind: Kind::Sled {
                sled_id: dns1_sled_id,
                kind: SledKind::InternalDnsZoneBadSubnet {
                    zone: dns1.clone(),
                    rack_dns_subnets: rack_subnet
                        .get_dns_subnets()
                        .into_iter()
                        .collect(),
                },
            },
        };

        let report =
            Blippy::new(&blueprint).into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        assert!(
            report.notes().contains(&expected_note),
            "did not find expected note {expected_note:?}"
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_duplicate_external_ip() {
        static TEST_NAME: &str = "test_duplicate_external_ip";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Copy the external IP from one Nexus to another.
        let mut nexus_iter = blueprint.blueprint_zones.iter_mut().flat_map(
            |(sled_id, zones_config)| {
                zones_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_nexus() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            },
        );
        let (nexus0_sled_id, nexus0) =
            nexus_iter.next().expect("at least one Nexus zone");
        let (nexus1_sled_id, nexus1) =
            nexus_iter.next().expect("at least two Nexus zones");
        assert_ne!(nexus0_sled_id, nexus1_sled_id);

        let dup_ip = match nexus0
            .zone_type
            .external_networking()
            .expect("Nexus has external networking")
            .0
        {
            OmicronZoneExternalIp::Floating(ip) => ip,
            OmicronZoneExternalIp::Snat(_) => {
                unreachable!("Nexus has a floating IP")
            }
        };
        match &mut nexus1.zone_type {
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                external_ip,
                ..
            }) => {
                *external_ip = dup_ip;
            }
            _ => unreachable!("this is a Nexus zone"),
        };

        let expected_notes = [
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: nexus1_sled_id,
                    kind: SledKind::DuplicateExternalIp {
                        zone1: nexus0.clone(),
                        zone2: nexus1.clone(),
                        ip: dup_ip.ip,
                    },
                },
            },
        ];

        let report =
            Blippy::new(&blueprint).into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        for note in expected_notes {
            assert!(
                report.notes().contains(&note),
                "did not find expected note {note:?}"
            );
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_duplicate_nic_ip() {
        static TEST_NAME: &str = "test_duplicate_nic_ip";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Copy the external IP from one Nexus to another.
        let mut nexus_iter = blueprint.blueprint_zones.iter_mut().flat_map(
            |(sled_id, zones_config)| {
                zones_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_nexus() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            },
        );
        let (nexus0_sled_id, nexus0) =
            nexus_iter.next().expect("at least one Nexus zone");
        let (nexus1_sled_id, nexus1) =
            nexus_iter.next().expect("at least two Nexus zones");
        assert_ne!(nexus0_sled_id, nexus1_sled_id);

        let dup_ip = nexus0
            .zone_type
            .external_networking()
            .expect("Nexus has external networking")
            .1
            .ip;
        match &mut nexus1.zone_type {
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                nic,
                ..
            }) => {
                nic.ip = dup_ip;
            }
            _ => unreachable!("this is a Nexus zone"),
        };

        let expected_notes = [
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: nexus1_sled_id,
                    kind: SledKind::DuplicateNicIp {
                        zone1: nexus0.clone(),
                        zone2: nexus1.clone(),
                        ip: dup_ip,
                    },
                },
            },
        ];

        let report =
            Blippy::new(&blueprint).into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        for note in expected_notes {
            assert!(
                report.notes().contains(&note),
                "did not find expected note {note:?}"
            );
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_duplicate_nic_mac() {
        static TEST_NAME: &str = "test_duplicate_nic_mac";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Copy the external IP from one Nexus to another.
        let mut nexus_iter = blueprint.blueprint_zones.iter_mut().flat_map(
            |(sled_id, zones_config)| {
                zones_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_nexus() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            },
        );
        let (nexus0_sled_id, nexus0) =
            nexus_iter.next().expect("at least one Nexus zone");
        let (nexus1_sled_id, nexus1) =
            nexus_iter.next().expect("at least two Nexus zones");
        assert_ne!(nexus0_sled_id, nexus1_sled_id);

        let dup_mac = nexus0
            .zone_type
            .external_networking()
            .expect("Nexus has external networking")
            .1
            .mac;
        match &mut nexus1.zone_type {
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                nic,
                ..
            }) => {
                nic.mac = dup_mac;
            }
            _ => unreachable!("this is a Nexus zone"),
        };

        let expected_notes = [
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: nexus1_sled_id,
                    kind: SledKind::DuplicateNicMac {
                        zone1: nexus0.clone(),
                        zone2: nexus1.clone(),
                        mac: dup_mac,
                    },
                },
            },
        ];

        let report =
            Blippy::new(&blueprint).into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        for note in expected_notes {
            assert!(
                report.notes().contains(&note),
                "did not find expected note {note:?}"
            );
        }

        logctx.cleanup_successful();
    }
}
