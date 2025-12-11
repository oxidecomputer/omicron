// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blippy::Blippy;
use crate::blippy::BlueprintKind;
use crate::blippy::PlanningInputKind;
use crate::blippy::Severity;
use crate::blippy::SledKind;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintDatasetConfig;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
use nexus_types::deployment::BlueprintSledConfig;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::OmicronZoneExternalIp;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::blueprint_zone_type;
use omicron_common::address::DnsSubnet;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Generation;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::MupdateOverrideUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry;
use std::net::IpAddr;
use std::net::Ipv6Addr;

pub(crate) fn perform_planning_input_checks(
    blippy: &mut Blippy<'_>,
    input: &PlanningInput,
) {
    check_planning_input_network_records_appear_in_blueprint(blippy, input);
}

pub(crate) fn perform_all_blueprint_only_checks(blippy: &mut Blippy<'_>) {
    check_underlay_ips(blippy);
    check_external_networking(blippy);
    check_dataset_zpool_uniqueness(blippy);
    check_datasets(blippy);
    check_mupdate_override(blippy);
    check_nexus_generation_consistency(blippy);
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
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
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
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
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

        // There should be no duplicate NIC IPs (of either version) or MACs.
        if let Some(ipv4) = nic.ip_config.ipv4_addr() {
            let ip = std::net::IpAddr::V4(*ipv4);
            if let Some(prev_zone) = used_nic_ips.insert(ip, zone) {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::DuplicateNicIp {
                        zone1: prev_zone.clone(),
                        zone2: zone.clone(),
                        ip,
                    },
                );
            }
        }
        if let Some(ipv6) = nic.ip_config.ipv6_addr() {
            let ip = std::net::IpAddr::V6(*ipv6);
            if let Some(prev_zone) = used_nic_ips.insert(ip, zone) {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::DuplicateNicIp {
                        zone1: prev_zone.clone(),
                        zone2: zone.clone(),
                        ip,
                    },
                );
            }
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
    for (sled_id, zone) in blippy
        .blueprint()
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
    {
        // Check "one kind per zpool" for transient datasets...
        let filesystem_dataset = zone.filesystem_dataset();
        let kind = zone.zone_type.kind();
        if let Some(previous) = transient_kinds_by_zpool
            .entry(filesystem_dataset.pool().id())
            .or_default()
            .insert(kind, zone)
        {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::ZoneFilesystemDatasetCollision {
                    zone1: previous.clone(),
                    zone2: zone.clone(),
                    zpool: *filesystem_dataset.pool(),
                },
            );
        }

        if let Some(durable_dataset) = zone.zone_type.durable_dataset() {
            let kind = zone.zone_type.kind();

            // ... and durable datasets.
            if let Some(previous) = durable_kinds_by_zpool
                .entry(durable_dataset.dataset.pool_name.id())
                .or_default()
                .insert(kind, zone)
            {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::ZoneDurableDatasetCollision {
                        zone1: previous.clone(),
                        zone2: zone.clone(),
                        zpool: durable_dataset.dataset.pool_name,
                    },
                );
            }

            // If a zone has a durable dataset, it should be on the same pool as
            // its transient filesystem dataset.
            if durable_dataset.dataset.pool_name != *filesystem_dataset.pool() {
                blippy.push_sled_note(
                    sled_id,
                    Severity::Fatal,
                    SledKind::ZoneWithDatasetsOnDifferentZpools {
                        zone: zone.clone(),
                        durable_zpool: durable_dataset.dataset.pool_name,
                        transient_zpool: *filesystem_dataset.pool(),
                    },
                );
            }
        }
    }
}

type DatasetByKind<'a> = BTreeMap<DatasetKind, &'a BlueprintDatasetConfig>;
type DatasetsByZpool<'a> = BTreeMap<ZpoolUuid, DatasetByKind<'a>>;

#[derive(Debug, Default)]
struct DatasetsBySledCache<'a> {
    by_sled: BTreeMap<SledUuid, DatasetsByZpool<'a>>,
}

impl<'a> DatasetsBySledCache<'a> {
    fn get_cached(
        &mut self,
        blippy: &mut Blippy<'_>,
        sled_id: SledUuid,
        sled_config: &'a BlueprintSledConfig,
    ) -> &DatasetsByZpool<'a> {
        let vacant_entry = match self.by_sled.entry(sled_id) {
            Entry::Vacant(vacant_entry) => vacant_entry,
            Entry::Occupied(occupied_entry) => {
                return occupied_entry.into_mut();
            }
        };

        let mut by_zpool = BTreeMap::new();
        for dataset in sled_config.datasets.iter() {
            if dataset.disposition.is_expunged() {
                continue;
            }

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

        vacant_entry.insert(by_zpool)
    }
}

fn check_datasets(blippy: &mut Blippy<'_>) {
    let mut datasets = DatasetsBySledCache::default();

    // As we loop through all the datasets we expect to see, mark them down.
    // Afterwards, we'll check for any datasets present that we _didn't_ expect
    // to see.
    let mut expected_datasets = BTreeSet::new();

    // In a check below, we want to look up Crucible zones by zpool; build that
    // map as we perform the next set of checks.
    let mut crucible_zone_by_zpool = BTreeMap::new();

    // All disks should have debug, zone root, and local storage datasets.
    for (&sled_id, sled_config) in &blippy.blueprint().sleds {
        let sled_datasets = datasets.get_cached(blippy, sled_id, sled_config);

        for disk in
            sled_config.disks.iter().filter(|d| d.disposition.is_in_service())
        {
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

            match sled_datasets
                .and_then(|by_zpool| by_zpool.get(&DatasetKind::LocalStorage))
            {
                Some(dataset) => {
                    expected_datasets.insert(dataset.id);
                }
                None => {
                    blippy.push_sled_note(
                        sled_id,
                        Severity::Fatal,
                        SledKind::ZpoolMissingLocalStorageDataset {
                            zpool: disk.pool_id,
                        },
                    );
                }
            }
        }

        // There should be a dataset for every dataset referenced by a running
        // zone (filesystem or durable).
        for zone_config in
            sled_config.zones.iter().filter(|z| z.disposition.is_in_service())
        {
            let dataset = zone_config.filesystem_dataset();
            match sled_datasets
                .get(&dataset.pool().id())
                .and_then(|by_zpool| by_zpool.get(dataset.kind()))
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

                if dataset.kind == DatasetKind::Crucible {
                    match &zone_config.zone_type {
                        BlueprintZoneType::Crucible(crucible_zone_config) => {
                            crucible_zone_by_zpool.insert(
                                dataset.dataset.pool_name.id(),
                                crucible_zone_config,
                            );
                        }
                        _ => unreachable!(
                            "zone_type.durable_dataset() returned Crucible for \
                             non-Crucible zone type"
                        ),
                    }
                }
            }
        }
    }

    let mut in_service_sled_zpools: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
    for (sled_id, disk_config) in blippy
        .blueprint()
        .all_omicron_disks(BlueprintPhysicalDiskDisposition::is_in_service)
    {
        in_service_sled_zpools
            .entry(sled_id)
            .or_default()
            .insert(disk_config.pool_id);
    }

    // All datasets should be on zpools that have disk records, and all datasets
    // should have been referenced by either a zone or a disk above.
    for (sled_id, dataset) in blippy
        .blueprint()
        .all_omicron_datasets(BlueprintDatasetDisposition::is_in_service)
    {
        if !expected_datasets.contains(&dataset.id) {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::OrphanedDataset { dataset: dataset.clone() },
            );
            continue;
        }

        let sled_zpools = in_service_sled_zpools.entry(sled_id).or_default();
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

    // All Crucible datasets should have their address set to the address of the
    // Crucible zone on their same zpool, and all non-Crucible datasets should
    // not have addresses.
    for (sled_id, dataset) in blippy
        .blueprint()
        .all_omicron_datasets(BlueprintDatasetDisposition::is_in_service)
    {
        match dataset.kind {
            DatasetKind::Crucible => {
                let Some(blueprint_zone_type::Crucible { address, .. }) =
                    crucible_zone_by_zpool.get(&dataset.pool.id())
                else {
                    // We already checked above that all datasets have
                    // corresponding zones, so a failure to find the zone for
                    // this dataset would have already produced a note; just
                    // skip it.
                    continue;
                };
                if dataset.address != Some(*address) {
                    blippy.push_sled_note(
                        sled_id,
                        Severity::Fatal,
                        SledKind::CrucibleDatasetWithIncorrectAddress {
                            dataset: dataset.clone(),
                            expected_address: *address,
                        },
                    );
                }
            }
            _ => {
                if let Some(address) = dataset.address {
                    blippy.push_sled_note(
                        sled_id,
                        Severity::Fatal,
                        SledKind::NonCrucibleDatasetWithAddress {
                            dataset: dataset.clone(),
                            address,
                        },
                    );
                }
            }
        }
    }
}

fn check_mupdate_override(blippy: &mut Blippy<'_>) {
    // Perform checks for invariants that should be upheld if
    // remove_mupdate_override is set for a sled.
    for (&sled_id, sled) in &blippy.blueprint().sleds {
        if !sled.state.matches(SledFilter::InService) {
            continue;
        }

        if let Some(mupdate_override_id) = sled.remove_mupdate_override {
            // All in-service zones should be set to InstallDataset.
            for zone in &sled.zones {
                if zone.disposition.is_in_service() {
                    match &zone.image_source {
                        BlueprintZoneImageSource::InstallDataset => {
                            // This is valid.
                        }
                        BlueprintZoneImageSource::Artifact {
                            version,
                            hash,
                        } => {
                            // This is invalid -- if remove_mupdate_override is
                            // set, all zones must be InstallDataset.
                            blippy.push_sled_note(
                                sled_id,
                                Severity::Fatal,
                                SledKind::MupdateOverrideWithArtifactZone {
                                    mupdate_override_id,
                                    zone: zone.clone(),
                                    version: version.clone(),
                                    hash: *hash,
                                },
                            );
                        }
                    }
                }
            }

            // The host phase 2 contents should be set to CurrentContents.
            check_mupdate_override_host_phase_2_contents(
                blippy,
                sled_id,
                mupdate_override_id,
                M2Slot::A,
                &sled.host_phase_2.slot_a,
            );
            check_mupdate_override_host_phase_2_contents(
                blippy,
                sled_id,
                mupdate_override_id,
                M2Slot::B,
                &sled.host_phase_2.slot_b,
            );

            // TODO: PendingMgsUpdates for this sled should be empty. Mapping
            // sled IDs to their MGS identifiers (baseboard ID) requires a map
            // that's not currently part of the blueprint. We may want to either
            // include that map in the blueprint, or pass it in via blippy.
        }
    }
}

fn check_mupdate_override_host_phase_2_contents(
    blippy: &mut Blippy<'_>,
    sled_id: SledUuid,
    mupdate_override_id: MupdateOverrideUuid,
    slot: M2Slot,
    contents: &BlueprintHostPhase2DesiredContents,
) {
    match contents {
        BlueprintHostPhase2DesiredContents::Artifact { version, hash } => {
            blippy.push_sled_note(
                sled_id,
                Severity::Fatal,
                SledKind::MupdateOverrideWithHostPhase2Artifact {
                    mupdate_override_id,
                    slot,
                    version: version.clone(),
                    hash: *hash,
                },
            );
        }
        BlueprintHostPhase2DesiredContents::CurrentContents => {}
    }
}

fn check_nexus_generation_consistency(blippy: &mut Blippy<'_>) {
    use std::collections::HashMap;

    // Map from generation -> (sled_id, image_source, zone)
    let mut generation_info: HashMap<
        Generation,
        Vec<(SledUuid, BlueprintZoneImageSource, &BlueprintZoneConfig)>,
    > = HashMap::new();

    // Collect all Nexus zones and their generations
    for (sled_id, zone) in blippy
        .blueprint()
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
    {
        if let BlueprintZoneType::Nexus(nexus) = &zone.zone_type {
            generation_info.entry(nexus.nexus_generation).or_default().push((
                sled_id,
                zone.image_source.clone(),
                zone,
            ));
        }
    }

    // Check that the top-level Nexus generation is consistent with the images
    let active_gen = blippy.blueprint().nexus_generation;
    if !generation_info.contains_key(&active_gen) {
        blippy.push_blueprint_note(
            Severity::Fatal,
            BlueprintKind::NoZonesWithActiveNexusGeneration(active_gen),
        );
        return;
    };

    // Check each generation for image source consistency
    for (generation, zones_with_gen) in &generation_info {
        // Take the first zone as the reference
        let (ref_sled_id, ref_image_source, ref_zone) = &zones_with_gen[0];

        if *generation > active_gen.next() {
            blippy.push_sled_note(
                *ref_sled_id,
                Severity::Fatal,
                SledKind::NexusZoneGenerationTooNew {
                    active_generation: active_gen,
                    zone_generation: *generation,
                    id: ref_zone.id,
                },
            );
        }

        if zones_with_gen.len() < 2 {
            // Only one zone with this generation, no consistency issue
            continue;
        }

        // Compare all other zones to the reference
        for (_sled_id, image_source, zone) in &zones_with_gen[1..] {
            if image_source != ref_image_source {
                blippy.push_sled_note(
                    *ref_sled_id,
                    Severity::Fatal,
                    SledKind::NexusZoneGenerationImageSourceMismatch {
                        zone1: (*ref_zone).clone(),
                        zone2: (*zone).clone(),
                        generation: *generation,
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlippyReportSortKey;
    use crate::blippy::Kind;
    use crate::blippy::Note;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_planning::example::example;
    use nexus_types::deployment::BlueprintArtifactVersion;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::blueprint_zone_type;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::MupdateOverrideUuid;
    use std::mem;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactVersion;

    // The tests below all take the example blueprint, mutate in some invalid
    // way, and confirm that blippy reports the invalidity. This test confirms
    // the unmutated blueprint has no blippy notes.
    #[test]
    fn test_example_blueprint_is_blippy_clean() {
        static TEST_NAME: &str = "test_example_blueprint_is_blippy_clean";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, blueprint) = example(&logctx.log, TEST_NAME);

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
        let mut nexus_iter =
            blueprint.sleds.iter_mut().flat_map(|(sled_id, sled_config)| {
                sled_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_nexus() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            });
        let (nexus0_sled_id, nexus0) =
            nexus_iter.next().expect("at least one Nexus zone");
        let (nexus1_sled_id, mut nexus1) =
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
        let nexus0 = nexus0.into_ref().clone();
        let nexus1 = nexus1.into_ref().clone();
        let (mixed_underlay_zone1, mixed_underlay_zone2) = {
            let mut sled1_zones =
                blueprint.sleds.get(&nexus1_sled_id).unwrap().zones.iter();
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
                    kind: Box::new(SledKind::DuplicateUnderlayIp {
                        zone1: nexus0.clone(),
                        zone2: nexus1.clone(),
                    }),
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: nexus1_sled_id,
                    kind: Box::new(SledKind::SledWithMixedUnderlaySubnets {
                        zone1: mixed_underlay_zone1,
                        zone2: mixed_underlay_zone2,
                    }),
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: nexus1_sled_id,
                    kind: Box::new(SledKind::ConflictingSledSubnets {
                        other_sled: nexus0_sled_id,
                        subnet: Ipv6Subnet::new(dup_ip),
                    }),
                },
            },
        ];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_bad_internal_dns_subnet() {
        static TEST_NAME: &str = "test_bad_internal_dns_subnet";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Change the second internal DNS zone to be from a different rack
        // subnet.
        let mut internal_dns_iter =
            blueprint.sleds.iter_mut().flat_map(|(sled_id, sled_config)| {
                sled_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_internal_dns() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            });
        let (dns0_sled_id, dns0) =
            internal_dns_iter.next().expect("at least one internal DNS zone");
        let (dns1_sled_id, mut dns1) =
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
                kind: Box::new(SledKind::InternalDnsZoneBadSubnet {
                    zone: dns1.clone(),
                    rack_dns_subnets: rack_subnet
                        .get_dns_subnets()
                        .into_iter()
                        .collect(),
                }),
            },
        };

        mem::drop(dns0);
        mem::drop(dns1);

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
        let mut nexus_iter =
            blueprint.sleds.iter_mut().flat_map(|(sled_id, sled_config)| {
                sled_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_nexus() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            });
        let (nexus0_sled_id, nexus0) =
            nexus_iter.next().expect("at least one Nexus zone");
        let (nexus1_sled_id, mut nexus1) =
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

        let expected_notes = [Note {
            severity: Severity::Fatal,
            kind: Kind::Sled {
                sled_id: nexus1_sled_id,
                kind: Box::new(SledKind::DuplicateExternalIp {
                    zone1: nexus0.clone(),
                    zone2: nexus1.clone(),
                    ip: dup_ip.ip,
                }),
            },
        }];

        mem::drop(nexus0);
        mem::drop(nexus1);

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
        let mut nexus_iter =
            blueprint.sleds.iter_mut().flat_map(|(sled_id, sled_config)| {
                sled_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_nexus() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            });
        let (nexus0_sled_id, nexus0) =
            nexus_iter.next().expect("at least one Nexus zone");
        let (nexus1_sled_id, mut nexus1) =
            nexus_iter.next().expect("at least two Nexus zones");
        assert_ne!(nexus0_sled_id, nexus1_sled_id);

        let dup_ip = &nexus0
            .zone_type
            .external_networking()
            .expect("Nexus has external networking")
            .1
            .ip_config;
        match &mut nexus1.zone_type {
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                nic,
                ..
            }) => {
                nic.ip_config = dup_ip.clone();
            }
            _ => unreachable!("this is a Nexus zone"),
        };

        let expected_notes = [Note {
            severity: Severity::Fatal,
            kind: Kind::Sled {
                sled_id: nexus1_sled_id,
                kind: Box::new(SledKind::DuplicateNicIp {
                    zone1: nexus0.clone(),
                    zone2: nexus1.clone(),
                    ip: dup_ip
                        .ipv4_addr()
                        .copied()
                        .expect("an IPv4 address")
                        .into(),
                }),
            },
        }];

        mem::drop(nexus0);
        mem::drop(nexus1);

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
        let mut nexus_iter =
            blueprint.sleds.iter_mut().flat_map(|(sled_id, sled_config)| {
                sled_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_nexus() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            });
        let (nexus0_sled_id, nexus0) =
            nexus_iter.next().expect("at least one Nexus zone");
        let (nexus1_sled_id, mut nexus1) =
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

        let expected_notes = [Note {
            severity: Severity::Fatal,
            kind: Kind::Sled {
                sled_id: nexus1_sled_id,
                kind: Box::new(SledKind::DuplicateNicMac {
                    zone1: nexus0.clone(),
                    zone2: nexus1.clone(),
                    mac: dup_mac,
                }),
            },
        }];

        mem::drop(nexus0);
        mem::drop(nexus1);

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_durable_dataset_collision() {
        static TEST_NAME: &str = "test_durable_dataset_collision";
        let logctx = test_setup_log(TEST_NAME);
        let (_, mut blueprint) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .external_dns_count(2)
                .unwrap()
                .build();

        // Copy the durable zpool from one external DNS to another.
        let mut dns_iter =
            blueprint.sleds.iter_mut().flat_map(|(sled_id, sled_config)| {
                sled_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_external_dns() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            });
        let (dns0_sled_id, dns0) =
            dns_iter.next().expect("at least one external DNS zone");
        let (dns1_sled_id, mut dns1) =
            dns_iter.next().expect("at least two external DNS zones");
        assert_ne!(dns0_sled_id, dns1_sled_id);

        let dup_zpool = *dns0
            .zone_type
            .durable_zpool()
            .expect("external DNS has a durable zpool");
        match &mut dns1.zone_type {
            BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns { dataset, .. },
            ) => {
                dataset.pool_name = dup_zpool;
            }
            _ => unreachable!("this is an external DNS zone"),
        };

        let dns0 = dns0.into_ref().clone();
        let dns1 = dns1.into_ref().clone();

        let expected_notes = [
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: dns1_sled_id,
                    kind: Box::new(SledKind::ZoneDurableDatasetCollision {
                        zone1: dns0.clone(),
                        zone2: dns1.clone(),
                        zpool: dup_zpool,
                    }),
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: dns1_sled_id,
                    kind: Box::new(
                        SledKind::ZoneWithDatasetsOnDifferentZpools {
                            zone: dns1.clone(),
                            durable_zpool: dup_zpool,
                            transient_zpool: dns1.filesystem_pool,
                        },
                    ),
                },
            },
        ];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_transient_root_dataset_collision() {
        static TEST_NAME: &str = "test_transient_root_dataset_collision";
        let logctx = test_setup_log(TEST_NAME);
        let (_, mut blueprint) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .external_dns_count(2)
                .unwrap()
                .build();

        // Copy the filesystem zpool from one external DNS to another.
        let mut dns_iter =
            blueprint.sleds.iter_mut().flat_map(|(sled_id, sled_config)| {
                sled_config.zones.iter_mut().filter_map(move |zone| {
                    if zone.zone_type.is_external_dns() {
                        Some((*sled_id, zone))
                    } else {
                        None
                    }
                })
            });
        let (dns0_sled_id, dns0) =
            dns_iter.next().expect("at least one external DNS zone");
        let (dns1_sled_id, mut dns1) =
            dns_iter.next().expect("at least two external DNS zones");
        assert_ne!(dns0_sled_id, dns1_sled_id);

        let dup_zpool = dns0.filesystem_pool;
        dns1.filesystem_pool = dup_zpool;

        let dns0 = dns0.into_ref().clone();
        let dns1 = dns1.into_ref().clone();
        let expected_notes = [
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: dns1_sled_id,
                    kind: Box::new(SledKind::ZoneFilesystemDatasetCollision {
                        zone1: dns0.clone(),
                        zone2: dns1.clone(),
                        zpool: dup_zpool,
                    }),
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: dns1_sled_id,
                    kind: Box::new(
                        SledKind::ZoneWithDatasetsOnDifferentZpools {
                            zone: dns1.clone(),
                            durable_zpool: *dns1
                                .zone_type
                                .durable_zpool()
                                .unwrap(),
                            transient_zpool: dup_zpool,
                        },
                    ),
                },
            },
        ];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_zpool_with_duplicate_dataset_kinds() {
        static TEST_NAME: &str = "test_zpool_with_duplicate_dataset_kinds";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        let mut by_kind = BTreeMap::new();

        // Loop over the datasets until we find a dataset kind that already
        // exists on a different zpool, then copy it over.
        let mut found_sled_id = None;
        let mut dataset1 = None;
        let mut dataset2 = None;
        let mut zpool = None;
        'outer: for (sled_id, sled_config) in blueprint.sleds.iter_mut() {
            for mut dataset in sled_config.datasets.iter_mut() {
                if let Some(prev) =
                    by_kind.insert(dataset.kind.clone(), dataset.clone())
                {
                    dataset.pool = prev.pool;

                    found_sled_id = Some(*sled_id);
                    dataset1 = Some(prev);
                    dataset2 = Some(dataset.clone());
                    zpool = Some(dataset.pool);
                    break 'outer;
                }
            }
        }
        let sled_id = found_sled_id.expect("found dataset to move");
        let dataset1 = dataset1.expect("found dataset to move");
        let dataset2 = dataset2.expect("found dataset to move");
        let zpool = zpool.expect("found dataset to move");

        let expected_notes = [Note {
            severity: Severity::Fatal,
            kind: Kind::Sled {
                sled_id,
                kind: Box::new(SledKind::ZpoolWithDuplicateDatasetKinds {
                    dataset1,
                    dataset2,
                    zpool: zpool.id(),
                }),
            },
        }];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_zpool_with_expunged_duplicate_dataset_kinds() {
        static TEST_NAME: &str =
            "test_zpool_with_expunged_duplicate_dataset_kinds";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        let mut by_kind = BTreeMap::new();

        // Loop over the datasets until we find a dataset kind that already
        // exists on a different zpool, then copy it over.
        //
        // When we make the copy, also mark it expunged.
        //
        // By marking the dataset expunged, we should not see "duplicate dataset
        // kind" errors.
        let mut found_duplicate = false;
        'outer: for (_, sled_config) in blueprint.sleds.iter_mut() {
            for mut dataset in sled_config.datasets.iter_mut() {
                if let Some(prev) =
                    by_kind.insert(dataset.kind.clone(), dataset.clone())
                {
                    dataset.pool = prev.pool;
                    dataset.disposition = BlueprintDatasetDisposition::Expunged;

                    found_duplicate = true;
                    break 'outer;
                }
            }
        }
        assert!(found_duplicate);

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        for note in report.notes() {
            match &note.kind {
                Kind::Sled { kind, .. } => match &**kind {
                    SledKind::ZpoolWithDuplicateDatasetKinds { .. } => {
                        panic!(
                            "Saw unexpected duplicate dataset kind note: {note:?}"
                        );
                    }
                    _ => (),
                },
                _ => panic!("Unexpected note: {note:?}"),
            }
        }

        logctx.cleanup_successful();
    }

    #[test]
    fn test_zpool_missing_default_datasets() {
        static TEST_NAME: &str = "test_zpool_missing_default_datasets";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Drop the Debug dataset from one zpool and the ZoneRoot dataset from
        // another; we should catch both errors.
        let (sled_id, sled_config) =
            blueprint.sleds.iter_mut().next().expect("at least one sled");

        let mut debug_dataset = None;
        let mut zoneroot_dataset = None;
        for dataset in &mut sled_config.datasets.iter_mut() {
            match &dataset.kind {
                DatasetKind::Debug if debug_dataset.is_none() => {
                    debug_dataset = Some(dataset.clone());
                }
                DatasetKind::TransientZoneRoot
                    if debug_dataset.is_some()
                        && zoneroot_dataset.is_none() =>
                {
                    if Some(&dataset.pool)
                        != debug_dataset.as_ref().map(|d| &d.pool)
                    {
                        zoneroot_dataset = Some(dataset.clone());
                        break;
                    }
                }
                _ => (),
            }
        }
        let debug_dataset =
            debug_dataset.expect("found Debug dataset to prune");
        let zoneroot_dataset =
            zoneroot_dataset.expect("found ZoneRoot dataset to prune");
        assert_ne!(debug_dataset.pool, zoneroot_dataset.pool);

        // Actually strip these from the blueprint.
        sled_config.datasets.retain(|dataset| {
            dataset.id != debug_dataset.id && dataset.id != zoneroot_dataset.id
        });

        let expected_notes = [
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: *sled_id,
                    kind: Box::new(SledKind::ZpoolMissingDebugDataset {
                        zpool: debug_dataset.pool.id(),
                    }),
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: *sled_id,
                    kind: Box::new(SledKind::ZpoolMissingZoneRootDataset {
                        zpool: zoneroot_dataset.pool.id(),
                    }),
                },
            },
        ];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_zone_missing_datasets() {
        static TEST_NAME: &str = "test_zone_missing_datasets";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        let (sled_id, sled_config) =
            blueprint.sleds.iter_mut().next().expect("at least one sled");

        // Pick a zone with a durable dataset to remove, and a different zone
        // with a filesystem_pool dataset to remove.
        let mut durable_zone = None;
        let mut root_zone = None;
        for z in &sled_config.zones {
            if durable_zone.is_none() {
                if z.zone_type.durable_zpool().is_some() {
                    durable_zone = Some(z.clone());
                }
            } else if root_zone.is_none() {
                root_zone = Some(z);
                break;
            }
        }
        let durable_zone =
            durable_zone.expect("found zone with durable dataset to prune");
        let root_zone =
            root_zone.expect("found zone with root dataset to prune");
        assert_ne!(durable_zone.filesystem_pool, root_zone.filesystem_pool);

        // Actually strip these from the blueprint.
        sled_config.datasets.retain(|dataset| {
            let matches_durable = (dataset.pool
                == *durable_zone.zone_type.durable_zpool().unwrap())
                && (dataset.kind
                    == durable_zone.zone_type.durable_dataset().unwrap().kind);
            let root_dataset = root_zone.filesystem_dataset();
            let matches_root = (dataset.pool == *root_dataset.pool())
                && (dataset.kind == *root_dataset.kind());
            !matches_durable && !matches_root
        });

        let expected_notes = [
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: *sled_id,
                    kind: Box::new(SledKind::ZoneMissingFilesystemDataset {
                        zone: root_zone.clone(),
                    }),
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id: *sled_id,
                    kind: Box::new(SledKind::ZoneMissingDurableDataset {
                        zone: durable_zone,
                    }),
                },
            },
        ];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_orphaned_datasets() {
        static TEST_NAME: &str = "test_orphaned_datasets";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Pick two zones (one with a durable dataset and one with a filesystem
        // root dataset), and remove both those zones, which should orphan their
        // datasets.
        let (sled_id, sled_config) =
            blueprint.sleds.iter_mut().next().expect("at least one sled");
        let mut durable_zone = None;
        let mut root_zone = None;
        for z in &sled_config.zones {
            if durable_zone.is_none() {
                if z.zone_type.durable_zpool().is_some() {
                    durable_zone = Some(z.clone());
                }
            } else if root_zone.is_none() {
                root_zone = Some(z.clone());
                break;
            }
        }
        let durable_zone =
            durable_zone.expect("found zone with durable dataset to prune");
        let root_zone =
            root_zone.expect("found zone with root dataset to prune");
        sled_config
            .zones
            .retain(|z| z.id != durable_zone.id && z.id != root_zone.id);

        let durable_dataset = durable_zone.zone_type.durable_dataset().unwrap();
        let root_dataset = root_zone.filesystem_dataset();

        // Find the datasets we expect to have been orphaned.
        let expected_notes = sled_config
            .datasets
            .iter()
            .filter_map(|dataset| {
                if (dataset.pool == durable_dataset.dataset.pool_name
                    && dataset.kind == durable_dataset.kind)
                    || (dataset.pool == *root_dataset.pool()
                        && dataset.kind == *root_dataset.kind())
                {
                    Some(Note {
                        severity: Severity::Fatal,
                        kind: Kind::Sled {
                            sled_id: *sled_id,
                            kind: Box::new(SledKind::OrphanedDataset {
                                dataset: dataset.clone(),
                            }),
                        },
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_dataset_on_nonexistent_zpool() {
        static TEST_NAME: &str = "test_dataset_on_nonexistent_zpool";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Remove one zpool from one sled, then check that all datasets on that
        // zpool produce report notes.
        let (sled_id, sled_config) = blueprint
            .sleds
            .iter_mut()
            .map(|(id, c)| (*id, c))
            .next()
            .expect("at least one sled");
        let first = sled_config.disks.first().unwrap().id;
        let removed_disk = sled_config.disks.remove(&first).unwrap();
        eprintln!("removed disk {removed_disk:?}");

        let expected_notes = blueprint
            .sleds
            .get(&sled_id)
            .unwrap()
            .datasets
            .iter()
            .filter_map(|dataset| {
                if dataset.pool.id() != removed_disk.pool_id {
                    return None;
                }

                let note = match dataset.kind {
                    DatasetKind::Debug
                    | DatasetKind::TransientZoneRoot
                    | DatasetKind::LocalStorage => Note {
                        severity: Severity::Fatal,
                        kind: Kind::Sled {
                            sled_id,
                            kind: Box::new(SledKind::OrphanedDataset {
                                dataset: dataset.clone(),
                            }),
                        },
                    },

                    _ => Note {
                        severity: Severity::Fatal,
                        kind: Kind::Sled {
                            sled_id,
                            kind: Box::new(
                                SledKind::DatasetOnNonexistentZpool {
                                    dataset: dataset.clone(),
                                },
                            ),
                        },
                    },
                };
                Some(note)
            })
            .collect::<Vec<_>>();
        assert!(!expected_notes.is_empty());

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_dataset_with_bad_address() {
        static TEST_NAME: &str = "test_dataset_with_bad_address";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        let crucible_addr_by_zpool = blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(_, z)| match z.zone_type {
                BlueprintZoneType::Crucible(
                    blueprint_zone_type::Crucible { address, .. },
                ) => {
                    let zpool_id = z
                        .zone_type
                        .durable_zpool()
                        .expect("crucible zone has durable zpool")
                        .id();
                    Some((zpool_id, address))
                }
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();

        // We have three ways a dataset address can be wrong:
        //
        // * A Crucible dataset has no address
        // * A Crucible dataset has an address but it doesn't match its zone
        // * A non-Crucible dataset has an address
        //
        // Make all three kinds of modifications to three different datasets and
        // ensure we see all three noted.
        let mut cleared_crucible_addr = false;
        let mut changed_crucible_addr = false;
        let mut set_non_crucible_addr = false;
        let mut expected_notes = Vec::new();

        for (sled_id, sled_config) in blueprint.sleds.iter_mut() {
            for mut dataset in sled_config.datasets.iter_mut() {
                match dataset.kind {
                    DatasetKind::Crucible => {
                        let bad_address = if !cleared_crucible_addr {
                            cleared_crucible_addr = true;
                            None
                        } else if !changed_crucible_addr {
                            changed_crucible_addr = true;
                            Some("[1234:5678:9abc::]:0".parse().unwrap())
                        } else {
                            continue;
                        };

                        dataset.address = bad_address;
                        let expected_address = *crucible_addr_by_zpool
                            .get(&dataset.pool.id())
                            .expect("found crucible zone for zpool");
                        expected_notes.push(Note {
                            severity: Severity::Fatal,
                            kind: Kind::Sled {
                                sled_id: *sled_id,
                                kind: Box::new(SledKind::CrucibleDatasetWithIncorrectAddress {
                                    dataset: dataset.clone(),
                                    expected_address,
                                }),
                            },
                        });
                    }
                    _ => {
                        if !set_non_crucible_addr {
                            set_non_crucible_addr = true;
                            let address = "[::1]:0".parse().unwrap();
                            dataset.address = Some(address);
                            expected_notes.push(Note {
                            severity: Severity::Fatal,
                            kind: Kind::Sled {
                                sled_id: *sled_id,
                                kind: Box::new(SledKind::NonCrucibleDatasetWithAddress {
                                    dataset: dataset.clone(),
                                    address,
                                }),
                            },
                        });
                        }
                    }
                }
            }
        }

        // We should have modified 3 datasets.
        assert_eq!(expected_notes.len(), 3);

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
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
    fn test_mupdate_override_with_artifact_image_source() {
        static TEST_NAME: &str =
            "test_remove_mupdate_override_with_artifact_image_source";
        let logctx = test_setup_log(TEST_NAME);
        let (_, _, mut blueprint) = example(&logctx.log, TEST_NAME);

        // Find a sled with zones and set remove_mupdate_override on it.
        let (&sled_id, sled) = blueprint
            .sleds
            .iter_mut()
            .find(|(_, config)| !config.zones.is_empty())
            .expect("at least one sled with zones");

        // Set the remove_mupdate_override field on the sled.
        let mupdate_override_id = MupdateOverrideUuid::max();
        sled.remove_mupdate_override = Some(mupdate_override_id);

        // Find a zone and set it to use an artifact image source.
        let artifact_zone_kind = {
            let mut zone = sled
                .zones
                .iter_mut()
                .find(|z| z.disposition.is_in_service())
                .expect("at least one in-service zone");

            let version = BlueprintArtifactVersion::Available {
                version: ArtifactVersion::new_const("1.0.0"),
            };
            let hash = ArtifactHash([1u8; 32]);
            zone.image_source = BlueprintZoneImageSource::Artifact {
                version: version.clone(),
                hash,
            };

            SledKind::MupdateOverrideWithArtifactZone {
                mupdate_override_id,
                zone: zone.clone(),
                version,
                hash,
            }
        };

        // Also set the host phase 2 contents.
        let host_phase_2_a_kind = {
            let version = BlueprintArtifactVersion::Available {
                version: ArtifactVersion::new_const("123"),
            };
            let hash = ArtifactHash([2u8; 32]);
            sled.host_phase_2.slot_a =
                BlueprintHostPhase2DesiredContents::Artifact {
                    version: version.clone(),
                    hash,
                };
            SledKind::MupdateOverrideWithHostPhase2Artifact {
                mupdate_override_id,
                slot: M2Slot::A,
                version,
                hash,
            }
        };

        let host_phase_2_b_kind = {
            let version = BlueprintArtifactVersion::Unknown;
            let hash = ArtifactHash([3u8; 32]);
            sled.host_phase_2.slot_b =
                BlueprintHostPhase2DesiredContents::Artifact {
                    version: version.clone(),
                    hash,
                };
            SledKind::MupdateOverrideWithHostPhase2Artifact {
                mupdate_override_id,
                slot: M2Slot::B,
                version,
                hash,
            }
        };

        let expected_notes = vec![
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id,
                    kind: Box::new(artifact_zone_kind),
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id,
                    kind: Box::new(host_phase_2_a_kind),
                },
            },
            Note {
                severity: Severity::Fatal,
                kind: Kind::Sled {
                    sled_id,
                    kind: Box::new(host_phase_2_b_kind),
                },
            },
        ];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        assert_eq!(report.notes(), &expected_notes);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_nexus_generation_no_nexus() {
        static TEST_NAME: &str = "test_nexus_generation_no_nexus";
        let logctx = test_setup_log(TEST_NAME);
        let (_, blueprint) = ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
            .nsleds(1)
            .nexus_count(0)
            .build();

        // Run blippy checks
        let expected_notes = [Note {
            severity: Severity::Fatal,
            kind: Kind::Blueprint(
                BlueprintKind::NoZonesWithActiveNexusGeneration(
                    blueprint.nexus_generation,
                ),
            ),
        }];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        assert_eq!(report.notes(), &expected_notes);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_nexus_generation_image_consistency() {
        static TEST_NAME: &str = "test_nexus_generation_image_consistency";
        let logctx = test_setup_log(TEST_NAME);
        let (_, mut blueprint) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .nsleds(3)
                .nexus_count(3)
                .build();

        // Find the Nexus zones
        let ((sled1, zone1_id), (sled2, zone2_id)) = {
            let nexus_zones: Vec<_> = blueprint
                .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
                .filter_map(|(sled_id, zone)| {
                    if matches!(zone.zone_type, BlueprintZoneType::Nexus(_)) {
                        Some((sled_id, zone))
                    } else {
                        None
                    }
                })
                .collect();

            // Should have exactly 3 Nexus zones
            assert_eq!(nexus_zones.len(), 3);

            // Modify two zones to have the same generation but different image sources
            let (sled1, zone1) = nexus_zones[0];
            let (sled2, zone2) = nexus_zones[1];

            ((sled1, zone1.id), (sled2, zone2.id))
        };

        let generation = Generation::new();

        let zone1 = {
            // Find the zones in the blueprint and modify them
            let mut zone1_config = blueprint
                .sleds
                .get_mut(&sled1)
                .unwrap()
                .zones
                .get_mut(&zone1_id)
                .unwrap();

            match &mut zone1_config.zone_type {
                BlueprintZoneType::Nexus(nexus) => {
                    nexus.nexus_generation = generation;
                }
                _ => unreachable!("this is a Nexus zone"),
            }
            zone1_config.image_source =
                BlueprintZoneImageSource::InstallDataset;
            zone1_config.clone()
        };

        let zone2 = {
            let mut zone2_config = blueprint
                .sleds
                .get_mut(&sled2)
                .unwrap()
                .zones
                .get_mut(&zone2_id)
                .unwrap();

            match &mut zone2_config.zone_type {
                BlueprintZoneType::Nexus(nexus) => {
                    nexus.nexus_generation = generation;
                }
                _ => unreachable!("this is a Nexus zone"),
            }
            zone2_config.image_source = BlueprintZoneImageSource::Artifact {
                version: BlueprintArtifactVersion::Available {
                    version: "1.0.0".parse().unwrap(),
                },
                hash: ArtifactHash([0; 32]),
            };
            zone2_config.clone()
        };

        // Run blippy checks
        let expected_notes = [Note {
            severity: Severity::Fatal,
            kind: Kind::Sled {
                sled_id: sled1,
                kind: Box::new(
                    SledKind::NexusZoneGenerationImageSourceMismatch {
                        zone1,
                        zone2,
                        generation,
                    },
                ),
            },
        }];

        let report = Blippy::new_blueprint_only(&blueprint)
            .into_report(BlippyReportSortKey::Kind);
        eprintln!("{}", report.display());
        assert_eq!(report.notes(), &expected_notes);

        logctx.cleanup_successful();
    }
}

// For a given `PlanningInput` / `Blueprint` pair that could be passed to the
// planner, there should never be any external networking resources in the
// planning input (which is derived from the contents of CRDB) that we don't
// know about from the parent blueprint. It's possible a given planning
// iteration could see such a state if there have been intermediate changes made
// by other Nexus instances; e.g.,
//
// 1. Nexus A generates a `PlanningInput` by reading from CRDB
// 2. Nexus B executes on a target blueprint that removes IPs/NICs from
//    CRDB
// 3. Nexus B regenerates a new blueprint and prunes the zone(s) associated
//    with the IPs/NICs from step 2
// 4. Nexus B makes this new blueprint the target
// 5. Nexus A attempts to run planning with its `PlanningInput` from step 1 but
//    the target blueprint from step 4; this will fail the following checks
//    because the input contains records that were removed in step 3
//
// We do not need to handle this class of error; it's a transient failure that
// will clear itself up when Nexus A repeats its planning loop from the top and
// generates a new `PlanningInput`.
//
// There may still be database records corresponding to _expunged_ zones, but
// that's okay: it just means we haven't yet realized a blueprint where those
// zones are expunged. And those should should still be in the blueprint (not
// pruned) until their database records are cleaned up.
//
// It's also possible that there may be networking records in the database
// assigned to zones that have been expunged, and the blueprint uses those same
// records for new zones. This is also fine and expected, and is a similar case
// to the previous paragraph: a zone with networking resources was expunged, the
// database doesn't realize it yet, but can still move forward and make planning
// decisions that reuse those resources for new zones.
fn check_planning_input_network_records_appear_in_blueprint(
    blippy: &mut Blippy<'_>,
    input: &PlanningInput,
) {
    use nexus_types::deployment::OmicronZoneExternalIp;
    use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
    use omicron_common::address::DNS_OPTE_IPV6_SUBNET;
    use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
    use omicron_common::address::NEXUS_OPTE_IPV6_SUBNET;
    use omicron_common::address::NTP_OPTE_IPV4_SUBNET;
    use omicron_common::address::NTP_OPTE_IPV6_SUBNET;
    use omicron_common::api::external::MacAddr;

    let mut all_macs: BTreeSet<MacAddr> = BTreeSet::new();
    let mut all_nexus_nic_ips: BTreeSet<IpAddr> = BTreeSet::new();
    let mut all_boundary_ntp_nic_ips: BTreeSet<IpAddr> = BTreeSet::new();
    let mut all_external_dns_nic_ips: BTreeSet<IpAddr> = BTreeSet::new();
    let mut all_external_ips: BTreeSet<OmicronZoneExternalIp> = BTreeSet::new();

    // Unlike the construction of the external IP allocator and existing IPs
    // constructed above in `BuilderExternalNetworking::new()`, we do not
    // check for duplicates here: we could very well see reuse of IPs
    // between expunged zones or between expunged -> running zones.
    for (_, z) in
        blippy.blueprint().all_omicron_zones(BlueprintZoneDisposition::any)
    {
        let zone_type = &z.zone_type;
        match zone_type {
            BlueprintZoneType::BoundaryNtp(ntp) => {
                if let Some(ip) = ntp.nic.ip_config.ipv4_addr().copied() {
                    all_boundary_ntp_nic_ips.insert(ip.into());
                }
                if let Some(ip) = ntp.nic.ip_config.ipv6_addr().copied() {
                    all_boundary_ntp_nic_ips.insert(ip.into());
                }
            }
            BlueprintZoneType::Nexus(nexus) => {
                if let Some(ip) = nexus.nic.ip_config.ipv4_addr().copied() {
                    all_nexus_nic_ips.insert(ip.into());
                }
                if let Some(ip) = nexus.nic.ip_config.ipv6_addr().copied() {
                    all_nexus_nic_ips.insert(ip.into());
                }
            }
            BlueprintZoneType::ExternalDns(dns) => {
                if let Some(ip) = dns.nic.ip_config.ipv4_addr().copied() {
                    all_external_dns_nic_ips.insert(ip.into());
                }
                if let Some(ip) = dns.nic.ip_config.ipv6_addr().copied() {
                    all_external_dns_nic_ips.insert(ip.into());
                }
            }
            _ => (),
        }

        if let Some((external_ip, nic)) = zone_type.external_networking() {
            // Ignore localhost (used by the test suite).
            if !external_ip.ip().is_loopback() {
                all_external_ips.insert(external_ip);
            }
            all_macs.insert(nic.mac);
        }
    }
    for external_ip_entry in
        input.network_resources().omicron_zone_external_ips()
    {
        // As above, ignore localhost (used by the test suite).
        if external_ip_entry.ip.ip().is_loopback() {
            continue;
        }
        if !all_external_ips.contains(&external_ip_entry.ip) {
            blippy.push_planning_input_note(
                Severity::Fatal,
                PlanningInputKind::IpNotInBlueprint(external_ip_entry.ip),
            );
        }
    }
    for nic_entry in input.network_resources().omicron_zone_nics() {
        if !all_macs.contains(&nic_entry.nic.mac) {
            blippy.push_planning_input_note(
                Severity::Fatal,
                PlanningInputKind::NicMacNotInBluperint(nic_entry),
            );
        }
        match nic_entry.nic.ip {
            IpAddr::V4(ip) if NEXUS_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_nexus_nic_ips.contains(&ip.into()) {
                    blippy.push_planning_input_note(
                        Severity::Fatal,
                        PlanningInputKind::NicIpNotInBlueprint(nic_entry),
                    );
                }
            }
            IpAddr::V4(ip) if NTP_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_boundary_ntp_nic_ips.contains(&ip.into()) {
                    blippy.push_planning_input_note(
                        Severity::Fatal,
                        PlanningInputKind::NicIpNotInBlueprint(nic_entry),
                    );
                }
            }
            IpAddr::V4(ip) if DNS_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_external_dns_nic_ips.contains(&ip.into()) {
                    blippy.push_planning_input_note(
                        Severity::Fatal,
                        PlanningInputKind::NicIpNotInBlueprint(nic_entry),
                    );
                }
            }
            IpAddr::V6(ip) if NEXUS_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_nexus_nic_ips.contains(&ip.into()) {
                    blippy.push_planning_input_note(
                        Severity::Fatal,
                        PlanningInputKind::NicIpNotInBlueprint(nic_entry),
                    );
                }
            }
            IpAddr::V6(ip) if NTP_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_boundary_ntp_nic_ips.contains(&ip.into()) {
                    blippy.push_planning_input_note(
                        Severity::Fatal,
                        PlanningInputKind::NicIpNotInBlueprint(nic_entry),
                    );
                }
            }
            IpAddr::V6(ip) if DNS_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_external_dns_nic_ips.contains(&ip.into()) {
                    blippy.push_planning_input_note(
                        Severity::Fatal,
                        PlanningInputKind::NicIpNotInBlueprint(nic_entry),
                    );
                }
            }
            _ => {
                // Ignore localhost (used by the test suite).
                if !nic_entry.nic.ip.is_loopback() {
                    blippy.push_planning_input_note(
                        Severity::Fatal,
                        PlanningInputKind::NicWithUnknownOpteSubnet(nic_entry),
                    );
                }
            }
        }
    }
}
