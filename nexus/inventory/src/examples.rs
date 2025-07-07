// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example collections used for testing

use crate::CollectionBuilder;
use crate::now_db_precision;
use camino::Utf8Path;
use clickhouse_admin_types::ClickhouseKeeperClusterMembership;
use clickhouse_admin_types::KeeperId;
use gateway_client::types::PowerState;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use gateway_types::rot::RotSlot;
use iddqd::id_ord_map;
use nexus_sled_agent_shared::inventory::Baseboard;
use nexus_sled_agent_shared::inventory::BootImageHeader;
use nexus_sled_agent_shared::inventory::BootPartitionDetails;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::Inventory;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::OmicronZonesConfig;
use nexus_sled_agent_shared::inventory::OrphanedDataset;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::RotPage;
use nexus_types::inventory::RotPageWhich;
use nexus_types::inventory::ZpoolName;
use omicron_cockroach_metrics::MetricValue;
use omicron_cockroach_metrics::PrometheusMetrics;
use omicron_common::api::external::ByteCount;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_common::disk::DiskVariant;
use omicron_common::disk::M2Slot;
use omicron_common::disk::OmicronPhysicalDiskConfig;
use omicron_common::disk::SharedDatasetConfig;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use sled_agent_types::zone_images::MupdateOverrideNonBootInfo;
use sled_agent_types::zone_images::MupdateOverrideNonBootMismatch;
use sled_agent_types::zone_images::MupdateOverrideNonBootResult;
use sled_agent_types::zone_images::MupdateOverrideReadError;
use sled_agent_types::zone_images::MupdateOverrideStatus;
use sled_agent_types::zone_images::ResolverStatus;
use sled_agent_types::zone_images::ZoneManifestNonBootInfo;
use sled_agent_types::zone_images::ZoneManifestNonBootMismatch;
use sled_agent_types::zone_images::ZoneManifestNonBootResult;
use sled_agent_types::zone_images::ZoneManifestReadError;
use sled_agent_types::zone_images::ZoneManifestStatus;
use sled_agent_zone_images_examples::BOOT_PATHS;
use sled_agent_zone_images_examples::NON_BOOT_2_PATHS;
use sled_agent_zone_images_examples::NON_BOOT_2_UUID;
use sled_agent_zone_images_examples::NON_BOOT_3_PATHS;
use sled_agent_zone_images_examples::NON_BOOT_3_UUID;
use sled_agent_zone_images_examples::NON_BOOT_PATHS;
use sled_agent_zone_images_examples::NON_BOOT_UUID;
use sled_agent_zone_images_examples::WriteInstallDatasetContext;
use sled_agent_zone_images_examples::dataset_missing_error;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

/// Returns an example Collection used for testing
///
/// This collection is intended to cover a variety of possible inventory data,
/// including:
///
/// - all three baseboard types (switch, sled, PSC)
/// - various valid values for all fields (sources, slot numbers, power
///   states, baseboard revisions, cabooses, etc.)
/// - some empty slots
/// - some missing cabooses
/// - some cabooses common to multiple baseboards; others not
/// - serial number reused across different model numbers
pub fn representative() -> Representative {
    let mut builder = CollectionBuilder::new("example");

    // an ordinary, working sled
    let sled1_bb = builder
        .found_sp_state(
            "fake MGS 1",
            SpType::Sled,
            3,
            SpState {
                base_mac_address: [0; 6],
                hubris_archive_id: String::from("hubris1"),
                model: String::from("model1"),
                power_state: PowerState::A0,
                revision: 0,
                rot: RotState::V2 {
                    active: RotSlot::A,
                    pending_persistent_boot_preference: None,
                    persistent_boot_preference: RotSlot::A,
                    slot_a_sha3_256_digest: Some(String::from("slotAdigest1")),
                    slot_b_sha3_256_digest: Some(String::from("slotBdigest1")),
                    transient_boot_preference: None,
                },
                serial_number: String::from("s1"),
            },
        )
        .unwrap();

    // another ordinary sled with different values for ordinary fields
    let sled2_bb = builder
        .found_sp_state(
            "fake MGS 2",
            SpType::Sled,
            4,
            SpState {
                base_mac_address: [1; 6],
                hubris_archive_id: String::from("hubris2"),
                model: String::from("model2"),
                power_state: PowerState::A2,
                revision: 1,
                rot: RotState::V2 {
                    active: RotSlot::B,
                    pending_persistent_boot_preference: Some(RotSlot::A),
                    persistent_boot_preference: RotSlot::A,
                    slot_a_sha3_256_digest: Some(String::from("slotAdigest2")),
                    slot_b_sha3_256_digest: Some(String::from("slotBdigest2")),
                    transient_boot_preference: Some(RotSlot::B),
                },
                // same serial number, which is okay because it's a
                // different model number
                serial_number: String::from("s1"),
            },
        )
        .unwrap();

    // a switch
    let switch1_bb = builder
        .found_sp_state(
            "fake MGS 2",
            SpType::Switch,
            0,
            SpState {
                base_mac_address: [2; 6],
                hubris_archive_id: String::from("hubris3"),
                model: String::from("model3"),
                power_state: PowerState::A1,
                revision: 2,
                rot: RotState::V2 {
                    active: RotSlot::B,
                    pending_persistent_boot_preference: None,
                    persistent_boot_preference: RotSlot::A,
                    slot_a_sha3_256_digest: Some(String::from("slotAdigest3")),
                    slot_b_sha3_256_digest: Some(String::from("slotBdigest3")),
                    transient_boot_preference: None,
                },
                // same serial number, which is okay because it's a
                // different model number
                serial_number: String::from("s1"),
            },
        )
        .unwrap();

    // a PSC
    let psc_bb = builder
        .found_sp_state(
            "fake MGS 1",
            SpType::Power,
            1,
            SpState {
                base_mac_address: [3; 6],
                hubris_archive_id: String::from("hubris4"),
                model: String::from("model4"),
                power_state: PowerState::A2,
                revision: 3,
                rot: RotState::V2 {
                    active: RotSlot::B,
                    pending_persistent_boot_preference: None,
                    persistent_boot_preference: RotSlot::A,
                    slot_a_sha3_256_digest: Some(String::from("slotAdigest4")),
                    slot_b_sha3_256_digest: Some(String::from("slotBdigest4")),
                    transient_boot_preference: None,
                },
                serial_number: String::from("s2"),
            },
        )
        .unwrap();

    // a sled with no RoT state or other optional fields
    let sled3_bb = builder
        .found_sp_state(
            "fake MGS 1",
            SpType::Sled,
            5,
            SpState {
                base_mac_address: [4; 6],
                hubris_archive_id: String::from("hubris5"),
                model: String::from("model1"),
                power_state: PowerState::A2,
                revision: 1,
                rot: RotState::CommunicationFailed {
                    message: String::from("test suite injected error"),
                },
                serial_number: String::from("s2"),
            },
        )
        .unwrap();

    // Report some cabooses.

    // We'll use the same cabooses for most of these components, although
    // that's not possible in a real system.  We deliberately construct a
    // new value each time to make sure the builder correctly normalizes it.
    let common_caboose_baseboards = [&sled1_bb, &sled2_bb, &switch1_bb];
    for bb in &common_caboose_baseboards {
        for which in CabooseWhich::iter() {
            assert!(!builder.found_caboose_already(bb, which));
            builder
                .found_caboose(bb, which, "test suite", caboose("1"))
                .unwrap();
            assert!(builder.found_caboose_already(bb, which));
        }
    }

    // For the PSC, use different cabooses for both slots of both the SP and
    // RoT, just to exercise that we correctly keep track of different
    // cabooses.
    builder
        .found_caboose(
            &psc_bb,
            CabooseWhich::SpSlot0,
            "test suite",
            caboose("psc_sp_0"),
        )
        .unwrap();
    builder
        .found_caboose(
            &psc_bb,
            CabooseWhich::SpSlot1,
            "test suite",
            caboose("psc_sp_1"),
        )
        .unwrap();
    builder
        .found_caboose(
            &psc_bb,
            CabooseWhich::RotSlotA,
            "test suite",
            caboose("psc_rot_a"),
        )
        .unwrap();
    builder
        .found_caboose(
            &psc_bb,
            CabooseWhich::RotSlotB,
            "test suite",
            caboose("psc_rot_b"),
        )
        .unwrap();

    // We deliberately provide no cabooses for sled3.

    // Report some RoT pages.

    // We'll use the same RoT pages for most of these components, although
    // that's not possible in a real system. We deliberately construct a new
    // value each time to make sure the builder correctly normalizes it.
    let common_rot_page_baseboards = [&sled1_bb, &sled3_bb, &switch1_bb];
    for bb in common_rot_page_baseboards {
        for which in RotPageWhich::iter() {
            assert!(!builder.found_rot_page_already(bb, which));
            builder
                .found_rot_page(bb, which, "test suite", rot_page("1"))
                .unwrap();
            assert!(builder.found_rot_page_already(bb, which));
        }
    }

    // For the PSC, use different RoT page data for each kind of page, just to
    // exercise that we correctly keep track of different data values.
    builder
        .found_rot_page(
            &psc_bb,
            RotPageWhich::Cmpa,
            "test suite",
            rot_page("psc cmpa"),
        )
        .unwrap();
    builder
        .found_rot_page(
            &psc_bb,
            RotPageWhich::CfpaActive,
            "test suite",
            rot_page("psc cfpa active"),
        )
        .unwrap();
    builder
        .found_rot_page(
            &psc_bb,
            RotPageWhich::CfpaInactive,
            "test suite",
            rot_page("psc cfpa inactive"),
        )
        .unwrap();
    builder
        .found_rot_page(
            &psc_bb,
            RotPageWhich::CfpaScratch,
            "test suite",
            rot_page("psc cfpa scratch"),
        )
        .unwrap();

    // We deliberately provide no RoT pages for sled2.

    // Report a representative set of Omicron zones, used in the sled-agent
    // constructors below.
    //
    // We've hand-selected a minimal set of files to cover each type of zone.
    // These files were constructed by:
    //
    // (1) copying the "omicron zones" ledgers from the sleds in a working
    //     Omicron deployment
    // (2) pretty-printing each one with `json --in-place --file FILENAME`
    // (3) adjusting the format slightly with
    //         `jq '{ generation: .omicron_generation, zones: .zones }'`
    let sled14_data = include_str!("../example-data/madrid-sled14.json");
    let sled16_data = include_str!("../example-data/madrid-sled16.json");
    let sled17_data = include_str!("../example-data/madrid-sled17.json");
    let sled14: OmicronZonesConfig = serde_json::from_str(sled14_data).unwrap();
    let sled16: OmicronZonesConfig = serde_json::from_str(sled16_data).unwrap();
    let sled17: OmicronZonesConfig = serde_json::from_str(sled17_data).unwrap();

    // Convert these to `OmicronSledConfig`s. We'll start with empty disks and
    // datasets for now, and add to them below for sled14.
    let mut sled14 = OmicronSledConfig {
        generation: sled14.generation,
        disks: Default::default(),
        datasets: Default::default(),
        zones: sled14.zones.into_iter().collect(),
        remove_mupdate_override: None,
    };
    let sled16 = OmicronSledConfig {
        generation: sled16.generation,
        disks: Default::default(),
        datasets: Default::default(),
        zones: sled16.zones.into_iter().collect(),
        remove_mupdate_override: None,
    };
    let sled17 = OmicronSledConfig {
        generation: sled17.generation,
        disks: Default::default(),
        datasets: Default::default(),
        zones: sled17.zones.into_iter().collect(),
        remove_mupdate_override: None,
    };

    // Create iterator producing fixed IDs.
    let mut disk_id_iter = std::iter::from_fn({
        //              "physicaldisk"
        let mut value = "70687973-6963-616c-6469-736b00000000"
            .parse::<Uuid>()
            .unwrap()
            .as_u128();
        move || {
            value += 1;
            Some(PhysicalDiskUuid::from_u128(value))
        }
    });
    let mut zpool_id_iter = std::iter::from_fn({
        //              "zpool"
        let mut value = "7a706f6f-6c00-0000-0000-000000000000"
            .parse::<Uuid>()
            .unwrap()
            .as_u128();
        move || {
            value += 1;
            Some(ZpoolUuid::from_u128(value))
        }
    });

    // Report some sled agents.
    //
    // This first one will match "sled1_bb"'s baseboard information.
    let sled_agent_id_basic =
        "c5aec1df-b897-49e4-8085-ccd975f9b529".parse().unwrap();

    // Add some disks to this first sled.
    let disks = vec![
        // Let's say we have one manufacturer for our M.2...
        InventoryDisk {
            identity: omicron_common::disk::DiskIdentity {
                vendor: "macrohard".to_string(),
                model: "box".to_string(),
                serial: "XXIV".to_string(),
            },
            variant: DiskVariant::M2,
            slot: 0,
            active_firmware_slot: 1,
            next_active_firmware_slot: None,
            number_of_firmware_slots: 1,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some("EXAMP1".to_string())],
        },
        // ... and a couple different vendors for our U.2s
        InventoryDisk {
            identity: omicron_common::disk::DiskIdentity {
                vendor: "memetendo".to_string(),
                model: "swatch".to_string(),
                serial: "0001".to_string(),
            },
            variant: DiskVariant::U2,
            slot: 1,
            active_firmware_slot: 1,
            next_active_firmware_slot: None,
            number_of_firmware_slots: 1,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some("EXAMP1".to_string())],
        },
        InventoryDisk {
            identity: omicron_common::disk::DiskIdentity {
                vendor: "memetendo".to_string(),
                model: "swatch".to_string(),
                serial: "0002".to_string(),
            },
            variant: DiskVariant::U2,
            slot: 2,
            active_firmware_slot: 1,
            next_active_firmware_slot: None,
            number_of_firmware_slots: 1,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some("EXAMP1".to_string())],
        },
        InventoryDisk {
            identity: omicron_common::disk::DiskIdentity {
                vendor: "tony".to_string(),
                model: "craystation".to_string(),
                serial: "5".to_string(),
            },
            variant: DiskVariant::U2,
            slot: 3,
            active_firmware_slot: 1,
            next_active_firmware_slot: None,
            number_of_firmware_slots: 1,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some("EXAMP1".to_string())],
        },
    ];
    let mut zpools = Vec::new();
    for disk in &disks {
        let pool_id = zpool_id_iter.next().unwrap();
        sled14.disks.insert(OmicronPhysicalDiskConfig {
            identity: disk.identity.clone(),
            id: disk_id_iter.next().unwrap(),
            pool_id,
        });
        zpools.push(InventoryZpool {
            id: pool_id,
            total_size: ByteCount::from(4096),
        });
    }
    let dataset_name = DatasetName::new(
        ZpoolName::new_external(zpools[0].id),
        DatasetKind::Debug,
    );
    let datasets = vec![InventoryDataset {
        id: Some("afc00483-0d7b-4181-87d5-0def937d3cd7".parse().unwrap()),
        name: dataset_name.full_name(),
        available: ByteCount::from(1024),
        used: ByteCount::from(0),
        quota: None,
        reservation: None,
        compression: "lz4".to_string(),
    }];
    sled14.datasets.insert(DatasetConfig {
        id: datasets[0].id.unwrap(),
        name: dataset_name,
        inner: SharedDatasetConfig {
            compression: datasets[0].compression.parse().unwrap(),
            quota: datasets[0].quota,
            reservation: datasets[0].reservation,
        },
    });

    builder
        .found_sled_inventory(
            "fake sled agent 1",
            sled_agent(
                sled_agent_id_basic,
                Baseboard::Gimlet {
                    identifier: String::from("s1"),
                    model: String::from("model1"),
                    revision: 0,
                },
                SledRole::Gimlet,
                disks,
                zpools,
                datasets,
                Some(sled14),
                zone_image_resolver(ZoneImageResolverExampleKind::Success {
                    deserialized_zone_manifest: true,
                    has_mupdate_override: true,
                }),
            ),
        )
        .unwrap();

    // Here, we report a different sled *with* baseboard information that
    // doesn't match one of the baseboards we found.  This is unlikely but could
    // happen.  Make this one a Scrimlet.
    let sled4_bb = Arc::new(BaseboardId {
        part_number: String::from("model1"),
        serial_number: String::from("s4"),
    });
    let sled_agent_id_extra =
        "d7efa9c4-833d-4354-a9a2-94ba9715c154".parse().unwrap();
    builder
        .found_sled_inventory(
            "fake sled agent 4",
            sled_agent(
                sled_agent_id_extra,
                Baseboard::Gimlet {
                    identifier: sled4_bb.serial_number.clone(),
                    model: sled4_bb.part_number.clone(),
                    revision: 0,
                },
                SledRole::Scrimlet,
                vec![],
                vec![],
                vec![],
                Some(sled16),
                zone_image_resolver(ZoneImageResolverExampleKind::Success {
                    deserialized_zone_manifest: false,
                    has_mupdate_override: false,
                }),
            ),
        )
        .unwrap();

    // Now report a different sled as though it were a PC.  It'd be unlikely to
    // see a mix of real Oxide hardware and PCs in the same deployment, but this
    // exercises different code paths.
    let sled_agent_id_pc =
        "c4a5325b-e852-4747-b28a-8aaa7eded8a0".parse().unwrap();
    builder
        .found_sled_inventory(
            "fake sled agent 5",
            sled_agent(
                sled_agent_id_pc,
                Baseboard::Pc {
                    identifier: String::from("fellofftruck1"),
                    model: String::from("fellofftruck"),
                },
                SledRole::Gimlet,
                vec![],
                vec![],
                vec![],
                Some(sled17),
                // Simulate a mismatch in this case with the mupdate override
                // being present. There's one case that's unexplored: mismatch
                // with no mupdate override. But to express that case we would
                // need an additional fifth sled.
                zone_image_resolver(ZoneImageResolverExampleKind::Mismatch {
                    has_mupdate_override: true,
                }),
            ),
        )
        .unwrap();

    // Finally, report a sled with unknown baseboard information.  This should
    // look the same as the PC as far as inventory is concerned but let's verify
    // it.
    let sled_agent_id_unknown =
        "5c5b4cf9-3e13-45fd-871c-f177d6537510".parse().unwrap();

    builder
        .found_sled_inventory(
            "fake sled agent 6",
            sled_agent(
                sled_agent_id_unknown,
                Baseboard::Unknown,
                SledRole::Gimlet,
                vec![],
                vec![],
                vec![],
                // We only have omicron zones for three sleds so report no sled
                // config here.
                None,
                // Simulate an error here.
                zone_image_resolver(ZoneImageResolverExampleKind::Error),
            ),
        )
        .unwrap();

    builder.found_clickhouse_keeper_cluster_membership(
        ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            leader_committed_log_index: 1000,
            raft_config: [KeeperId(1)].into_iter().collect(),
        },
    );

    builder.found_cockroach_metrics(
        omicron_cockroach_metrics::NodeId::new(1),
        PrometheusMetrics {
            metrics: BTreeMap::from([(
                "ranges_underreplicated".to_string(),
                MetricValue::Unsigned(0),
            )]),
        },
    );

    Representative {
        builder,
        sleds: [sled1_bb, sled2_bb, sled3_bb, sled4_bb],
        switch: switch1_bb,
        psc: psc_bb,
        sled_agents: [
            sled_agent_id_basic,
            sled_agent_id_extra,
            sled_agent_id_pc,
            sled_agent_id_unknown,
        ],
    }
}

pub struct Representative {
    pub builder: CollectionBuilder,
    pub sleds: [Arc<BaseboardId>; 4],
    pub switch: Arc<BaseboardId>,
    pub psc: Arc<BaseboardId>,
    pub sled_agents: [SledUuid; 4],
}

impl Representative {
    pub fn new(
        builder: CollectionBuilder,
        sleds: [Arc<BaseboardId>; 4],
        switch: Arc<BaseboardId>,
        psc: Arc<BaseboardId>,
        sled_agents: [SledUuid; 4],
    ) -> Self {
        Self { builder, sleds, switch, psc, sled_agents }
    }
}

/// Returns an SP state that can be used to populate a collection for testing
pub fn sp_state(unique: &str) -> SpState {
    SpState {
        base_mac_address: [0; 6],
        hubris_archive_id: format!("hubris{}", unique),
        model: format!("model{}", unique),
        power_state: PowerState::A2,
        revision: 0,
        rot: RotState::V2 {
            active: RotSlot::A,
            pending_persistent_boot_preference: None,
            persistent_boot_preference: RotSlot::A,
            slot_a_sha3_256_digest: Some(String::from("slotAdigest1")),
            slot_b_sha3_256_digest: Some(String::from("slotBdigest1")),
            transient_boot_preference: None,
        },
        serial_number: format!("serial{}", unique),
    }
}

pub fn caboose(unique: &str) -> SpComponentCaboose {
    SpComponentCaboose {
        board: format!("board_{}", unique),
        git_commit: format!("git_commit_{}", unique),
        name: format!("name_{}", unique),
        version: format!("version_{}", unique),
        sign: Some(format!("sign_{}", unique)),
        epoch: None,
    }
}

pub fn rot_page(unique: &str) -> RotPage {
    use base64::Engine;
    RotPage {
        data_base64: base64::engine::general_purpose::STANDARD.encode(unique),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZoneImageResolverExampleKind {
    /// Success, with or without treating the manifest as deserialized and the
    /// mupdate override being present.
    Success { deserialized_zone_manifest: bool, has_mupdate_override: bool },

    /// The zone manifest is successfully read but doesn't match entries on
    /// disk.
    Mismatch { has_mupdate_override: bool },

    /// Errors while reading the zone manifest and mupdate override status.
    Error,
}

/// Generate an example zone image resolver inventory.
pub fn zone_image_resolver(
    kind: ZoneImageResolverExampleKind,
) -> ZoneImageResolverInventory {
    let dir_path = Utf8Path::new("/some/path");

    // Create a bunch of contexts.
    let mut cx = WriteInstallDatasetContext::new_basic();

    let mut invalid_cx = WriteInstallDatasetContext::new_basic();
    invalid_cx.make_error_cases();

    // Determine the zone manifest and mupdate override results for the boot
    // disk.
    let (boot_zm_result, boot_override_result) = match kind {
        ZoneImageResolverExampleKind::Success {
            deserialized_zone_manifest,
            has_mupdate_override,
        } => {
            if !deserialized_zone_manifest {
                cx.write_zone_manifest_to_disk(false);
            }
            let zm_result = Ok(
                cx.expected_result(&dir_path.join(&BOOT_PATHS.install_dataset))
            );
            let override_result =
                Ok(has_mupdate_override.then(|| cx.override_info()));
            (zm_result, override_result)
        }
        ZoneImageResolverExampleKind::Mismatch { has_mupdate_override } => {
            // In this case, the zone manifest result is generated using the
            // invalid (mismatched) context.
            let zm_result = Ok(invalid_cx
                .expected_result(&dir_path.join(&BOOT_PATHS.install_dataset)));
            let override_result =
                Ok(has_mupdate_override.then(|| cx.override_info()));
            (zm_result, override_result)
        }
        ZoneImageResolverExampleKind::Error => {
            // Use the invalid context to generate an error.
            let zm_result = Err(ZoneManifestReadError::InstallMetadata(
                dataset_missing_error(
                    &dir_path.join(&BOOT_PATHS.install_dataset),
                ),
            ));
            let override_result =
                Err(MupdateOverrideReadError::InstallMetadata(
                    dataset_missing_error(
                        &dir_path.join(&BOOT_PATHS.install_dataset),
                    ),
                ));
            (zm_result, override_result)
        }
    };

    // Generate a status struct first.
    let status = ResolverStatus {
        zone_manifest: ZoneManifestStatus {
            boot_disk_path: dir_path.join(&BOOT_PATHS.zones_json),
            boot_disk_result: boot_zm_result,
            non_boot_disk_metadata: id_ord_map! {
                // Non-boot disk metadata that matches.
                ZoneManifestNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    dataset_dir: dir_path.join(&NON_BOOT_PATHS.install_dataset),
                    path: dir_path.join(&NON_BOOT_PATHS.zones_json),
                    // XXX Technically, if the boot disk had an error, this
                    // can't be Matches. We choose to punt on this issue because
                    // the conversion to the inventory type squishes down
                    // errors into a string.
                    result: ZoneManifestNonBootResult::Matches(
                        cx.expected_result(
                            &dir_path.join(&NON_BOOT_PATHS.install_dataset)
                        )
                    ),
                },
                // Non-boot disk mismatch (zones different + errors).
                ZoneManifestNonBootInfo {
                    zpool_id: NON_BOOT_2_UUID,
                    dataset_dir: dir_path.join(&NON_BOOT_2_PATHS.install_dataset),
                    path: dir_path.join(&NON_BOOT_2_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::Mismatch(
                        ZoneManifestNonBootMismatch::ValueMismatch {
                            non_boot_disk_result: invalid_cx.expected_result(
                                &dir_path.join(&NON_BOOT_2_PATHS.install_dataset),
                            ),
                        },
                    ),
                },
                // Non-boot disk mismatch (error reading zone manifest).
                ZoneManifestNonBootInfo {
                    zpool_id: NON_BOOT_3_UUID,
                    dataset_dir: dir_path.join(&NON_BOOT_3_PATHS.install_dataset),
                    path: dir_path.join(&NON_BOOT_3_PATHS.zones_json),
                    result: ZoneManifestNonBootResult::ReadError(
                        dataset_missing_error(
                            &dir_path.join(&NON_BOOT_3_PATHS.install_dataset),
                        ).into(),
                    ),
                },
            },
        },
        mupdate_override: MupdateOverrideStatus {
            boot_disk_path: dir_path.join(&BOOT_PATHS.mupdate_override_json),
            boot_disk_override: boot_override_result,
            non_boot_disk_overrides: id_ord_map! {
                // Non-boot disk mupdate overrides that match.
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_UUID,
                    path: dir_path.join(&NON_BOOT_PATHS.mupdate_override_json),
                    // XXX Technically, if the boot disk had an error, this
                    // can't be Matches. We choose to punt on this issue because
                    // the conversion to the inventory type squishes down errors
                    // into a string.
                    result: MupdateOverrideNonBootResult::MatchesPresent,
                },
                // Non-boot disk mupdate overrides that have a mismatch.
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_2_UUID,
                    path: dir_path.join(&NON_BOOT_2_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::Mismatch(
                        MupdateOverrideNonBootMismatch::BootPresentOtherAbsent,
                    ),
                },
                // Non-boot disk updates (error reading zone manifest).
                MupdateOverrideNonBootInfo {
                    zpool_id: NON_BOOT_3_UUID,
                    path: dir_path.join(&NON_BOOT_3_PATHS.mupdate_override_json),
                    result: MupdateOverrideNonBootResult::ReadError(
                        dataset_missing_error(
                            &dir_path.join(&NON_BOOT_3_PATHS.install_dataset),
                        ).into(),
                    ),
                },
            },
        },
    };

    status.to_inventory()
}

#[expect(clippy::too_many_arguments)]
pub fn sled_agent(
    sled_id: SledUuid,
    baseboard: Baseboard,
    sled_role: SledRole,
    disks: Vec<InventoryDisk>,
    zpools: Vec<InventoryZpool>,
    datasets: Vec<InventoryDataset>,
    ledgered_sled_config: Option<OmicronSledConfig>,
    zone_image_resolver: ZoneImageResolverInventory,
) -> Inventory {
    // Assume the `ledgered_sled_config` was reconciled successfully.
    let last_reconciliation = ledgered_sled_config.clone().map(|config| {
        let mut inv = ConfigReconcilerInventory::debug_assume_success(config);

        // Add an orphaned dataset with no tie to other pools/datasets.
        inv.orphaned_datasets.insert_overwrite(OrphanedDataset {
            name: DatasetName::new(
                ZpoolName::new_external(ZpoolUuid::new_v4()),
                DatasetKind::ExternalDns,
            ),
            reason: "example orphaned dataset".to_string(),
            id: Some(DatasetUuid::new_v4()),
            mounted: false,
            available: 0.into(),
            used: 0.into(),
        });

        // Fill in some fake boot partition details.
        inv.boot_partitions.boot_disk = Ok(M2Slot::A);
        inv.boot_partitions.slot_a = Ok(BootPartitionDetails {
            header: BootImageHeader {
                flags: 0,
                data_size: 10_000,
                image_size: 10_000,
                target_size: 10_000,
                sha256: [0; 32],
                image_name: "fake image for tests".to_string(),
            },
            artifact_hash: ArtifactHash([1; 32]),
            artifact_size: 10_000 + 4096,
        });

        inv
    });

    let reconciler_status = if last_reconciliation.is_some() {
        ConfigReconcilerInventoryStatus::Idle {
            completed_at: now_db_precision(),
            ran_for: Duration::from_secs(5),
        }
    } else {
        ConfigReconcilerInventoryStatus::NotYetRun
    };

    Inventory {
        baseboard,
        reservoir_size: ByteCount::from(1024),
        sled_role,
        sled_agent_address: "[::1]:56792".parse().unwrap(),
        sled_id,
        usable_hardware_threads: 10,
        usable_physical_ram: ByteCount::from(1024 * 1024),
        disks,
        zpools,
        datasets,
        ledgered_sled_config,
        reconciler_status,
        last_reconciliation,
        zone_image_resolver,
    }
}
