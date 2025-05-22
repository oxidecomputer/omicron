// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example collections used for testing

use crate::CollectionBuilder;
use crate::now_db_precision;
use clickhouse_admin_types::ClickhouseKeeperClusterMembership;
use clickhouse_admin_types::KeeperId;
use gateway_client::types::PowerState;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use gateway_types::rot::RotSlot;
use nexus_sled_agent_shared::inventory::Baseboard;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::Inventory;
use nexus_sled_agent_shared::inventory::InventoryDataset;
use nexus_sled_agent_shared::inventory::InventoryDisk;
use nexus_sled_agent_shared::inventory::InventoryZpool;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::OmicronZonesConfig;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::RotPage;
use nexus_types::inventory::RotPageWhich;
use nexus_types::inventory::ZpoolName;
use omicron_common::api::external::ByteCount;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DatasetKind;
use omicron_common::disk::DatasetName;
use omicron_common::disk::DiskVariant;
use omicron_common::disk::OmicronPhysicalDiskConfig;
use omicron_common::disk::SharedDatasetConfig;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
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

pub fn sled_agent(
    sled_id: SledUuid,
    baseboard: Baseboard,
    sled_role: SledRole,
    disks: Vec<InventoryDisk>,
    zpools: Vec<InventoryZpool>,
    datasets: Vec<InventoryDataset>,
    ledgered_sled_config: Option<OmicronSledConfig>,
) -> Inventory {
    // Assume the `ledgered_sled_config` was reconciled successfully.
    let last_reconciliation = ledgered_sled_config.clone().map(|config| {
        let external_disks = config
            .disks
            .iter()
            .map(|d| (d.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let datasets = config
            .datasets
            .iter()
            .map(|d| (d.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        let zones = config
            .zones
            .iter()
            .map(|z| (z.id, ConfigReconcilerInventoryResult::Ok))
            .collect();
        ConfigReconcilerInventory {
            last_reconciled_config: config,
            external_disks,
            datasets,
            zones,
        }
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
    }
}
