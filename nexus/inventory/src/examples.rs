// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example collections used for testing

use crate::CollectionBuilder;
use gateway_client::types::PowerState;
use gateway_client::types::RotSlot;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::OmicronZonesConfig;
use nexus_types::inventory::RotPage;
use nexus_types::inventory::RotPageWhich;
use omicron_common::api::external::ByteCount;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use std::sync::Arc;
use strum::IntoEnumIterator;

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
                rot: RotState::Enabled {
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
                rot: RotState::Enabled {
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
                rot: RotState::Enabled {
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
                rot: RotState::Enabled {
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

    // Report some sled agents.
    //
    // This first one will match "sled1_bb"'s baseboard information.
    let sled_agent_id_basic =
        "c5aec1df-b897-49e4-8085-ccd975f9b529".parse().unwrap();
    // Add some disks to this first sled.
    let disks = vec![
        // Let's say we have one manufacturer for our M.2...
        sled_agent_client::types::InventoryDisk {
            identity: omicron_common::disk::DiskIdentity {
                vendor: "macrohard".to_string(),
                model: "box".to_string(),
                serial: "XXIV".to_string(),
            },
            variant: sled_agent_client::types::DiskVariant::M2,
            slot: 0,
        },
        // ... and a couple different vendors for our U.2s
        sled_agent_client::types::InventoryDisk {
            identity: omicron_common::disk::DiskIdentity {
                vendor: "memetendo".to_string(),
                model: "swatch".to_string(),
                serial: "0001".to_string(),
            },
            variant: sled_agent_client::types::DiskVariant::U2,
            slot: 1,
        },
        sled_agent_client::types::InventoryDisk {
            identity: omicron_common::disk::DiskIdentity {
                vendor: "memetendo".to_string(),
                model: "swatch".to_string(),
                serial: "0002".to_string(),
            },
            variant: sled_agent_client::types::DiskVariant::U2,
            slot: 2,
        },
        sled_agent_client::types::InventoryDisk {
            identity: omicron_common::disk::DiskIdentity {
                vendor: "tony".to_string(),
                model: "craystation".to_string(),
                serial: "5".to_string(),
            },
            variant: sled_agent_client::types::DiskVariant::U2,
            slot: 3,
        },
    ];
    let zpools = vec![];

    builder
        .found_sled_inventory(
            "fake sled agent 1",
            sled_agent(
                sled_agent_id_basic,
                sled_agent_client::types::Baseboard::Gimlet {
                    identifier: String::from("s1"),
                    model: String::from("model1"),
                    revision: 0,
                },
                sled_agent_client::types::SledRole::Gimlet,
                disks,
                zpools,
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
                sled_agent_client::types::Baseboard::Gimlet {
                    identifier: sled4_bb.serial_number.clone(),
                    model: sled4_bb.part_number.clone(),
                    revision: 0,
                },
                sled_agent_client::types::SledRole::Scrimlet,
                vec![],
                vec![],
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
                sled_agent_client::types::Baseboard::Pc {
                    identifier: String::from("fellofftruck1"),
                    model: String::from("fellofftruck"),
                },
                sled_agent_client::types::SledRole::Gimlet,
                vec![],
                vec![],
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
                sled_agent_client::types::Baseboard::Unknown,
                sled_agent_client::types::SledRole::Gimlet,
                vec![],
                vec![],
            ),
        )
        .unwrap();

    // Report a representative set of Omicron zones.
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

    let sled14_id = "7612d745-d978-41c8-8ee0-84564debe1d2".parse().unwrap();
    builder
        .found_sled_omicron_zones("fake sled 14 agent", sled14_id, sled14)
        .unwrap();
    let sled16_id = "af56cb43-3422-4f76-85bf-3f229db5f39c".parse().unwrap();
    builder
        .found_sled_omicron_zones("fake sled 15 agent", sled16_id, sled16)
        .unwrap();
    let sled17_id = "6eb2a0d9-285d-4e03-afa1-090e4656314b".parse().unwrap();
    builder
        .found_sled_omicron_zones("fake sled 15 agent", sled17_id, sled17)
        .unwrap();

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
        rot: RotState::Enabled {
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
    baseboard: sled_agent_client::types::Baseboard,
    sled_role: sled_agent_client::types::SledRole,
    disks: Vec<sled_agent_client::types::InventoryDisk>,
    zpools: Vec<sled_agent_client::types::InventoryZpool>,
) -> sled_agent_client::types::Inventory {
    sled_agent_client::types::Inventory {
        baseboard,
        reservoir_size: ByteCount::from(1024),
        sled_role,
        sled_agent_address: "[::1]:56792".parse().unwrap(),
        sled_id: sled_id.into_untyped_uuid(),
        usable_hardware_threads: 10,
        usable_physical_ram: ByteCount::from(1024 * 1024),
        disks,
        zpools,
    }
}
