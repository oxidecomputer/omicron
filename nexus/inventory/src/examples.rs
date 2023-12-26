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
use nexus_types::inventory::RotPage;
use nexus_types::inventory::RotPageWhich;
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

    Representative {
        builder,
        sleds: [sled1_bb, sled2_bb, sled3_bb],
        switch: switch1_bb,
        psc: psc_bb,
    }
}

pub struct Representative {
    pub builder: CollectionBuilder,
    pub sleds: [Arc<BaseboardId>; 3],
    pub switch: Arc<BaseboardId>,
    pub psc: Arc<BaseboardId>,
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
