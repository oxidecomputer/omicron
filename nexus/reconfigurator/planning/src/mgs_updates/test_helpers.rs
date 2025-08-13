// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test-only support code for testing MGS update planning.

use std::collections::BTreeMap;

use gateway_client::types::PowerState;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use gateway_types::rot::RotSlot;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use tufaceous_artifact::ArtifactVersion;

/// Hash of fake RoT signing keys
const ROT_SIGN_GIMLET: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";
const ROT_SIGN_PSC: &str =
    "2222222222222222222222222222222222222222222222222222222222222222";
const ROT_SIGN_SWITCH: &str =
    "3333333333333333333333333333333333333333333333333333333333333333";

/// Description of a single fake board (sled, switch, or PSC).
#[derive(Debug)]
pub(super) struct TestBoard {
    pub(super) id: SpIdentifier,
    pub(super) serial: &'static str,
    pub(super) sp_board: &'static str,
    pub(super) rot_board: &'static str,
    pub(super) rot_sign: &'static str,
}

impl IdOrdItem for TestBoard {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    iddqd::id_upcast!();
}

/// Collection of [`TestBoard`]s used throughout MGS planning tests.
#[derive(Debug)]
pub(super) struct TestBoards {
    test_name: &'static str,
    boards: IdOrdMap<TestBoard>,
}

impl TestBoards {
    /// Describes the SPs and RoTs in the environment used in these tests
    ///
    /// There will be:
    ///
    /// - 4 sled SPs
    /// - 2 switch SPs
    /// - 2 PSC SPs
    ///
    /// The specific set of hardware (boards) vary and are hardcoded:
    ///
    /// - sled 0: gimlet-d, oxide-rot-1
    /// - other sleds: gimlet-e, oxide-rot-1
    /// - switch 0: sidecar-b, oxide-rot-1
    /// - switch 1: sidecar-c, oxide-rot-1
    /// - psc 0: psc-b, oxide-rot-1
    /// - psc 1: psc-c, oxide-rot-1
    pub fn new(test_name: &'static str) -> Self {
        let mut boards = IdOrdMap::new();
        for (type_, details) in [
            (
                SpType::Sled,
                &[
                    ("sled_0", "gimlet-d", "oxide-rot-1", ROT_SIGN_GIMLET),
                    ("sled_1", "gimlet-e", "oxide-rot-1", ROT_SIGN_GIMLET),
                    ("sled_2", "gimlet-e", "oxide-rot-1", ROT_SIGN_GIMLET),
                    ("sled_3", "gimlet-e", "oxide-rot-1", ROT_SIGN_GIMLET),
                ] as &[_],
            ),
            (
                SpType::Switch,
                &[
                    ("switch_0", "sidecar-b", "oxide-rot-1", ROT_SIGN_SWITCH),
                    ("switch_1", "sidecar-c", "oxide-rot-1", ROT_SIGN_SWITCH),
                ],
            ),
            (
                SpType::Power,
                &[
                    ("power_0", "psc-b", "oxide-rot-1", ROT_SIGN_PSC),
                    ("power_1", "psc-c", "oxide-rot-1", ROT_SIGN_PSC),
                ],
            ),
        ] {
            for (slot, (serial, sp_board, rot_board, rot_sign)) in
                details.into_iter().enumerate()
            {
                let slot = slot as u16;
                boards
                    .insert_unique(TestBoard {
                        id: SpIdentifier { type_, slot },
                        serial,
                        sp_board,
                        rot_board,
                        rot_sign,
                    })
                    .expect("test board IDs are unique");
            }
        }
        Self { boards, test_name }
    }

    pub fn into_iter(self) -> impl Iterator<Item = TestBoard> {
        self.boards.into_iter()
    }

    /// Get a helper to build an inventory collection reflecting specific
    /// versions of our test boards.
    pub fn collection_builder<'a>(&'a self) -> TestBoardCollectionBuilder<'a> {
        TestBoardCollectionBuilder::new(self)
    }
}

/// Test helper that will produce an inventory collection.
///
/// After construction, the caller _must_ call:
///
/// * `sp_active_version()`
/// * `rot_active_version()`
/// * `sp_inactive_version()`
/// * `rot_inactive_version()`
///
/// to set the default active and inactive versions reported for all SPs and
/// RoTs. The caller may also call the various `*_exception` methods to override
/// these defaults for specific boards. Once all properties have been set, call
/// `build()` to produce a collection.
#[derive(Debug, Clone)]
pub(super) struct TestBoardCollectionBuilder<'a> {
    boards: &'a TestBoards,

    // fields that callers _must_ provide before calling `build()`
    sp_active_version: Option<ArtifactVersion>,
    sp_inactive_version: Option<ExpectedVersion>,
    rot_active_version: Option<ArtifactVersion>,
    rot_inactive_version: Option<ExpectedVersion>,

    // fields that callers _may_ influence before calling `build()`
    sp_active_version_exceptions: BTreeMap<SpIdentifier, ArtifactVersion>,
    rot_active_version_exceptions: BTreeMap<SpIdentifier, ArtifactVersion>,
}

impl<'a> TestBoardCollectionBuilder<'a> {
    fn new(boards: &'a TestBoards) -> Self {
        Self {
            boards,
            sp_active_version: None,
            sp_inactive_version: None,
            rot_active_version: None,
            rot_inactive_version: None,
            sp_active_version_exceptions: BTreeMap::new(),
            rot_active_version_exceptions: BTreeMap::new(),
        }
    }

    pub fn sp_active_version(mut self, v: ArtifactVersion) -> Self {
        self.sp_active_version = Some(v);
        self
    }

    pub fn sp_inactive_version(mut self, v: ExpectedVersion) -> Self {
        self.sp_inactive_version = Some(v);
        self
    }

    pub fn sp_active_version_exception(
        mut self,
        type_: SpType,
        slot: u16,
        v: ArtifactVersion,
    ) -> Self {
        self.sp_active_version_exceptions
            .insert(SpIdentifier { type_, slot }, v);
        self
    }

    pub fn has_sp_active_version_exception(
        &self,
        type_: SpType,
        slot: u16,
    ) -> bool {
        self.sp_active_version_exceptions
            .contains_key(&SpIdentifier { type_, slot })
    }

    pub fn rot_active_version(mut self, v: ArtifactVersion) -> Self {
        self.rot_active_version = Some(v);
        self
    }

    pub fn rot_inactive_version(mut self, v: ExpectedVersion) -> Self {
        self.rot_inactive_version = Some(v);
        self
    }

    pub fn rot_active_version_exception(
        mut self,
        type_: SpType,
        slot: u16,
        v: ArtifactVersion,
    ) -> Self {
        self.rot_active_version_exceptions
            .insert(SpIdentifier { type_, slot }, v);
        self
    }

    pub fn has_rot_active_version_exception(
        &self,
        type_: SpType,
        slot: u16,
    ) -> bool {
        self.rot_active_version_exceptions
            .contains_key(&SpIdentifier { type_, slot })
    }


    pub fn build(self) -> Collection {
        let sp_active_version =
            self.sp_active_version.expect("sp_active_version() was provided");
        let sp_inactive_version = self
            .sp_inactive_version
            .expect("sp_inactive_version() was provided");
        let rot_active_version =
            self.rot_active_version.expect("rot_active_version() was provided");
        let rot_inactive_version = self
            .rot_inactive_version
            .expect("rot_inactive_version() was provided");

        let mut builder =
            nexus_inventory::CollectionBuilder::new(self.boards.test_name);

        let dummy_sp_state = SpState {
            base_mac_address: [0; 6],
            hubris_archive_id: String::from("unused"),
            model: String::from("unused"),
            power_state: PowerState::A0,
            revision: 0,
            rot: RotState::V3 {
                active: RotSlot::A,
                pending_persistent_boot_preference: None,
                persistent_boot_preference: RotSlot::A,
                slot_a_error: None,
                slot_a_fwid: Default::default(),
                slot_b_error: None,
                slot_b_fwid: Default::default(),
                stage0_error: None,
                stage0_fwid: Default::default(),
                stage0next_error: None,
                stage0next_fwid: Default::default(),
                transient_boot_preference: None,
            },
            serial_number: String::from("unused"),
        };

        for board in &self.boards.boards {
            let &TestBoard {
                id: sp_id,
                serial,
                sp_board: caboose_sp_board,
                rot_board: caboose_rot_board,
                rot_sign: rkth,
            } = board;

            let sp_state = SpState {
                model: format!("dummy_{}", sp_id.type_),
                serial_number: serial.to_string(),
                ..dummy_sp_state.clone()
            };

            let baseboard_id = builder
                .found_sp_state("test", sp_id.type_, sp_id.slot, sp_state)
                .unwrap();
            let sp_active_version = self
                .sp_active_version_exceptions
                .get(&sp_id)
                .unwrap_or(&sp_active_version);
            let rot_active_version = self
                .rot_active_version_exceptions
                .get(&sp_id)
                .unwrap_or(&rot_active_version);

            builder
                .found_caboose(
                    &baseboard_id,
                    CabooseWhich::SpSlot0,
                    "test",
                    SpComponentCaboose {
                        board: caboose_sp_board.to_string(),
                        epoch: None,
                        git_commit: String::from("unused"),
                        name: caboose_sp_board.to_string(),
                        sign: None,
                        version: sp_active_version.as_str().to_string(),
                    },
                )
                .unwrap();

            builder
                .found_caboose(
                    &baseboard_id,
                    CabooseWhich::RotSlotA,
                    "test",
                    SpComponentCaboose {
                        board: caboose_rot_board.to_string(),
                        epoch: None,
                        git_commit: String::from("unused"),
                        name: caboose_rot_board.to_string(),
                        sign: Some(rkth.to_string()),
                        version: rot_active_version.as_str().to_string(),
                    },
                )
                .unwrap();

            if let ExpectedVersion::Version(sp_inactive_version) =
                &sp_inactive_version
            {
                builder
                    .found_caboose(
                        &baseboard_id,
                        CabooseWhich::SpSlot1,
                        "test",
                        SpComponentCaboose {
                            board: caboose_sp_board.to_string(),
                            epoch: None,
                            git_commit: String::from("unused"),
                            name: caboose_sp_board.to_string(),
                            sign: None,
                            version: sp_inactive_version.as_str().to_string(),
                        },
                    )
                    .unwrap();
            }

            if let ExpectedVersion::Version(rot_inactive_version) =
                &rot_inactive_version
            {
                builder
                    .found_caboose(
                        &baseboard_id,
                        CabooseWhich::RotSlotB,
                        "test",
                        SpComponentCaboose {
                            board: caboose_rot_board.to_string(),
                            epoch: None,
                            git_commit: String::from("unused"),
                            name: caboose_rot_board.to_string(),
                            sign: Some(rkth.to_string()),
                            version: rot_inactive_version.as_str().to_string(),
                        },
                    )
                    .unwrap();
            }
        }

        builder.build()
    }
}
