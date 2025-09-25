// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test-only support code for testing MGS update planning.

use std::collections::BTreeMap;

use chrono::Utc;
use gateway_client::types::PowerState;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpState;
use gateway_types::rot::RotSlot;
use id_map::IdMap;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use nexus_sled_agent_shared::inventory::Baseboard;
use nexus_sled_agent_shared::inventory::BootImageHeader;
use nexus_sled_agent_shared::inventory::BootPartitionContents;
use nexus_sled_agent_shared::inventory::BootPartitionDetails;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
use nexus_sled_agent_shared::inventory::Inventory;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::SledCpuFamily;
use nexus_sled_agent_shared::inventory::SledRole;
use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdateHostPhase1Details;
use nexus_types::deployment::PendingMgsUpdateRotBootloaderDetails;
use nexus_types::deployment::PendingMgsUpdateRotDetails;
use nexus_types::deployment::PendingMgsUpdateSpDetails;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use nexus_types::inventory::SpType;
use omicron_common::api::external::Generation;
use omicron_common::api::external::TufArtifactMeta;
use omicron_common::api::external::TufRepoDescription;
use omicron_common::api::external::TufRepoMeta;
use omicron_common::disk::M2Slot;
use omicron_common::update::ArtifactId;
use omicron_uuid_kinds::SledUuid;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactKind;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;

use crate::mgs_updates::PendingHostPhase2Changes;

/// Version that will be used for all artifacts in the TUF repo
pub(super) const ARTIFACT_VERSION_2: ArtifactVersion =
    ArtifactVersion::new_const("2.0.0");
/// Version that will be "deployed" in the SP we want to update
pub(super) const ARTIFACT_VERSION_1: ArtifactVersion =
    ArtifactVersion::new_const("1.0.0");
/// Version that's different from the other two
pub(super) const ARTIFACT_VERSION_1_5: ArtifactVersion =
    ArtifactVersion::new_const("1.5.0");

/// Hash of fake artifact for fake gimlet-e SP
pub(super) const ARTIFACT_HASH_SP_GIMLET_E: ArtifactHash =
    ArtifactHash([1; 32]);
/// Hash of fake artifact for fake gimlet-d SP
pub(super) const ARTIFACT_HASH_SP_GIMLET_D: ArtifactHash =
    ArtifactHash([2; 32]);
/// Hash of fake artifact for fake sidecar-b SP
pub(super) const ARTIFACT_HASH_SP_SIDECAR_B: ArtifactHash =
    ArtifactHash([5; 32]);
/// Hash of fake artifact for fake sidecar-c SP
pub(super) const ARTIFACT_HASH_SP_SIDECAR_C: ArtifactHash =
    ArtifactHash([6; 32]);
/// Hash of fake artifact for fake psc-b SP
pub(super) const ARTIFACT_HASH_SP_PSC_B: ArtifactHash = ArtifactHash([9; 32]);
/// Hash of fake artifact for fake psc-c SP
pub(super) const ARTIFACT_HASH_SP_PSC_C: ArtifactHash = ArtifactHash([10; 32]);
/// Hash of fake artifact for fake gimlet RoT slot A
pub(super) const ARTIFACT_HASH_ROT_GIMLET_A: ArtifactHash =
    ArtifactHash([13; 32]);
/// Hash of fake artifact for fake gimlet RoT slot B
pub(super) const ARTIFACT_HASH_ROT_GIMLET_B: ArtifactHash =
    ArtifactHash([14; 32]);
/// Hash of fake artifact for fake psc RoT slot A
pub(super) const ARTIFACT_HASH_ROT_PSC_A: ArtifactHash = ArtifactHash([17; 32]);
/// Hash of fake artifact for fake psc RoT slot B
pub(super) const ARTIFACT_HASH_ROT_PSC_B: ArtifactHash = ArtifactHash([18; 32]);
/// Hash of fake artifact for fake switch RoT slot A
pub(super) const ARTIFACT_HASH_ROT_SWITCH_A: ArtifactHash =
    ArtifactHash([21; 32]);
/// Hash of fake artifact for fake switch RoT slot B
pub(super) const ARTIFACT_HASH_ROT_SWITCH_B: ArtifactHash =
    ArtifactHash([22; 32]);
/// Hash of fake artifact for fake gimlet RoT bootloader
pub(super) const ARTIFACT_HASH_ROT_BOOTLOADER_GIMLET: ArtifactHash =
    ArtifactHash([24; 32]);
/// Hash of fake artifact for fake psc RoT bootloader
pub(super) const ARTIFACT_HASH_ROT_BOOTLOADER_PSC: ArtifactHash =
    ArtifactHash([25; 32]);
/// Hash of fake artifact for fake switch RoT bootloader
pub(super) const ARTIFACT_HASH_ROT_BOOTLOADER_SWITCH: ArtifactHash =
    ArtifactHash([28; 32]);
/// Hash of fake artifact for host OS phase 1
pub(super) const ARTIFACT_HASH_HOST_PHASE_1: ArtifactHash =
    ArtifactHash([29; 32]);
/// Hash of fake artifact for host OS phase 1 (for a fake version 1.5)
pub(super) const ARTIFACT_HASH_HOST_PHASE_1_V1_5: ArtifactHash =
    ArtifactHash([30; 32]);
/// Hash of fake artifact for host OS phase 2
pub(super) const ARTIFACT_HASH_HOST_PHASE_2: ArtifactHash =
    ArtifactHash([31; 32]);

/// Hash of a fake "version 1" artifact for host OS phase 1
///
/// This can be used to produce an inventory collection for a host slot that
/// needs an update.
pub(super) const ARTIFACT_HASH_HOST_PHASE_1_V1: ArtifactHash =
    ArtifactHash([32; 32]);
/// Hash of a fake "version 1" artifact for host OS phase 1
///
/// This can be used to produce an inventory collection for a host slot that
/// needs an update.
pub(super) const ARTIFACT_HASH_HOST_PHASE_2_V1: ArtifactHash =
    ArtifactHash([33; 32]);

// unused artifact hashes contained in our fake TUF repo
const ARTIFACT_HASH_CONTROL_PLANE: ArtifactHash = ArtifactHash([33; 32]);
const ARTIFACT_HASH_NEXUS: ArtifactHash = ArtifactHash([34; 32]);

/// Hash of fake RoT signing keys
const ROT_SIGN_GIMLET: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";
const ROT_SIGN_PSC: &str =
    "2222222222222222222222222222222222222222222222222222222222222222";
const ROT_SIGN_SWITCH: &str =
    "3333333333333333333333333333333333333333333333333333333333333333";

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub(super) enum MgsUpdateComponent {
    Sp,
    Rot,
    RotBootloader,
    HostOs,
}

impl From<&'_ PendingMgsUpdateDetails> for MgsUpdateComponent {
    fn from(value: &'_ PendingMgsUpdateDetails) -> Self {
        match value {
            PendingMgsUpdateDetails::Rot { .. } => Self::Rot,
            PendingMgsUpdateDetails::RotBootloader { .. } => {
                Self::RotBootloader
            }
            PendingMgsUpdateDetails::Sp { .. } => Self::Sp,
            PendingMgsUpdateDetails::HostPhase1(_) => Self::HostOs,
        }
    }
}

/// Description of a single fake board (sled, switch, or PSC).
#[derive(Debug)]
pub(super) struct TestBoard {
    pub(super) id: SpIdentifier,
    /// `sled_id` is only meaningful for test boards of the `SpType::Sled`
    /// variety, but it's simpler to just provide one for them all. If this
    /// weren't test code, we should have something more correct.
    pub(super) sled_id: SledUuid,
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
    /// Describes the SPs, RoTs, and RoT bootloaders in the environment used in
    /// these tests
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
                        sled_id: SledUuid::new_v4(),
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

    /// Get the sled ID of a particular sled by SP slot number.
    pub fn sled_id(&self, sp_slot: u16) -> Option<SledUuid> {
        self.boards.iter().find_map(|b| {
            (b.id.type_ == SpType::Sled && b.id.slot == sp_slot)
                .then_some(b.sled_id)
        })
    }

    /// Get a helper to build an inventory collection reflecting specific
    /// versions of our test boards.
    ///
    /// By default, the active version for all reported SPs, RoTs, and RoT
    /// bootloaders will be `ARTIFACT_VERSION_2` and the inactive version will
    /// be `ExpectedVersion::NoValidVersion`. All active host phase 1 slots will
    /// be reported as containing `ARTIFACT_HASH_HOST_PHASE_1` and all inactive
    /// host phase 1 slots will be reported as containing
    /// `ARTIFACT_HASH_HOST_PHASE_1_V1`.
    ///
    /// These values are the defaults to produce a collection where all
    /// updateable items do not need updates; these match the versions and
    /// hashes produced by our `tuf_repo()` method. They can be overridden by
    /// methods on the returned builder before the collection is created to
    /// induce particular kinds of needed updates.
    pub fn collection_builder<'a>(&'a self) -> TestBoardCollectionBuilder<'a> {
        TestBoardCollectionBuilder::new(
            self,
            ARTIFACT_VERSION_2,
            ExpectedVersion::NoValidVersion,
            ARTIFACT_HASH_HOST_PHASE_1,
            ARTIFACT_HASH_HOST_PHASE_1_V1,
            ARTIFACT_HASH_HOST_PHASE_2,
            ARTIFACT_HASH_HOST_PHASE_2_V1,
        )
    }

    /// Returns a TufRepoDescription that we can use to exercise the planning
    /// code.
    pub fn tuf_repo(&self) -> TufRepoDescription {
        const SYSTEM_VERSION: semver::Version = semver::Version::new(0, 0, 1);
        const SYSTEM_HASH: ArtifactHash = ArtifactHash([3; 32]);

        fn make_artifact(
            name: &str,
            kind: ArtifactKind,
            hash: ArtifactHash,
            board: Option<&str>,
            sign: Option<Vec<u8>>,
        ) -> TufArtifactMeta {
            TufArtifactMeta {
                id: ArtifactId {
                    name: name.to_string(),
                    version: ARTIFACT_VERSION_2,
                    kind,
                },
                hash,
                size: 0, // unused here
                board: board.map(|s| s.to_string()),
                sign,
            }
        }

        // Include a bunch of SP-related artifacts, as well as a few others just
        // to make sure those are properly ignored.
        let artifacts = vec![
            make_artifact(
                "control-plane",
                KnownArtifactKind::ControlPlane.into(),
                ARTIFACT_HASH_CONTROL_PLANE,
                None,
                None,
            ),
            make_artifact(
                "nexus",
                KnownArtifactKind::Zone.into(),
                ARTIFACT_HASH_NEXUS,
                None,
                None,
            ),
            make_artifact(
                "gimlet-host-os-phase-1",
                ArtifactKind::GIMLET_HOST_PHASE_1,
                ARTIFACT_HASH_HOST_PHASE_1,
                None,
                None,
            ),
            make_artifact(
                "cosmo-host-os-phase-1",
                ArtifactKind::COSMO_HOST_PHASE_1,
                ARTIFACT_HASH_HOST_PHASE_1,
                None,
                None,
            ),
            make_artifact(
                "host-os-phase-2",
                ArtifactKind::HOST_PHASE_2,
                ARTIFACT_HASH_HOST_PHASE_2,
                None,
                None,
            ),
            make_artifact(
                "gimlet-d",
                KnownArtifactKind::GimletSp.into(),
                test_artifact_for_board("gimlet-d"),
                Some("gimlet-d"),
                None,
            ),
            make_artifact(
                "gimlet-e",
                KnownArtifactKind::GimletSp.into(),
                test_artifact_for_board("gimlet-e"),
                Some("gimlet-e"),
                None,
            ),
            make_artifact(
                "sidecar-b",
                KnownArtifactKind::SwitchSp.into(),
                test_artifact_for_board("sidecar-b"),
                Some("sidecar-b"),
                None,
            ),
            make_artifact(
                "sidecar-c",
                KnownArtifactKind::SwitchSp.into(),
                test_artifact_for_board("sidecar-c"),
                Some("sidecar-c"),
                None,
            ),
            make_artifact(
                "psc-b",
                KnownArtifactKind::PscSp.into(),
                test_artifact_for_board("psc-b"),
                Some("psc-b"),
                None,
            ),
            make_artifact(
                "psc-c",
                KnownArtifactKind::PscSp.into(),
                test_artifact_for_board("psc-c"),
                Some("psc-c"),
                None,
            ),
            make_artifact(
                "oxide-rot-1-fake-key",
                ArtifactKind::GIMLET_ROT_IMAGE_A,
                test_artifact_for_artifact_kind(
                    ArtifactKind::GIMLET_ROT_IMAGE_A,
                ),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_GIMLET.into()),
            ),
            make_artifact(
                "oxide-rot-1-fake-key",
                ArtifactKind::GIMLET_ROT_IMAGE_B,
                test_artifact_for_artifact_kind(
                    ArtifactKind::GIMLET_ROT_IMAGE_B,
                ),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_GIMLET.into()),
            ),
            make_artifact(
                "oxide-rot-1-fake-key",
                ArtifactKind::PSC_ROT_IMAGE_A,
                test_artifact_for_artifact_kind(ArtifactKind::PSC_ROT_IMAGE_A),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_PSC.into()),
            ),
            make_artifact(
                "oxide-rot-1-fake-key",
                ArtifactKind::PSC_ROT_IMAGE_B,
                test_artifact_for_artifact_kind(ArtifactKind::PSC_ROT_IMAGE_B),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_PSC.into()),
            ),
            make_artifact(
                "oxide-rot-1-fake-key",
                ArtifactKind::SWITCH_ROT_IMAGE_A,
                test_artifact_for_artifact_kind(
                    ArtifactKind::SWITCH_ROT_IMAGE_A,
                ),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_SWITCH.into()),
            ),
            make_artifact(
                "oxide-rot-1-fake-key",
                ArtifactKind::SWITCH_ROT_IMAGE_B,
                test_artifact_for_artifact_kind(
                    ArtifactKind::SWITCH_ROT_IMAGE_B,
                ),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_SWITCH.into()),
            ),
            make_artifact(
                "bootloader-fake-key",
                ArtifactKind::GIMLET_ROT_STAGE0,
                test_artifact_for_artifact_kind(
                    ArtifactKind::GIMLET_ROT_STAGE0,
                ),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_GIMLET.into()),
            ),
            make_artifact(
                "bootloader-fake-key",
                ArtifactKind::PSC_ROT_STAGE0,
                test_artifact_for_artifact_kind(ArtifactKind::PSC_ROT_STAGE0),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_PSC.into()),
            ),
            make_artifact(
                "bootloader-fake-key",
                ArtifactKind::SWITCH_ROT_STAGE0,
                test_artifact_for_artifact_kind(
                    ArtifactKind::SWITCH_ROT_STAGE0,
                ),
                Some("oxide-rot-1"),
                Some(ROT_SIGN_SWITCH.into()),
            ),
        ];

        TufRepoDescription {
            repo: TufRepoMeta {
                hash: SYSTEM_HASH,
                targets_role_version: 0,
                valid_until: Utc::now(),
                system_version: SYSTEM_VERSION,
                file_name: String::new(),
            },
            artifacts,
        }
    }

    /// Collect the set of all expected updates for all supported components of
    /// these test boards.
    pub fn expected_updates(&self) -> ExpectedUpdates {
        let mut updates = IdOrdMap::new();
        let mut phase2 = IdOrdMap::new();

        for board in &self.boards {
            updates
                .insert_unique(ExpectedUpdate {
                    sp_type: board.id.type_,
                    sp_slot: board.id.slot,
                    component: MgsUpdateComponent::Sp,
                    expected_serial: board.serial,
                    expected_artifact: test_artifact_for_board(board.sp_board),
                })
                .expect("boards are unique");
            updates
                .insert_unique(ExpectedUpdate {
                    sp_type: board.id.type_,
                    sp_slot: board.id.slot,
                    component: MgsUpdateComponent::Rot,
                    expected_serial: board.serial,
                    expected_artifact: test_artifact_for_artifact_kind(
                        match board.id.type_ {
                            SpType::Sled => ArtifactKind::GIMLET_ROT_IMAGE_B,
                            SpType::Power => ArtifactKind::PSC_ROT_IMAGE_B,
                            SpType::Switch => ArtifactKind::SWITCH_ROT_IMAGE_B,
                        },
                    ),
                })
                .expect("boards are unique");
            updates
                .insert_unique(ExpectedUpdate {
                    sp_type: board.id.type_,
                    sp_slot: board.id.slot,
                    component: MgsUpdateComponent::RotBootloader,
                    expected_serial: board.serial,
                    expected_artifact: test_artifact_for_artifact_kind(
                        match board.id.type_ {
                            SpType::Sled => ArtifactKind::GIMLET_ROT_STAGE0,
                            SpType::Power => ArtifactKind::PSC_ROT_STAGE0,
                            SpType::Switch => ArtifactKind::SWITCH_ROT_STAGE0,
                        },
                    ),
                })
                .expect("boards are unique");

            if board.id.type_ == SpType::Sled {
                updates
                    .insert_unique(ExpectedUpdate {
                        sp_type: board.id.type_,
                        sp_slot: board.id.slot,
                        component: MgsUpdateComponent::HostOs,
                        expected_serial: board.serial,
                        expected_artifact: ARTIFACT_HASH_HOST_PHASE_1,
                    })
                    .expect("boards are unique");
                phase2
                    .insert_unique(ExpectedHostPhase2Change {
                        sp_slot: board.id.slot,
                        sled_id: board.sled_id,
                        slot: M2Slot::B,
                        contents:
                            BlueprintHostPhase2DesiredContents::Artifact {
                                version: BlueprintArtifactVersion::Available {
                                    version: ARTIFACT_VERSION_2,
                                },
                                hash: ARTIFACT_HASH_HOST_PHASE_2,
                            },
                    })
                    .expect("boards are unique");
            }
        }

        ExpectedUpdates { updates, phase2 }
    }
}

#[derive(Debug)]
struct ExpectedUpdate {
    sp_type: SpType,
    sp_slot: u16,
    component: MgsUpdateComponent,
    expected_serial: &'static str,
    expected_artifact: ArtifactHash,
}

impl IdOrdItem for ExpectedUpdate {
    type Key<'a> = (SpType, u16, MgsUpdateComponent);

    fn key(&self) -> Self::Key<'_> {
        (self.sp_type, self.sp_slot, self.component)
    }

    iddqd::id_upcast!();
}

#[derive(Debug)]
struct ExpectedHostPhase2Change {
    sp_slot: u16,
    sled_id: SledUuid,
    slot: M2Slot,
    contents: BlueprintHostPhase2DesiredContents,
}

impl IdOrdItem for ExpectedHostPhase2Change {
    type Key<'a> = u16;

    fn key(&self) -> Self::Key<'_> {
        self.sp_slot
    }

    iddqd::id_upcast!();
}

/// Test helper containing all the expected updates from a `TestBoards`.
pub(super) struct ExpectedUpdates {
    updates: IdOrdMap<ExpectedUpdate>,
    phase2: IdOrdMap<ExpectedHostPhase2Change>,
}

impl ExpectedUpdates {
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    pub fn len(&self) -> usize {
        self.updates.len()
    }

    /// Confirm that `update` matches one of our expected updates, and _remove_
    /// that update.
    ///
    /// Callers can confirm that all updates have been verified by calling this
    /// method for each expected update and then checking `self.is_empty()`.
    ///
    /// If `update` describes a host phase 1 update, we'll also confirm that
    /// `pending_host_phase_2_changes` contains the expected corresponding
    /// change, and _remove_ that change from `pending_host_phase_2_changes`.
    /// This allows the calling tests to assert that
    /// `pending_host_phase_2_changes` is empty once all updates have been
    /// verified.
    pub fn verify_one(
        &mut self,
        update: &PendingMgsUpdate,
        pending_host_phase_2_changes: &mut PendingHostPhase2Changes,
    ) {
        let sp_type = update.sp_type;
        let sp_slot = update.slot_id;
        let component = MgsUpdateComponent::from(&update.details);
        println!("found update: {} slot {}", sp_type, sp_slot);
        let ExpectedUpdate { expected_serial, expected_artifact, .. } = self
            .updates
            .remove(&(sp_type, sp_slot, component))
            .expect("unexpected update");
        assert_eq!(update.artifact_hash, expected_artifact);
        assert_eq!(update.artifact_version, ARTIFACT_VERSION_2);
        assert_eq!(update.baseboard_id.serial_number, *expected_serial);
        let (expected_active_version, expected_inactive_version) = match &update
            .details
        {
            PendingMgsUpdateDetails::Rot(PendingMgsUpdateRotDetails {
                expected_active_slot,
                expected_inactive_version,
                ..
            }) => (&expected_active_slot.version, expected_inactive_version),
            PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
                expected_active_version,
                expected_inactive_version,
            }) => (expected_active_version, expected_inactive_version),
            PendingMgsUpdateDetails::RotBootloader(
                PendingMgsUpdateRotBootloaderDetails {
                    expected_stage0_version,
                    expected_stage0_next_version,
                },
            ) => (expected_stage0_version, expected_stage0_next_version),
            PendingMgsUpdateDetails::HostPhase1(
                PendingMgsUpdateHostPhase1Details {
                    expected_active_phase_1_slot,
                    expected_boot_disk,
                    expected_active_phase_1_hash,
                    expected_active_phase_2_hash,
                    expected_inactive_phase_1_hash,
                    expected_inactive_phase_2_hash,
                    sled_agent_address: _,
                },
            ) => {
                // Host OS updates aren't in terms of versions, so we can't
                // return the expected versions in this match arm. Just do
                // our own checks then return directly.
                assert_eq!(*expected_active_phase_1_slot, M2Slot::A);
                assert_eq!(*expected_boot_disk, M2Slot::A);
                assert_eq!(
                    *expected_active_phase_1_hash,
                    ARTIFACT_HASH_HOST_PHASE_1_V1
                );
                assert_eq!(
                    *expected_inactive_phase_1_hash,
                    ARTIFACT_HASH_HOST_PHASE_1_V1
                );
                assert_eq!(
                    *expected_active_phase_2_hash,
                    ARTIFACT_HASH_HOST_PHASE_2_V1
                );
                // The inactive phase 2 hash should match the _new_ artifact
                // in the TUF repo; our planner sets this precondition and
                // execution waits for sled-agent to fulfill it.
                assert_eq!(
                    *expected_inactive_phase_2_hash,
                    ARTIFACT_HASH_HOST_PHASE_2
                );

                // We should also have a corresponding phase 2 change for this
                // phase 1 update.
                let expected_phase2 = self
                    .phase2
                    .remove(&sp_slot)
                    .expect("missing phase2 update");
                let (actual_phase2_slot, actual_phase2_contents) =
                    pending_host_phase_2_changes
                        .remove(&expected_phase2.sled_id)
                        .expect("missing expected pending phase 2 change");
                assert_eq!(expected_phase2.slot, actual_phase2_slot);
                assert_eq!(expected_phase2.contents, actual_phase2_contents);
                return;
            }
        };
        assert_eq!(*expected_active_version, ARTIFACT_VERSION_1);
        assert_eq!(*expected_inactive_version, ExpectedVersion::NoValidVersion);
    }
}

/// Test helper that will produce an inventory collection.
///
/// After construction, the caller _must_ call:
///
/// * `sp_versions()`
/// * `rot_versions()`
///
/// to set the default active and inactive versions reported for all SPs and
/// RoTs. The caller may also call the various `*_exception` methods to override
/// these defaults for specific boards. Once all properties have been set, call
/// `build()` to produce a collection.
#[derive(Debug, Clone)]
pub(super) struct TestBoardCollectionBuilder<'a> {
    boards: &'a TestBoards,

    // default versions
    sp_active_version: ArtifactVersion,
    sp_inactive_version: ExpectedVersion,
    rot_active_version: ArtifactVersion,
    rot_inactive_version: ExpectedVersion,
    stage0_version: ArtifactVersion,
    stage0_next_version: ExpectedVersion,

    // default artifacts (host OS updates don't work in terms of versions)
    host_phase_1_active_artifact: ArtifactHash,
    host_phase_1_inactive_artifact: ArtifactHash,
    host_phase_2_active_artifact: ArtifactHash,
    host_phase_2_inactive_artifact: ArtifactHash,

    // fields that callers _may_ influence before calling `build()`
    sp_active_version_exceptions: BTreeMap<SpIdentifier, ArtifactVersion>,
    rot_active_version_exceptions: BTreeMap<SpIdentifier, ArtifactVersion>,
    stage0_version_exceptions: BTreeMap<SpIdentifier, ArtifactVersion>,
    rot_active_slot_exceptions: BTreeMap<SpIdentifier, RotSlot>,
    rot_persistent_boot_preference_exceptions: BTreeMap<SpIdentifier, RotSlot>,

    // host exceptions are keyed only by slot; they only apply to sleds.
    host_exceptions: BTreeMap<u16, HostOsException>,
}

impl<'a> TestBoardCollectionBuilder<'a> {
    fn new(
        boards: &'a TestBoards,
        default_active_version: ArtifactVersion,
        default_inactive_version: ExpectedVersion,
        default_active_host_phase_1: ArtifactHash,
        default_inactive_host_phase_1: ArtifactHash,
        default_active_host_phase_2: ArtifactHash,
        default_inactive_host_phase_2: ArtifactHash,
    ) -> Self {
        Self {
            boards,
            sp_active_version: default_active_version.clone(),
            sp_inactive_version: default_inactive_version.clone(),
            rot_active_version: default_active_version.clone(),
            rot_inactive_version: default_inactive_version.clone(),
            stage0_version: default_active_version,
            stage0_next_version: default_inactive_version,
            host_phase_1_active_artifact: default_active_host_phase_1,
            host_phase_1_inactive_artifact: default_inactive_host_phase_1,
            host_phase_2_active_artifact: default_active_host_phase_2,
            host_phase_2_inactive_artifact: default_inactive_host_phase_2,
            sp_active_version_exceptions: BTreeMap::new(),
            rot_active_version_exceptions: BTreeMap::new(),
            stage0_version_exceptions: BTreeMap::new(),
            rot_active_slot_exceptions: BTreeMap::new(),
            rot_persistent_boot_preference_exceptions: BTreeMap::new(),
            host_exceptions: BTreeMap::new(),
        }
    }

    pub fn sp_versions(
        mut self,
        active: ArtifactVersion,
        inactive: ExpectedVersion,
    ) -> Self {
        self.sp_active_version = active;
        self.sp_inactive_version = inactive;
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

    pub fn rot_versions(
        mut self,
        active: ArtifactVersion,
        inactive: ExpectedVersion,
    ) -> Self {
        self.rot_active_version = active;
        self.rot_inactive_version = inactive;
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

    pub fn rot_active_slot_exception(
        mut self,
        type_: SpType,
        sp_slot: u16,
        s: RotSlot,
    ) -> Self {
        self.rot_active_slot_exceptions
            .insert(SpIdentifier { type_, slot: sp_slot }, s);
        self
    }

    pub fn rot_persistent_boot_preference_exception(
        mut self,
        type_: SpType,
        sp_slot: u16,
        s: RotSlot,
    ) -> Self {
        self.rot_persistent_boot_preference_exceptions
            .insert(SpIdentifier { type_, slot: sp_slot }, s);
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

    pub fn stage0_versions(
        mut self,
        stage0: ArtifactVersion,
        stage0_next: ExpectedVersion,
    ) -> Self {
        self.stage0_version = stage0;
        self.stage0_next_version = stage0_next;
        self
    }

    pub fn stage0_version_exception(
        mut self,
        type_: SpType,
        slot: u16,
        v: ArtifactVersion,
    ) -> Self {
        self.stage0_version_exceptions.insert(SpIdentifier { type_, slot }, v);
        self
    }

    pub fn has_stage0_version_exception(
        &self,
        type_: SpType,
        slot: u16,
    ) -> bool {
        self.stage0_version_exceptions
            .contains_key(&SpIdentifier { type_, slot })
    }

    pub fn host_phase_1_artifacts(
        mut self,
        active: ArtifactHash,
        inactive: ArtifactHash,
    ) -> Self {
        self.host_phase_1_active_artifact = active;
        self.host_phase_1_inactive_artifact = inactive;
        self
    }

    pub fn host_phase_2_artifacts(
        mut self,
        active: ArtifactHash,
        inactive: ArtifactHash,
    ) -> Self {
        self.host_phase_2_active_artifact = active;
        self.host_phase_2_inactive_artifact = inactive;
        self
    }

    pub fn host_active_exception(
        mut self,
        sp_slot: u16,
        phase_1: ArtifactHash,
        phase_2: ArtifactHash,
    ) -> Self {
        self.host_exceptions
            .insert(sp_slot, HostOsException { phase_1, phase_2 });
        self
    }

    pub fn has_host_active_exception(&self, slot: u16) -> bool {
        self.host_exceptions.contains_key(&slot)
    }

    pub fn build(self) -> Collection {
        let mut builder =
            nexus_inventory::CollectionBuilder::new(self.boards.test_name);

        for board in &self.boards.boards {
            let &TestBoard {
                id: sp_id,
                sled_id,
                serial,
                sp_board: caboose_sp_board,
                rot_board: caboose_rot_board,
                rot_sign: rkth,
            } = board;

            let rot_active_slot = self
                .rot_active_slot_exceptions
                .get(&sp_id)
                .cloned()
                .unwrap_or(RotSlot::A);
            let rot_persistent_boot_preference = self
                .rot_persistent_boot_preference_exceptions
                .get(&sp_id)
                .cloned()
                .unwrap_or(RotSlot::A);

            let dummy_sp_state = SpState {
                base_mac_address: [0; 6],
                hubris_archive_id: String::from("unused"),
                model: String::from("unused"),
                power_state: PowerState::A0,
                revision: 0,
                rot: RotState::V3 {
                    active: rot_active_slot,
                    pending_persistent_boot_preference: None,
                    persistent_boot_preference: rot_persistent_boot_preference,
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

            let sp_state = SpState {
                model: format!("dummy_{}", sp_id.type_),
                serial_number: serial.to_string(),
                ..dummy_sp_state.clone()
            };

            let baseboard_id = builder
                .found_sp_state(
                    "test",
                    sp_id.type_,
                    sp_id.slot,
                    sp_state.clone(),
                )
                .unwrap();
            let sp_active_version = self
                .sp_active_version_exceptions
                .get(&sp_id)
                .unwrap_or(&self.sp_active_version);
            let rot_active_version = self
                .rot_active_version_exceptions
                .get(&sp_id)
                .unwrap_or(&self.rot_active_version);
            let stage0_version = self
                .stage0_version_exceptions
                .get(&sp_id)
                .unwrap_or(&self.stage0_version);

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
                    CabooseWhich::from_rot_slot(rot_active_slot),
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

            builder
                .found_caboose(
                    &baseboard_id,
                    CabooseWhich::Stage0,
                    "test",
                    SpComponentCaboose {
                        board: caboose_rot_board.to_string(),
                        epoch: None,
                        git_commit: String::from("unused"),
                        name: caboose_rot_board.to_string(),
                        sign: Some(rkth.to_string()),
                        version: stage0_version.as_str().to_string(),
                    },
                )
                .unwrap();

            if let ExpectedVersion::Version(sp_inactive_version) =
                &self.sp_inactive_version
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
                &self.rot_inactive_version
            {
                builder
                    .found_caboose(
                        &baseboard_id,
                        CabooseWhich::from_rot_slot(rot_active_slot.toggled()),
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

            if let ExpectedVersion::Version(stage0_next_version) =
                &self.stage0_next_version
            {
                builder
                    .found_caboose(
                        &baseboard_id,
                        CabooseWhich::Stage0Next,
                        "test",
                        SpComponentCaboose {
                            board: caboose_rot_board.to_string(),
                            epoch: None,
                            git_commit: String::from("unused"),
                            name: caboose_rot_board.to_string(),
                            sign: Some(rkth.to_string()),
                            version: stage0_next_version.as_str().to_string(),
                        },
                    )
                    .unwrap();
            }

            if board.id.type_ == SpType::Sled {
                let phase_1_active_artifact = self
                    .host_exceptions
                    .get(&board.id.slot)
                    .map(|ex| ex.phase_1)
                    .unwrap_or(self.host_phase_1_active_artifact);
                let phase_2_active_artifact = self
                    .host_exceptions
                    .get(&board.id.slot)
                    .map(|ex| ex.phase_2)
                    .unwrap_or(self.host_phase_2_active_artifact);
                builder
                    .found_host_phase_1_active_slot(
                        &baseboard_id,
                        "test",
                        M2Slot::A,
                    )
                    .unwrap();
                builder
                    .found_host_phase_1_flash_hash(
                        &baseboard_id,
                        M2Slot::A,
                        "test",
                        phase_1_active_artifact,
                    )
                    .unwrap();
                builder
                    .found_host_phase_1_flash_hash(
                        &baseboard_id,
                        M2Slot::B,
                        "test",
                        self.host_phase_1_inactive_artifact,
                    )
                    .unwrap();
                let fake_sled_config = OmicronSledConfig {
                    generation: Generation::new(),
                    disks: IdMap::new(),
                    datasets: IdMap::new(),
                    zones: IdMap::new(),
                    remove_mupdate_override: None,
                    host_phase_2: HostPhase2DesiredSlots::current_contents(),
                };

                // The only sled-agent fields that matter for the purposes of
                // update testing are:
                //
                // * `sled_id` (used to validate expected phase 2 changes)
                // * `baseboard` (must match this fake SP's)
                // * `last_reconciliation` (must contain a valid boot disk and
                //   active slot phase 2 hash)
                let fake_phase_2_header = BootImageHeader {
                    flags: 0,
                    data_size: 0,
                    image_size: 0,
                    target_size: 0,
                    sha256: [0; 32],
                    image_name: "fake image for planner tests".to_string(),
                };
                let boot_partitions = BootPartitionContents {
                    boot_disk: Ok(M2Slot::A),
                    slot_a: Ok(BootPartitionDetails {
                        header: fake_phase_2_header.clone(),
                        artifact_hash: phase_2_active_artifact,
                        artifact_size: 0,
                    }),
                    slot_b: Ok(BootPartitionDetails {
                        header: fake_phase_2_header.clone(),
                        artifact_hash: self.host_phase_2_inactive_artifact,
                        artifact_size: 0,
                    }),
                };
                builder
                    .found_sled_inventory(
                        "test",
                        Inventory {
                            // fields we care about
                            sled_id,
                            baseboard: Baseboard::Gimlet {
                                identifier: sp_state.serial_number.clone(),
                                model: sp_state.model.clone(),
                                revision: 0,
                            },
                            last_reconciliation: Some(
                                ConfigReconcilerInventory {
                                    last_reconciled_config: fake_sled_config
                                        .clone(),
                                    external_disks: BTreeMap::new(),
                                    datasets: BTreeMap::new(),
                                    orphaned_datasets: IdOrdMap::new(),
                                    zones: BTreeMap::new(),
                                    boot_partitions,
                                    remove_mupdate_override: None,
                                },
                            ),
                            // fields we never inspect; filled in with dummy data
                            sled_agent_address: "[::1]:0".parse().unwrap(),
                            sled_role: SledRole::Gimlet,
                            usable_hardware_threads: 0,
                            usable_physical_ram: 0.into(),
                            cpu_family: SledCpuFamily::Unknown,
                            reservoir_size: 0.into(),
                            disks: vec![],
                            zpools: vec![],
                            datasets: vec![],
                            ledgered_sled_config: Some(fake_sled_config),
                            reconciler_status:
                                ConfigReconcilerInventoryStatus::NotYetRun,
                            zone_image_resolver:
                                ZoneImageResolverInventory::new_fake(),
                        },
                    )
                    .unwrap();
            }
        }

        builder.build()
    }
}

#[derive(Debug, Clone, Copy)]
struct HostOsException {
    phase_1: ArtifactHash,
    phase_2: ArtifactHash,
}

fn test_artifact_for_board(board: &str) -> ArtifactHash {
    match board {
        "gimlet-d" => ARTIFACT_HASH_SP_GIMLET_D,
        "gimlet-e" => ARTIFACT_HASH_SP_GIMLET_E,
        "sidecar-b" => ARTIFACT_HASH_SP_SIDECAR_B,
        "sidecar-c" => ARTIFACT_HASH_SP_SIDECAR_C,
        "psc-b" => ARTIFACT_HASH_SP_PSC_B,
        "psc-c" => ARTIFACT_HASH_SP_PSC_C,
        _ => panic!("test bug: no artifact for board {board:?}"),
    }
}

fn test_artifact_for_artifact_kind(kind: ArtifactKind) -> ArtifactHash {
    if kind == ArtifactKind::GIMLET_ROT_IMAGE_A {
        ARTIFACT_HASH_ROT_GIMLET_A
    } else if kind == ArtifactKind::GIMLET_ROT_IMAGE_B {
        ARTIFACT_HASH_ROT_GIMLET_B
    } else if kind == ArtifactKind::PSC_ROT_IMAGE_A {
        ARTIFACT_HASH_ROT_PSC_A
    } else if kind == ArtifactKind::PSC_ROT_IMAGE_B {
        ARTIFACT_HASH_ROT_PSC_B
    } else if kind == ArtifactKind::SWITCH_ROT_IMAGE_A {
        ARTIFACT_HASH_ROT_SWITCH_A
    } else if kind == ArtifactKind::SWITCH_ROT_IMAGE_B {
        ARTIFACT_HASH_ROT_SWITCH_B
    } else if kind == ArtifactKind::GIMLET_ROT_STAGE0 {
        ARTIFACT_HASH_ROT_BOOTLOADER_GIMLET
    } else if kind == ArtifactKind::PSC_ROT_STAGE0 {
        ARTIFACT_HASH_ROT_BOOTLOADER_PSC
    } else if kind == ArtifactKind::SWITCH_ROT_STAGE0 {
        ARTIFACT_HASH_ROT_BOOTLOADER_SWITCH
    } else {
        panic!("test bug: no artifact for artifact kind {kind:?}")
    }
}
