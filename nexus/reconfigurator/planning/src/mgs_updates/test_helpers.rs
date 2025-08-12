// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test-only support code for testing MGS update planning.

use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;

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
    pub fn new() -> Self {
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
        Self { boards }
    }

    pub fn into_iter(self) -> impl Iterator<Item = TestBoard> {
        self.boards.into_iter()
    }
}
