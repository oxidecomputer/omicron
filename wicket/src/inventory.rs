// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Information about all top-level Oxide components (sleds, switches, PSCs)

/// Inventory is the most recent information about rack composition as
/// received from MGS.
#[derive(Debug, Default)]
pub struct Inventory {
    sleds: [Option<FakeSled>; 32],
    switches: [Option<FakeSwitch>; 2],
    psc: Option<FakePsc>,
}

#[derive(Debug)]
pub struct FakeSled {
    // 0-31
    slot: u8,
    serial_number: String,
    part_number: String,
    sp_version: String,
    rot_version: String,
    host_os_version: String,
    control_plane_version: Option<String>,
}

#[derive(Debug)]
pub struct FakeSwitch {
    // Top is 0, bottom is 1
    slot: u8,
    serial_number: String,
    part_number: String,
    sp_version: String,
    rot_version: String,
}

#[derive(Debug)]
pub struct FakePsc {
    // Top is 0 power shelf, 1 is bottom
    slot: u8,
    serial_number: String,
    part_number: String,
    sp_version: String,
    rot_version: String,
}

/// TODO: Use real inventory received from MGS
#[derive(Debug)]
pub enum Component {
    Sled(FakeSled),
    Switch(FakeSwitch),
    Psc(FakePsc),
}

impl Component {
    pub fn name(&self) -> String {
        match self {
            Component::Sled(s) => format!("sled {}", s.slot),
            Component::Switch(s) => format!("switch {}", s.slot),
            Component::Psc(p) => format!("psc {}", p.slot),
        }
    }
}
