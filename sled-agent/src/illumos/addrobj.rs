// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for operating on addrobj objects.

/// Describes an "addrobj", which is the combination of an interface
/// with an associated name.
///
/// This frequently is used as a two-part name, such as:
///
/// igb0/omicron
/// ^    ^
/// |    | AddrObject name
/// | Interface name
#[derive(Debug, PartialEq)]
pub struct AddrObject {
    interface: String,
    name: String,
}

impl AddrObject {
    pub fn new_control(interface: &str) -> Self {
        Self::new(interface, "omicron")
    }

    pub fn on_same_interface(&self, name: &str) -> Self {
        Self::new(&self.interface, name)
    }

    pub fn new(interface: &str, name: &str) -> Self {
        // TODO: These could be checked / returned as a Result.
        assert!(!interface.contains('/'));
        assert!(!name.contains('/'));
        Self { interface: interface.to_string(), name: name.to_string() }
    }
}

impl ToString for AddrObject {
    fn to_string(&self) -> String {
        format!("{}/{}", self.interface, self.name)
    }
}
