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
#[derive(Debug, PartialEq, Clone)]
pub struct AddrObject {
    interface: String,
    name: String,
}

/// Errors which may be returned from constructing an [`AddrObject`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to parse addrobj name: {0}")]
    Parse(String),
}

impl AddrObject {
    pub fn new_control(interface: &str) -> Result<Self, Error> {
        Self::new(interface, "omicron")
    }

    pub fn on_same_interface(&self, name: &str) -> Result<Self, Error> {
        Self::new(&self.interface, name)
    }

    pub fn new(interface: &str, name: &str) -> Result<Self, Error> {
        if interface.contains('/') {
            return Err(Error::Parse(interface.to_string()));
        }
        if name.contains('/') {
            return Err(Error::Parse(name.to_string()));
        }
        Ok(Self { interface: interface.to_string(), name: name.to_string() })
    }

    pub fn interface(&self) -> &str {
        &self.interface
    }
}

impl std::fmt::Display for AddrObject {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}/{}", self.interface, self.name)
    }
}
