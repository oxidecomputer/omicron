// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for operating on addrobj objects.

/// The name provided to all link-local IPv6 addresses.
pub const IPV6_LINK_LOCAL_ADDROBJ_NAME: &str = "ll";

/// The name provided to all static IPv6 underlay addresses.
pub const IPV6_STATIC_ADDROBJ_NAME: &str = "omicron6";

/// The name provided to all static IPv4 addresses, usually for public OPTE
/// interfaces.
pub const IPV4_STATIC_ADDROBJ_NAME: &str = "omicron4";

/// The name provided to DHCP-configured addresses, of either family.
pub const DHCP_ADDROBJ_NAME: &str = "omicron";

/// Describes an "addrobj", which is the combination of an interface
/// with an associated name.
///
/// This frequently is used as a two-part name, such as:
///
/// igb0/omicron
/// ^    ^
/// |    | AddrObject name
/// | Interface name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AddrObject {
    interface: String,
    name: String,
}

#[derive(Debug, PartialEq, Clone)]
enum BadName {
    Interface(String),
    Object(String),
}

impl std::fmt::Display for BadName {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        match self {
            BadName::Interface(s) => write!(f, "Bad interface name: {}", s),
            BadName::Object(s) => write!(f, "Bad object name: {}", s),
        }
    }
}

/// Errors which may be returned from constructing an [`AddrObject`].
#[derive(Debug, thiserror::Error)]
#[error("Failed to parse addrobj name: {name}")]
pub struct ParseError {
    name: BadName,
}

impl AddrObject {
    pub fn new_control(interface: &str) -> Result<Self, ParseError> {
        Self::new(interface, "omicron")
    }

    pub fn on_same_interface(&self, name: &str) -> Result<Self, ParseError> {
        Self::new(&self.interface, name)
    }

    /// Create a new addrobj on the same interface with the IPv6 link-local
    /// name.
    pub fn link_local_on_same_interface(&self) -> Result<Self, ParseError> {
        self.on_same_interface(IPV6_LINK_LOCAL_ADDROBJ_NAME)
    }

    pub fn new(interface: &str, name: &str) -> Result<Self, ParseError> {
        if interface.contains('/') {
            return Err(ParseError {
                name: BadName::Interface(interface.to_string()),
            });
        }
        if name.contains('/') {
            return Err(ParseError { name: BadName::Object(name.to_string()) });
        }
        Ok(Self { interface: interface.to_string(), name: name.to_string() })
    }

    /// A link-local IPv6 addrobj over the provided interface.
    pub fn link_local(interface: &str) -> Result<Self, ParseError> {
        Self::new(interface, IPV6_LINK_LOCAL_ADDROBJ_NAME)
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
