// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for allocating and managing data links.

use crate::illumos::dladm::{
    CreateVnicError, DeleteVnicError, VnicSource, VNIC_PREFIX,
    VNIC_PREFIX_CONTROL, VNIC_PREFIX_GUEST,
};
use omicron_common::api::external::MacAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[cfg(not(test))]
use crate::illumos::dladm::Dladm;
#[cfg(test)]
use crate::illumos::dladm::MockDladm as Dladm;

/// A shareable wrapper around an atomic counter.
/// May be used to allocate runtime-unique IDs for objects
/// which have naming constraints - such as VNICs.
#[derive(Clone, Debug)]
pub struct VnicAllocator<DL: VnicSource + 'static> {
    value: Arc<AtomicU64>,
    scope: String,
    data_link: DL,
}

impl<DL: VnicSource + Clone> VnicAllocator<DL> {
    /// Creates a new Vnic name allocator with a particular scope.
    ///
    /// The intent with varying scopes is to create non-overlapping
    /// ranges of Vnic names, for example:
    ///
    /// VnicAllocator::new("Instance")
    /// - oxGuestInstance0
    /// - oxControlInstance0
    ///
    /// VnicAllocator::new("Storage") produces
    /// - oxControlStorage0
    pub fn new<S: AsRef<str>>(scope: S, data_link: DL) -> Self {
        Self {
            value: Arc::new(AtomicU64::new(0)),
            scope: scope.as_ref().to_string(),
            data_link,
        }
    }

    /// Creates a new NIC, intended for allowing Propolis to communicate
    /// with the control plane.
    pub fn new_control(
        &self,
        mac: Option<MacAddr>,
    ) -> Result<Link, CreateVnicError> {
        let allocator = self.new_superscope("Control");
        let name = allocator.next();
        debug_assert!(name.starts_with(VNIC_PREFIX));
        debug_assert!(name.starts_with(VNIC_PREFIX_CONTROL));
        Dladm::create_vnic(&self.data_link, &name, mac, None)?;
        Ok(Link { name, deleted: false, kind: LinkKind::OxideControlVnic })
    }

    fn new_superscope<S: AsRef<str>>(&self, scope: S) -> Self {
        Self {
            value: self.value.clone(),
            scope: format!("{}{}", scope.as_ref(), self.scope),
            data_link: self.data_link.clone(),
        }
    }

    /// Allocates a new VNIC name, which should be unique within the
    /// scope of this allocator.
    fn next(&self) -> String {
        format!("{}{}{}", VNIC_PREFIX, self.scope, self.next_id())
    }

    fn next_id(&self) -> u64 {
        self.value.fetch_add(1, Ordering::SeqCst)
    }
}

/// Represents the kind of a Link, such as whether it's for guest networking or
/// communicating with Oxide services.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkKind {
    Physical,
    OxideControlVnic,
    GuestVnic,
}

impl LinkKind {
    /// Infer the kind from a VNIC's name, if this one the sled agent can
    /// manage, and `None` otherwise.
    pub fn from_name(name: &str) -> Option<Self> {
        if name.starts_with(VNIC_PREFIX) {
            Some(LinkKind::OxideControlVnic)
        } else if name.starts_with(VNIC_PREFIX_GUEST) {
            Some(LinkKind::GuestVnic)
        } else {
            None
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("VNIC with name '{0}' is not valid for sled agent management")]
pub struct InvalidLinkKind(String);

/// Represents an allocated VNIC on the system.
/// The VNIC is de-allocated when it goes out of scope.
///
/// Note that the "ownership" of the VNIC is based on convention;
/// another process in the global zone could also modify / destroy
/// the VNIC while this object is alive.
#[derive(Debug)]
pub struct Link {
    name: String,
    deleted: bool,
    kind: LinkKind,
}

impl Link {
    /// Takes ownership of an existing VNIC.
    pub fn wrap_existing<S: AsRef<str>>(
        name: S,
    ) -> Result<Self, InvalidLinkKind> {
        match LinkKind::from_name(name.as_ref()) {
            Some(kind) => Ok(Self {
                name: name.as_ref().to_owned(),
                deleted: false,
                kind,
            }),
            None => Err(InvalidLinkKind(name.as_ref().to_owned())),
        }
    }

    /// Wraps a physical nic in a Link structure.
    ///
    /// It is the caller's responsibility to ensure this is a physical link.
    pub fn wrap_physical<S: AsRef<str>>(name: S) -> Self {
        Link {
            name: name.as_ref().to_owned(),
            deleted: false,
            kind: LinkKind::Physical,
        }
    }

    /// Deletes a NIC (if it has not already been deleted).
    pub fn delete(&mut self) -> Result<(), DeleteVnicError> {
        if self.deleted || self.kind == LinkKind::Physical {
            Ok(())
        } else {
            self.deleted = true;
            Dladm::delete_vnic(&self.name)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> LinkKind {
        self.kind
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        let r = self.delete();
        if let Err(e) = r {
            eprintln!("Failed to delete Link: {}", e);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::illumos::dladm::Etherstub;

    #[test]
    fn test_allocate() {
        let allocator =
            VnicAllocator::new("Foo", Etherstub("mystub".to_string()));
        assert_eq!("oxFoo0", allocator.next());
        assert_eq!("oxFoo1", allocator.next());
        assert_eq!("oxFoo2", allocator.next());
    }

    #[test]
    fn test_allocate_within_scopes() {
        let allocator =
            VnicAllocator::new("Foo", Etherstub("mystub".to_string()));
        assert_eq!("oxFoo0", allocator.next());
        let allocator = allocator.new_superscope("Baz");
        assert_eq!("oxBazFoo1", allocator.next());
    }
}
