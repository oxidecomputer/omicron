// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::vlan::VlanID;
use crate::illumos::dladm::{PhysicalLink, VNIC_PREFIX, VNIC_PREFIX_CONTROL};
use omicron_common::api::external::MacAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[cfg(not(test))]
use crate::illumos::dladm::Dladm;
#[cfg(test)]
use crate::illumos::dladm::MockDladm as Dladm;

type Error = crate::illumos::dladm::Error;

/// A shareable wrapper around an atomic counter.
/// May be used to allocate runtime-unique IDs for objects
/// which have naming constraints - such as VNICs.
#[derive(Clone, Debug)]
pub struct VnicAllocator {
    value: Arc<AtomicU64>,
    scope: String,
}

impl VnicAllocator {
    /// Creates a new Vnic name allocator with a particular scope.
    ///
    /// The intent with varying scopes is to create non-overlapping
    /// ranges of Vnic names, for example:
    ///
    /// VnicAllocator::new("Instance")
    /// - oxGuestInstance[NNN]
    /// - oxControlInstance[NNN]
    ///
    /// VnicAllocator::new("Storage") produces
    /// - oxControlStorage[NNN]
    pub fn new<S: AsRef<str>>(scope: S) -> Self {
        Self {
            value: Arc::new(AtomicU64::new(0)),
            scope: scope.as_ref().to_string(),
        }
    }

    /// Creates a new NIC, intended for usage by the guest.
    pub fn new_guest(
        &self,
        physical_dl: &PhysicalLink,
        mac: Option<MacAddr>,
        vlan: Option<VlanID>,
    ) -> Result<Vnic, Error> {
        let allocator = self.new_superscope("Guest");
        let name = allocator.next();
        debug_assert!(name.starts_with(VNIC_PREFIX));
        Dladm::create_vnic(physical_dl, &name, mac, vlan)?;
        Ok(Vnic { name, deleted: false })
    }

    /// Creates a new NIC, intended for allowing Propolis to communicate
    /// with the control plane.
    pub fn new_control(
        &self,
        physical_dl: &PhysicalLink,
        mac: Option<MacAddr>,
    ) -> Result<Vnic, Error> {
        let allocator = self.new_superscope("Control");
        let name = allocator.next();
        debug_assert!(name.starts_with(VNIC_PREFIX));
        debug_assert!(name.starts_with(VNIC_PREFIX_CONTROL));
        Dladm::create_vnic(physical_dl, &name, mac, None)?;
        Ok(Vnic { name, deleted: false })
    }

    fn new_superscope<S: AsRef<str>>(&self, scope: S) -> Self {
        Self {
            value: self.value.clone(),
            scope: format!("{}{}", scope.as_ref(), self.scope),
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

/// Represents an allocated VNIC on the system.
/// The VNIC is de-allocated when it goes out of scope.
///
/// Note that the "ownership" of the VNIC is based on convention;
/// another process in the global zone could also modify / destroy
/// the VNIC while this object is alive.
#[derive(Debug)]
pub struct Vnic {
    name: String,
    deleted: bool,
}

impl Vnic {
    /// Takes ownership of an existing VNIC.
    pub fn wrap_existing(name: String) -> Self {
        Vnic { name, deleted: false }
    }

    /// Deletes a NIC (if it has not already been deleted).
    pub fn delete(&mut self) -> Result<(), Error> {
        if self.deleted {
            Ok(())
        } else {
            self.deleted = true;
            Dladm::delete_vnic(&self.name)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for Vnic {
    fn drop(&mut self) {
        let r = self.delete();
        if let Err(e) = r {
            eprintln!("Failed to delete VNIC: {}", e);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_allocate() {
        let allocator = VnicAllocator::new("Foo");
        assert_eq!("oxFoo0", allocator.next());
        assert_eq!("oxFoo1", allocator.next());
        assert_eq!("oxFoo2", allocator.next());
    }

    #[test]
    fn test_allocate_within_scopes() {
        let allocator = VnicAllocator::new("Foo");
        assert_eq!("oxFoo0", allocator.next());
        let allocator = allocator.new_superscope("Baz");
        assert_eq!("oxBazFoo1", allocator.next());
    }
}
