// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::vlan::VlanID;
use crate::illumos::dladm::{
    PhysicalLink, VNIC_PREFIX_CONTROL, VNIC_PREFIX_GUEST,
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

type Error = crate::illumos::dladm::Error;

fn guest_vnic_name(id: u64) -> String {
    format!("{}{}", VNIC_PREFIX_GUEST, id)
}

pub fn control_vnic_name(id: u64) -> String {
    format!("{}{}", VNIC_PREFIX_CONTROL, id)
}

pub fn interface_name(vnic_name: &str) -> String {
    format!("{}/omicron", vnic_name)
}

/// A shareable wrapper around an atomic counter.
/// May be used to allocate runtime-unique IDs for objects
/// which have naming constraints - such as VNICs.
#[derive(Clone, Debug)]
pub struct IdAllocator {
    value: Arc<AtomicU64>,
}

impl IdAllocator {
    pub fn new() -> Self {
        Self { value: Arc::new(AtomicU64::new(0)) }
    }

    pub fn next(&self) -> u64 {
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

    /// Creates a new NIC, intended for usage by the guest.
    pub fn new_guest(
        allocator: &IdAllocator,
        physical_dl: &PhysicalLink,
        mac: Option<MacAddr>,
        vlan: Option<VlanID>,
    ) -> Result<Self, Error> {
        let name = guest_vnic_name(allocator.next());
        Dladm::create_vnic(physical_dl, &name, mac, vlan)?;
        Ok(Vnic { name, deleted: false })
    }

    /// Creates a new NIC, intended for allowing Propolis to communicate
    // with the control plane.
    pub fn new_control(
        allocator: &IdAllocator,
        physical_dl: &PhysicalLink,
        mac: Option<MacAddr>,
    ) -> Result<Self, Error> {
        let name = control_vnic_name(allocator.next());
        Dladm::create_vnic(physical_dl, &name, mac, None)?;
        Ok(Vnic { name, deleted: false })
    }

    // Deletes a NIC (if it has not already been deleted).
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
