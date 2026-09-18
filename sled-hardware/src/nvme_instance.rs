// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The instance number the illumos kernel assigns to each `nvme` driver node.

use std::fmt;

/// The instance number the illumos kernel assigned to an NVMe controller
/// when it bound the `nvme` driver to it.
///
/// Every device node that binds to a driver gets an instance number,
/// unique among that driver's nodes and recorded in `/etc/path_to_inst`
/// so it survives reboots. It is the `3` in `nvme3`. libtopo records it as
/// the `io/instance` property of its `nvme` node, and libnvme opens
/// controllers by it. That shared key is how sled-hardware asks libnvme
/// about the controller the topology found in a bay.
///
/// Unrelated to the PCIe physical slot number and to NVMe firmware slots.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct NvmeInstance(i32);

/// An instance number outside the range libnvme accepts, which is zero
/// through `i32::MAX`.
#[derive(Debug, thiserror::Error)]
#[error("invalid nvme driver instance number: {0}")]
pub struct InvalidNvmeInstance(pub u32);

impl NvmeInstance {
    /// From the `io/instance` property of a topo node, which is unsigned.
    pub fn from_topo(value: u32) -> Result<Self, InvalidNvmeInstance> {
        i32::try_from(value).map(Self).map_err(|_| InvalidNvmeInstance(value))
    }

    /// The instance number as libnvme represents it.
    pub fn as_i32(self) -> i32 {
        self.0
    }
}

impl fmt::Display for NvmeInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "nvme{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inst(n: u32) -> NvmeInstance {
        NvmeInstance::from_topo(n).unwrap()
    }

    #[test]
    fn nvme_instance_conversions() {
        assert_eq!(inst(0).as_i32(), 0);
        assert_eq!(inst(17).as_i32(), 17);
        assert_eq!(inst(i32::MAX as u32).as_i32(), i32::MAX);
        assert!(NvmeInstance::from_topo(u32::MAX).is_err());
        assert!(NvmeInstance::from_topo(1 << 31).is_err());
        assert_eq!(inst(5).to_string(), "nvme5");
    }
}
