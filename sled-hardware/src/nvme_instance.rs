// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The instance number of an `nvme` driver node, the `N` in `nvme<N>`.
//!
//! devinfo reports it on the controller's node and libtopo reports it as
//! the `io/instance` property of its `nvme` node, so it is the key that
//! joins the two views of a controller. sled-hardware uses that join to
//! attach the chassis location topo knows about to the disk devinfo found.

use std::fmt;

/// Instance number of an `nvme` driver node, the `N` in `nvme<N>`.
///
/// Unrelated to the PCIe physical slot number and to NVMe firmware slots.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct NvmeInstance(i32);

/// An instance number outside the range devinfo and libnvme use, which is
/// zero through `i32::MAX`.
#[derive(Debug, thiserror::Error)]
#[error("invalid nvme driver instance number: {0}")]
pub struct InvalidNvmeInstance(pub i64);

impl NvmeInstance {
    /// The instance number as devinfo and libnvme represent it.
    pub fn as_i32(self) -> i32 {
        self.0
    }
}

impl TryFrom<i32> for NvmeInstance {
    type Error = InvalidNvmeInstance;

    /// From a devinfo instance number, which is signed but never negative.
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 0 {
            Err(InvalidNvmeInstance(value.into()))
        } else {
            Ok(Self(value))
        }
    }
}

impl TryFrom<u32> for NvmeInstance {
    type Error = InvalidNvmeInstance;

    /// From a topo `io/instance` property, which is unsigned.
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        i32::try_from(value)
            .map(Self)
            .map_err(|_| InvalidNvmeInstance(value.into()))
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

    fn inst(n: i32) -> NvmeInstance {
        NvmeInstance::try_from(n).unwrap()
    }

    #[test]
    fn nvme_instance_conversions() {
        assert_eq!(inst(0).as_i32(), 0);
        assert_eq!(inst(17).as_i32(), 17);
        assert!(NvmeInstance::try_from(-1i32).is_err());
        assert_eq!(NvmeInstance::try_from(3u32).unwrap(), inst(3));
        assert!(NvmeInstance::try_from(u32::MAX).is_err());
        assert_eq!(inst(5).to_string(), "nvme5");
    }
}
