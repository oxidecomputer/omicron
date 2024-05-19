// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Symbolic representations of physical disks

use super::{Enumerable, SymbolicId, SymbolicIdGenerator};
use serde::{Deserialize, Serialize};

/// A symbolic representation of a `DiskIdentity`
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct DiskIdentity {
    pub symbolic_id: SymbolicId,
}

impl DiskIdentity {
    pub fn new(symbolic_id: SymbolicId) -> Self {
        DiskIdentity { symbolic_id }
    }
}

impl Enumerable for DiskIdentity {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// A symbolic representation of a `PhysicalDiskUuid`
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct PhysicalDiskUuid {
    symbolic_id: SymbolicId,
}

impl PhysicalDiskUuid {
    fn new(symbolic_id: SymbolicId) -> Self {
        PhysicalDiskUuid { symbolic_id }
    }
}

impl Enumerable for PhysicalDiskUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// A symbolic representation of a single disk that has been adopted by the
/// control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlanePhysicalDisk {
    pub disk_identity: DiskIdentity,
    pub disk_id: PhysicalDiskUuid,
    pub policy: PhysicalDiskPolicy,
    pub state: PhysicalDiskState,
}

impl ControlPlanePhysicalDisk {
    pub fn new(id_gen: &mut SymbolicIdGenerator) -> ControlPlanePhysicalDisk {
        ControlPlanePhysicalDisk {
            disk_identity: DiskIdentity::new(id_gen.next()),
            disk_id: PhysicalDiskUuid::new(id_gen.next()),
            policy: PhysicalDiskPolicy::InService,
            state: PhysicalDiskState::Active,
        }
    }
}

/// Symbolic representation of a `PhysicalDiskPolicy`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalDiskPolicy {
    InService,
    Expunged,
}

/// Symbolic representation of `PhysicalDiskState`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalDiskState {
    Active,
    Decommissioned,
}

/// A symbolic representation of a `ZpoolUuid`
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct ZpoolUuid {
    symbolic_id: SymbolicId,
}

impl ZpoolUuid {
    pub fn new(symbolic_id: SymbolicId) -> ZpoolUuid {
        ZpoolUuid { symbolic_id }
    }
}

impl Enumerable for ZpoolUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}
