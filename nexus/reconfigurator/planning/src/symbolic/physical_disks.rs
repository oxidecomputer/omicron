// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Symbolic representations of physical disks

use super::{
    test_harness::TestHarnessRng, Enumerable, ReificationError, SymbolMap,
    SymbolicId, SymbolicIdGenerator, TestPool,
};
use nexus_types::external_api::views::{PhysicalDiskPolicy, PhysicalDiskState};
use omicron_uuid_kinds::PhysicalDiskKind;
use serde::{Deserialize, Serialize};
use typed_rng::TypedUuidRng;

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

    pub fn reify(
        &self,
        pool: &mut TestPool,
        symbol_map: &mut SymbolMap,
    ) -> Result<omicron_common::disk::DiskIdentity, ReificationError> {
        // Check to see if we have a cached value
        if let Some(disk_id) = symbol_map.disk_identities.get(&self.symbolic_id)
        {
            return Ok(disk_id.clone());
        }

        // Get a new value from the pool if possible.
        let disk_id = pool
            .disk_identities
            .pop()
            .ok_or(ReificationError::OutOfDiskIdentities)?;

        // Save for later
        symbol_map.disk_identities.insert(self.symbolic_id, disk_id.clone());
        Ok(disk_id)
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

    pub fn reify(
        &self,
        symbol_map: &mut SymbolMap,
        rng: &mut TypedUuidRng<PhysicalDiskKind>,
    ) -> omicron_uuid_kinds::PhysicalDiskUuid {
        match symbol_map.disk_uuids.get(&self.symbolic_id) {
            Some(uuid) => *uuid,
            None => rng.next(),
        }
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

    pub fn reify(
        &self,
        pool: &mut TestPool,
        symbol_map: &mut SymbolMap,
        rng: &mut TestHarnessRng,
    ) -> Result<nexus_types::deployment::SledDisk, ReificationError> {
        Ok(nexus_types::deployment::SledDisk {
            disk_identity: self.disk_identity.reify(pool, symbol_map)?,
            disk_id: self
                .disk_id
                .reify(symbol_map, &mut rng.physical_disks_rng),
            policy: self.policy,
            state: self.state,
        })
    }
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
