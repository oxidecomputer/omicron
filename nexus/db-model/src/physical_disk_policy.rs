// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a disks's operator-defined policy.
//!
//! This is related to, but different from `PhysicalDiskState`: a disk's **policy** is
//! its disposition as specified by the operator, while its **state** refers to
//! what's currently on it, as determined by Nexus.
//!
//! For example, a disk might be in the `Active` state, but have a policy of
//! `Expunged` -- this would mean that Nexus knows about resources currently
//! provisioned on the disk, but the operator has said that it should be marked
//! as gone.

use super::impl_enum_type;
use nexus_types::external_api::physical_disk;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    PhysicalDiskPolicyEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum PhysicalDiskPolicy;

    // Enum values
    InService => b"in_service"
    Expunged => b"expunged"
);

impl From<PhysicalDiskPolicy> for physical_disk::PhysicalDiskPolicy {
    fn from(policy: PhysicalDiskPolicy) -> Self {
        match policy {
            PhysicalDiskPolicy::InService => {
                physical_disk::PhysicalDiskPolicy::InService
            }
            PhysicalDiskPolicy::Expunged => {
                physical_disk::PhysicalDiskPolicy::Expunged
            }
        }
    }
}

impl From<physical_disk::PhysicalDiskPolicy> for PhysicalDiskPolicy {
    fn from(policy: physical_disk::PhysicalDiskPolicy) -> Self {
        match policy {
            physical_disk::PhysicalDiskPolicy::InService => {
                PhysicalDiskPolicy::InService
            }
            physical_disk::PhysicalDiskPolicy::Expunged => {
                PhysicalDiskPolicy::Expunged
            }
        }
    }
}
