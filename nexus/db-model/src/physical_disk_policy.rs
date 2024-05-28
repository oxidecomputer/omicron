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
use nexus_types::external_api::views;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "physical_disk_policy", schema = "public"))]
    pub struct PhysicalDiskPolicyEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = PhysicalDiskPolicyEnum)]
    pub enum PhysicalDiskPolicy;

    // Enum values
    InService => b"in_service"
    Expunged => b"expunged"
);

impl From<PhysicalDiskPolicy> for views::PhysicalDiskPolicy {
    fn from(policy: PhysicalDiskPolicy) -> Self {
        match policy {
            PhysicalDiskPolicy::InService => {
                views::PhysicalDiskPolicy::InService
            }
            PhysicalDiskPolicy::Expunged => views::PhysicalDiskPolicy::Expunged,
        }
    }
}

impl From<views::PhysicalDiskPolicy> for PhysicalDiskPolicy {
    fn from(policy: views::PhysicalDiskPolicy) -> Self {
        match policy {
            views::PhysicalDiskPolicy::InService => {
                PhysicalDiskPolicy::InService
            }
            views::PhysicalDiskPolicy::Expunged => PhysicalDiskPolicy::Expunged,
        }
    }
}
