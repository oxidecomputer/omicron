// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a sled's operator-defined policy.
//!
//! This is related to, but different from `SledState`: a sled's **policy** is
//! its disposition as specified by the operator, while its **state** refers to
//! what's currently on it, as determined by Nexus.
//!
//! For example, a sled might be in the `Active` state, but have a policy of
//! `Expunged` -- this would mean that Nexus knows about resources currently
//! provisioned on the sled, but the operator has said that it should be marked
//! as gone.

use super::impl_enum_type;
use nexus_types::external_api::views::{SledPolicy, SledProvisionPolicy};
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "sled_policy", schema = "public"))]
    pub struct SledPolicyEnum;

    /// This type is not actually public, because [`SledPolicy`] has a somewhat
    /// different, friendlier shape while being equivalent -- external code
    /// should always use [`SledPolicy`].
    ///
    /// However, it must be marked `pub` to avoid errors like `crate-private
    /// type `DbSledPolicy` in public interface`. Marking this type `pub`,
    /// without actually making it public, tricks rustc in a desirable way.
    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SledPolicyEnum)]
    pub enum DbSledPolicy;

    // Enum values
    InService => b"in_service"
    NoProvision => b"no_provision"
    Expunged => b"expunged"
);

/// Converts a [`SledPolicy`] to a version that can be inserted into a
/// database.
pub fn to_db_sled_policy(policy: SledPolicy) -> DbSledPolicy {
    match policy {
        SledPolicy::InService {
            provision_policy: SledProvisionPolicy::Provisionable,
        } => DbSledPolicy::InService,
        SledPolicy::InService {
            provision_policy: SledProvisionPolicy::NonProvisionable,
        } => DbSledPolicy::NoProvision,
        SledPolicy::Expunged => DbSledPolicy::Expunged,
    }
}

impl From<DbSledPolicy> for SledPolicy {
    fn from(policy: DbSledPolicy) -> Self {
        match policy {
            DbSledPolicy::InService => SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            },
            DbSledPolicy::NoProvision => SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
            DbSledPolicy::Expunged => SledPolicy::Expunged,
        }
    }
}
