// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SledPolicyEnum)]
    pub enum DbSledPolicy;

    // Enum values
    InService => b"in_service"
    InServiceNoProvision => b"in_service_no_provision"
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
        } => DbSledPolicy::InServiceNoProvision,
        SledPolicy::Expunged => DbSledPolicy::Expunged,
    }
}

impl DbSledPolicy {
    /// Converts self into the appropriate provision policy, in a lossy manner.
    pub fn to_provision_policy(self) -> SledProvisionPolicy {
        match self {
            DbSledPolicy::InService => SledProvisionPolicy::Provisionable,
            DbSledPolicy::InServiceNoProvision => {
                SledProvisionPolicy::NonProvisionable
            }
            DbSledPolicy::Expunged => SledProvisionPolicy::NonProvisionable,
        }
    }
}

impl From<DbSledPolicy> for SledPolicy {
    fn from(policy: DbSledPolicy) -> Self {
        match policy {
            DbSledPolicy::InService => SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            },
            DbSledPolicy::InServiceNoProvision => SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
            DbSledPolicy::Expunged => SledPolicy::Expunged,
        }
    }
}
