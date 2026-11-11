// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for silo types.

use crate::latest::silo::{
    AuthenticationMode, SiloIdentityMode, SiloQuotasCreate, SiloUtilization,
    UserProvisionType, VirtualResourceCounts,
};
use omicron_common::api::external::{ByteCount, Name, SimpleIdentityOrName};
use uuid::Uuid;

impl SiloIdentityMode {
    pub fn authentication_mode(&self) -> AuthenticationMode {
        match self {
            SiloIdentityMode::LocalOnly => AuthenticationMode::Local,
            SiloIdentityMode::SamlJit => AuthenticationMode::Saml,
            SiloIdentityMode::SamlScim => AuthenticationMode::Saml,
        }
    }

    pub fn user_provision_type(&self) -> UserProvisionType {
        match self {
            SiloIdentityMode::LocalOnly => UserProvisionType::ApiOnly,
            SiloIdentityMode::SamlJit => UserProvisionType::Jit,
            SiloIdentityMode::SamlScim => UserProvisionType::Scim,
        }
    }
}

// We want to be able to paginate SiloUtilization by NameOrId
// but we can't derive ObjectIdentity because this isn't a typical asset.
// Instead we implement this new simple identity trait which is used under the
// hood by the pagination code.
impl SimpleIdentityOrName for SiloUtilization {
    fn id(&self) -> Uuid {
        self.silo_id
    }
    fn name(&self) -> &Name {
        &self.silo_name
    }
}

impl SiloQuotasCreate {
    /// All quotas set to 0
    pub fn empty() -> Self {
        Self {
            cpus: 0,
            memory: ByteCount::from(0),
            storage: ByteCount::from(0),
        }
    }

    /// An arbitrarily high but identifiable default for quotas
    /// that can be used for creating a Silo for testing
    ///
    /// The only silo that customers will see that this should be set on is the default
    /// silo. Ultimately the default silo should only be initialized with an empty quota,
    /// but as tests currently relying on it having a quota, we need to set something.
    pub fn arbitrarily_high_default() -> Self {
        Self {
            cpus: 9999999999,
            memory: ByteCount::try_from(999999999999999999_u64).unwrap(),
            storage: ByteCount::try_from(999999999999999999_u64).unwrap(),
        }
    }
}

impl From<SiloQuotasCreate> for VirtualResourceCounts {
    fn from(quota: SiloQuotasCreate) -> Self {
        Self { cpus: quota.cpus, memory: quota.memory, storage: quota.storage }
    }
}
