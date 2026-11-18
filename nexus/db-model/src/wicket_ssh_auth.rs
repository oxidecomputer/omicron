// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the operator-controlled wicket SSH auth settings in
//! the database

use crate::DbTypedGeneration;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::{wicket_ssh_auth, wicket_ssh_auth_key};
use nexus_types::external_api::wicket_ssh_auth as nexus_types;
use omicron_common::now_db_precision;
use omicron_generation_kinds::WicketSshAuthGeneration;
use omicron_generation_kinds::WicketSshAuthGenerationKind;

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = wicket_ssh_auth)]
pub struct WicketSshAuth {
    pub generation: DbTypedGeneration<WicketSshAuthGenerationKind>,
    pub time_created: DateTime<Utc>,
    pub description: String,

    // private: this encodes the `WicketSshAuthKind`:
    //
    // * `None` => unauthenticated
    // * `Some(true)` => authenticated with baked-in keys plus any
    //    operator-specified keys
    // * `Some(false)` => authenticated with only operator-specified keys
    allow_support_keys: Option<bool>,
}

impl WicketSshAuth {
    pub fn new(
        wicket_ssh_auth: &nexus_types::WicketSshAuthCreate,
        generation: WicketSshAuthGeneration,
    ) -> Self {
        let allow_support_keys = match &wicket_ssh_auth.kind {
            nexus_types::WicketSshAuthKindCreate::Unauthenticated => None,
            nexus_types::WicketSshAuthKindCreate::PublicKeys {
                authorized_keys: _,
                allow_support_keys,
            } => Some(*allow_support_keys),
        };
        Self {
            generation: generation.into(),
            time_created: now_db_precision(),
            description: wicket_ssh_auth.description.clone(),
            allow_support_keys,
        }
    }

    /// Converts this database [`WicketSshAuth`] row into the
    /// sled-agent-API-shaped `WicketSshAuth`, but without any
    /// operator-specified keys.
    ///
    /// If the returned variant is `Unauthenticated`, no keys are relevant. If
    /// the returned variant is `PublicKeys`, the `allow_support_keys`
    /// boolean will be filled in correctly, but the set of operator keys will
    /// be empty; the caller will need to fill them in by querying the
    /// appropriate generation of keys via [`WicketSshAuthKey`].
    pub fn to_wicket_ssh_auth_without_operator_keys(
        &self,
    ) -> nexus_types::WicketSshAuth {
        let kind = match self.allow_support_keys {
            None => nexus_types::WicketSshAuthKind::Unauthenticated,
            Some(allow_support_keys) => {
                nexus_types::WicketSshAuthKind::PublicKeys {
                    authorized_keys: Vec::new(),
                    allow_support_keys,
                }
            }
        };

        nexus_types::WicketSshAuth {
            generation: self.generation.into(),
            kind,
            time_created: self.time_created,
            description: self.description.clone(),
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = wicket_ssh_auth_key)]
pub struct WicketSshAuthKey {
    pub generation: DbTypedGeneration<WicketSshAuthGenerationKind>,
    pub authorized_key: String,
    pub description: String,
}

impl WicketSshAuthKey {
    pub fn new(
        auth_key: nexus_types::WicketSshKeyCreate,
        generation: WicketSshAuthGeneration,
    ) -> Self {
        Self {
            generation: generation.into(),
            authorized_key: auth_key.key.into_string(),
            description: auth_key.description,
        }
    }
}

impl From<WicketSshAuthKey> for nexus_types::WicketSshKey {
    fn from(value: WicketSshAuthKey) -> Self {
        Self { key: value.authorized_key, description: value.description }
    }
}
