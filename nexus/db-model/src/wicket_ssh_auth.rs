// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the operator-controlled wicket SSH auth settings in
//! the database

use crate::DbTypedGeneration;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::{wicket_ssh_auth, wicket_ssh_auth_key};
use omicron_common::now_db_precision;
use omicron_generation_kinds::WicketSshAuthGenerationKind;
use sled_agent_types::inventory::WicketSshAuthKind;
use std::collections::BTreeSet;

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
    /// Converts the API-shaped `wicket_ssh_auth` into the db model type.
    ///
    /// If `wicket_ssh_auth.kind` contains any operator keys, they're discarded
    /// by this method; they must be stored as [`WicketSshAuthKey`] rows.
    pub fn new_discarding_operator_keys(
        wicket_ssh_auth: &sled_agent_types::inventory::WicketSshAuth,
        description: String,
    ) -> Self {
        let allow_support_keys = match &wicket_ssh_auth.kind {
            WicketSshAuthKind::Unauthenticated => None,
            WicketSshAuthKind::PublicKeys {
                authorized_keys: _,
                allow_support_keys,
            } => Some(*allow_support_keys),
        };
        Self {
            generation: wicket_ssh_auth.generation.into(),
            time_created: now_db_precision(),
            description,
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
    ) -> sled_agent_types::inventory::WicketSshAuth {
        let kind = match self.allow_support_keys {
            None => WicketSshAuthKind::Unauthenticated,
            Some(allow_support_keys) => WicketSshAuthKind::PublicKeys {
                authorized_keys: BTreeSet::new(),
                allow_support_keys,
            },
        };

        sled_agent_types::inventory::WicketSshAuth {
            generation: self.generation.into(),
            kind,
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
