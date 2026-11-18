// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for wicket SSH authentication types.

use crate::latest::wicket_ssh_auth::WicketSshAuth;
use crate::latest::wicket_ssh_auth::WicketSshAuthKind;
use crate::latest::wicket_ssh_auth::WicketSshAuthKindCreate;
use crate::latest::wicket_ssh_auth::WicketSshKey;
use sled_agent_types_versions::latest::inventory as sled_agent_types;

impl From<WicketSshAuth> for sled_agent_types::WicketSshAuth {
    fn from(value: WicketSshAuth) -> Self {
        let WicketSshAuth {
            // meaningful fields to sled-agent
            generation,
            kind,
            // fields only meaningful to nexus
            time_created: _,
            description: _,
        } = value;

        Self { generation, kind: kind.into() }
    }
}

impl From<WicketSshAuthKind> for sled_agent_types::WicketSshAuthKind {
    fn from(value: WicketSshAuthKind) -> Self {
        match value {
            WicketSshAuthKind::Unauthenticated => Self::Unauthenticated,
            WicketSshAuthKind::PublicKeys {
                authorized_keys,
                allow_support_keys,
            } => Self::PublicKeys {
                authorized_keys: authorized_keys
                    .into_iter()
                    .map(|ak| {
                        let WicketSshKey { key, description: _ } = ak;
                        key
                    })
                    .collect(),
                allow_support_keys,
            },
        }
    }
}

impl From<WicketSshAuthKindCreate> for WicketSshAuthKind {
    fn from(value: WicketSshAuthKindCreate) -> Self {
        match value {
            WicketSshAuthKindCreate::Unauthenticated => Self::Unauthenticated,
            WicketSshAuthKindCreate::PublicKeys {
                authorized_keys,
                allow_support_keys,
            } => Self::PublicKeys {
                authorized_keys: authorized_keys
                    .into_iter()
                    .map(|ak| WicketSshKey {
                        key: ak.key.into_string(),
                        description: ak.description,
                    })
                    .collect(),
                allow_support_keys,
            },
        }
    }
}
