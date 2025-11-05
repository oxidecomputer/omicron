// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::SitrepUuid;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlertRequest {
    pub id: AlertUuid,
    pub class: AlertClass,
    pub payload: serde_json::Value,
    pub requested_sitrep_id: SitrepUuid,
}

impl iddqd::IdOrdItem for AlertRequest {
    type Key<'a> = &'a AlertUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlertClass {
    PsuInserted,
    PsuRemoved,
}
