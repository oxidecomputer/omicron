// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::impls::id_path_param;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

id_path_param!(
    SledExpungeRequestPath,
    sled_expunge_request_id,
    "sled expunge request",
    SledExpungeRequestUuid
);
