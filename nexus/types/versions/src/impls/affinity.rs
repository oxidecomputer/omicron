// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::affinity::{AffinityGroupMember, AntiAffinityGroupMember};

use omicron_common::api::external::{Name, SimpleIdentityOrName};
use omicron_uuid_kinds::GenericUuid;
use uuid::Uuid;

impl SimpleIdentityOrName for AffinityGroupMember {
    fn id(&self) -> Uuid {
        match self {
            AffinityGroupMember::Instance { id, .. } => *id.as_untyped_uuid(),
        }
    }

    fn name(&self) -> &Name {
        match self {
            AffinityGroupMember::Instance { name, .. } => name,
        }
    }
}

impl SimpleIdentityOrName for AntiAffinityGroupMember {
    fn id(&self) -> Uuid {
        match self {
            AntiAffinityGroupMember::Instance { id, .. } => {
                *id.as_untyped_uuid()
            }
        }
    }

    fn name(&self) -> &Name {
        match self {
            AntiAffinityGroupMember::Instance { name, .. } => name,
        }
    }
}
