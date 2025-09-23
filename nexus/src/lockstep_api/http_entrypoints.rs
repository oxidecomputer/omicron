// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for HTTP APIs internal to the control plane
//! whose callers are updated in lockstep with Nexus

use crate::context::ApiContext;
use dropshot::ApiDescription;
use nexus_lockstep_api::*;

type NexusApiDescription = ApiDescription<ApiContext>;

/// Returns a description of the nexus lockstep API
pub(crate) fn lockstep_api() -> NexusApiDescription {
    nexus_lockstep_api_mod::api_description::<NexusLockstepApiImpl>()
        .expect("registered API endpoints successfully")
}

enum NexusLockstepApiImpl {}

impl NexusLockstepApi for NexusLockstepApiImpl {
    type Context = ApiContext;
}
