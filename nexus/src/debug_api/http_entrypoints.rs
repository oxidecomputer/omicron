// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for debugging-specific HTTP APIs internal to
//! the control plane

use crate::context::ApiContext;
use dropshot::ApiDescription;
use nexus_debug_api::*;

type NexusApiDescription = ApiDescription<ApiContext>;

/// Returns a description of the nexus debug API
pub(crate) fn debug_api() -> NexusApiDescription {
    nexus_debug_api_mod::api_description::<NexusDebugApiImpl>()
        .expect("registered API endpoints successfully")
}

enum NexusDebugApiImpl {}

impl NexusDebugApi for NexusDebugApiImpl {
    type Context = ApiContext;
}
