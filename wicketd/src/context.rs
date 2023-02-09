// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! User provided dropshot server context

use crate::artifacts::WicketdArtifactStore;
use crate::update_planner::UpdatePlanner;
use crate::MgsHandle;

/// Shared state used by API handlers
pub struct ServerContext {
    pub mgs_handle: MgsHandle,
    pub(crate) artifact_store: WicketdArtifactStore,
    pub(crate) update_planner: UpdatePlanner,
}
