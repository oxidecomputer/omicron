// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! User provided dropshot server context

use crate::artifacts::WicketdArtifactStore;
use crate::update_tracker::UpdateTracker;
use crate::MgsHandle;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use sled_hardware::Baseboard;

/// Shared state used by API handlers
pub struct ServerContext {
    pub mgs_handle: MgsHandle,
    pub mgs_client: gateway_client::Client,
    pub(crate) artifact_store: WicketdArtifactStore,
    pub(crate) update_tracker: UpdateTracker,
    pub(crate) baseboard: Baseboard,
}

impl ServerContext {
    pub(crate) fn can_update_target_sp(
        &self,
        id: SpIdentifier,
        state: &SpState,
    ) -> bool {
        // If we failed don't know our own baseboard, fall back to just using
        // the id, from which we know whether or not the target sled is one
        // of the scrimlets. If it is, refuse to update, because it might be
        // where we're running now.
        if self.baseboard == Baseboard::unknown() {
            !is_scrimlet(id)
        } else {
            // Otherwise, we can update this SP as long as it isn't ours.
            self.baseboard.identifier() != state.serial_number
                || self.baseboard.model() != state.model
                || self.baseboard.revision() != i64::from(state.revision)
        }
    }
}

/// TODO-correctness This function hardcodes the cubby locations that identify
/// scrimlets. This may be wrong in the future! We only use it as a fallback if
/// we aren't given a baseboard when we're launched by sled-agen; we should
/// decide if there's a more futureproof way to handle that failure.
fn is_scrimlet(id: SpIdentifier) -> bool {
    matches!((id.type_, id.slot), (SpType::Sled, 14 | 16))
}
