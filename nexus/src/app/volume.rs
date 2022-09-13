// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Volumes

use crate::app::sagas;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    /// Kick off a saga to delete a volume (and clean up any Crucible resources
    /// as a result). Importantly, this should not be a sub-saga - whoever is
    /// calling this should not block on cleaning up Crucible Resources, because
    /// the deletion of a "disk" or "snapshot" could free up a *lot* of Crucible
    /// resources and the user's query shouldn't wait on those DELETE calls.
    pub async fn volume_delete(
        self: &Arc<Self>,
        volume_id: Uuid,
    ) -> DeleteResult {
        let saga_params = sagas::volume_delete::Params { volume_id };

        // TODO execute this in the background instead, not using the usual SEC
        let saga_outputs = self
            .execute_saga::<sagas::volume_delete::SagaVolumeDelete>(saga_params)
            .await?;

        let volume_deleted =
            saga_outputs.lookup_node_output::<()>("final_no_result").map_err(
                |e| Error::InternalError { internal_message: e.to_string() },
            )?;

        Ok(volume_deleted)
    }
}
