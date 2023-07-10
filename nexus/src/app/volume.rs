// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Volumes

use crate::app::sagas;
use crate::authn;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    /// Kick off a saga to delete a volume (and clean up any Crucible resources
    /// as a result). Note that this does not unconditionally delete the volume
    /// record: if the allocated Crucible regions associated with this volume
    /// still have references, we cannot delete it, so it will be soft-deleted.
    /// Only when all the associated resources have been cleaned up does Nexus
    /// hard delete the volume record.
    ///
    /// Note it is **not** valid to call this function from an "action" node in
    /// a saga because it would not be idempotent in the case of a rerun. This
    /// function is ok to call from an "undo" node: as of this writing, this
    /// occurs when unwinding the creation of a resource. If that unwind fails,
    /// then the saga is parked in "Stuck" anyway.
    pub async fn volume_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        volume_id: Uuid,
    ) -> DeleteResult {
        let saga_params = sagas::volume_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            volume_id,
        };

        let saga_outputs = self
            .execute_saga::<sagas::volume_delete::SagaVolumeDelete>(saga_params)
            .await?;

        let volume_deleted =
            saga_outputs.lookup_node_output::<()>("final_no_result").map_err(
                |e| Error::InternalError { internal_message: e.to_string() },
            )?;

        Ok(volume_deleted)
    }

    /// Start a saga to remove a read only parent from a volume.
    pub async fn volume_remove_read_only_parent(
        self: &Arc<Self>,
        opctx: &OpContext,
        volume_id: Uuid,
    ) -> DeleteResult {
        let saga_params = sagas::volume_remove_rop::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            volume_id,
        };

        self.execute_saga::<sagas::volume_remove_rop::SagaVolumeRemoveROP>(
            saga_params,
        )
        .await?;

        Ok(())
    }
}
