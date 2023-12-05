// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Volumes

use crate::app::sagas;
use nexus_db_queries::authn;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::DeleteResult;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    /// Start a saga to remove a read only parent from a volume.
    pub(crate) async fn volume_remove_read_only_parent(
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
