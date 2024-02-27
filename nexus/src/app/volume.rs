// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Volumes

use crate::app::sagas;
use nexus_db_model::UpstairsRepairNotification;
use nexus_db_model::UpstairsRepairNotificationType;
use nexus_db_queries::authn;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::internal::nexus::RepairFinishInfo;
use omicron_common::api::internal::nexus::RepairStartInfo;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsKind;
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

    /// An Upstairs is telling us when a repair is starting.
    pub(crate) async fn upstairs_repair_start(
        self: &Arc<Self>,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        repair_start_info: RepairStartInfo,
    ) -> DeleteResult {
        info!(
            self.log,
            "received upstairs_repair_start from upstairs {upstairs_id}: {:?}",
            repair_start_info,
        );

        for repaired_downstairs in repair_start_info.repairs {
            self.db_datastore
                .upstairs_repair_notification(
                    opctx,
                    UpstairsRepairNotification::new(
                        repair_start_info.repair_id,
                        repair_start_info.repair_type.into(),
                        upstairs_id,
                        repair_start_info.session_id,
                        repaired_downstairs.region_uuid,
                        repaired_downstairs.target_addr,
                        UpstairsRepairNotificationType::Started,
                    ),
                )
                .await?;
        }

        Ok(())
    }

    /// An Upstairs is telling us when a repair is finished, and the result.
    pub(crate) async fn upstairs_repair_finish(
        self: &Arc<Self>,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        repair_finish_info: RepairFinishInfo,
    ) -> DeleteResult {
        info!(
            self.log,
            "received upstairs_repair_finish from upstairs {upstairs_id}: {:?}",
            repair_finish_info,
        );

        for repaired_downstairs in repair_finish_info.repairs {
            self.db_datastore
                .upstairs_repair_notification(
                    opctx,
                    UpstairsRepairNotification::new(
                        repair_finish_info.repair_id,
                        repair_finish_info.repair_type.into(),
                        upstairs_id,
                        repair_finish_info.session_id,
                        repaired_downstairs.region_uuid,
                        repaired_downstairs.target_addr,
                        if repair_finish_info.aborted {
                            UpstairsRepairNotificationType::Failed
                        } else {
                            UpstairsRepairNotificationType::Succeeded
                        },
                    ),
                )
                .await?;

            if !repair_finish_info.aborted {
                // TODO-followup if there's an active region replacement
                // occurring, a successfully completed live repair can trigger a
                // saga to destroy the original region.
            }
        }

        Ok(())
    }
}
