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
use omicron_common::api::internal::nexus::DownstairsClientStopRequest;
use omicron_common::api::internal::nexus::DownstairsClientStopped;
use omicron_common::api::internal::nexus::RepairFinishInfo;
use omicron_common::api::internal::nexus::RepairProgress;
use omicron_common::api::internal::nexus::RepairStartInfo;
use omicron_uuid_kinds::DownstairsKind;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsKind;
use omicron_uuid_kinds::UpstairsRepairKind;
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

        self.sagas
            .saga_execute::<sagas::volume_remove_rop::SagaVolumeRemoveROP>(
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
                        repair_start_info.time,
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
            // First, record this notification

            self.db_datastore
                .upstairs_repair_notification(
                    opctx,
                    UpstairsRepairNotification::new(
                        repair_finish_info.time,
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

            // If the live repair or reconciliation was successfully completed,
            // check if the repaired downstairs is part of a region
            // replacement request.

            if !repair_finish_info.aborted {
                let maybe_region_replacement = self
                    .datastore()
                    .lookup_in_progress_region_replacement_request_by_new_region_id(
                        opctx,
                        repaired_downstairs.region_uuid,
                    )
                    .await?;

                if maybe_region_replacement.is_none() {
                    // A live repair or reconciliation completed successfully,
                    // but there is no in-progress region replacement request
                    // for that region, so it wasn't initated by Nexus.
                    //
                    // TODO-followup if there are too many repairs to the same
                    // downstairs, do something with that information.
                }
            }
        }

        Ok(())
    }

    /// An Upstairs is updating us with repair progress
    pub(crate) async fn upstairs_repair_progress(
        self: &Arc<Self>,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        repair_id: TypedUuid<UpstairsRepairKind>,
        repair_progress: RepairProgress,
    ) -> DeleteResult {
        info!(
            self.log,
            "received upstairs_repair_progress from upstairs {upstairs_id} for repair {repair_id}: {:?}",
            repair_progress,
        );

        self.db_datastore
            .upstairs_repair_progress(
                opctx,
                upstairs_id,
                repair_id,
                repair_progress,
            )
            .await
    }

    /// An Upstairs is telling us that a Downstairs client task was requested to
    /// stop
    pub(crate) async fn downstairs_client_stop_request_notification(
        self: &Arc<Self>,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        downstairs_id: TypedUuid<DownstairsKind>,
        downstairs_client_stop_request: DownstairsClientStopRequest,
    ) -> DeleteResult {
        info!(
            self.log,
            "received downstairs_client_stop_request_notification from upstairs {upstairs_id} for downstairs {downstairs_id}: {:?}",
            downstairs_client_stop_request,
        );

        self.db_datastore
            .downstairs_client_stop_request_notification(
                opctx,
                upstairs_id,
                downstairs_id,
                downstairs_client_stop_request,
            )
            .await
    }

    /// An Upstairs is telling us that a Downstairs client task was stopped
    pub(crate) async fn downstairs_client_stopped_notification(
        self: &Arc<Self>,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        downstairs_id: TypedUuid<DownstairsKind>,
        downstairs_client_stopped: DownstairsClientStopped,
    ) -> DeleteResult {
        info!(
            self.log,
            "received downstairs_client_stopped_notification from upstairs {upstairs_id} for downstairs {downstairs_id}: {:?}",
            downstairs_client_stopped,
        );

        self.db_datastore
            .downstairs_client_stopped_notification(
                opctx,
                upstairs_id,
                downstairs_id,
                downstairs_client_stopped,
            )
            .await
    }
}
