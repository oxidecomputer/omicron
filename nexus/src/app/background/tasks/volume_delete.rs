// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus is responsible for telling Crucible Agent(s) when to clean up
//! resources - those Agents do not have any idea of what volumes are
//! constructed, currently active, etc. Plus, volumes can (and will) change
//! during their lifetime. Operations like growing a disk, removing a read-only
//! parent after a scrub has completed, or re-encrypting a disk will all change
//! the volume that backs a disk.
//!
//! Nexus has to account for all the Crucible resources it is using, and count
//! how many volumes are using those resources. Only when that count drops to
//! zero is it valid to clean up the appropriate Crucible resource.
//!
//! Complicating things is the fact that ZFS datasets cannot be deleted if there
//! are snapshots of that dataset. Nexus' resource accounting must take this
//! dependency into account. Note that ZFS snapshots can layer, but any snapshot
//! can be deleted without the requirement of (for example) deleting the
//! snapshots in a certain order.
//!
//! Multiple Nexus will be running this background task and will result in many
//! identical requests being sent to the downstream Crucible agents. Each
//! Crucible agent's endpoint is idempotent and should handle this without any
//! issues.

use crate::app::background::BackgroundTask;
use crucible_agent_client::Client as CrucibleAgentClient;
use crucible_agent_client::types::GetSnapshotResponse;
use crucible_agent_client::types::Region;
use crucible_agent_client::types::RegionId;
use crucible_agent_client::types::State as RegionState;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::CrucibleResources;
use nexus_db_queries::db::model;
use nexus_db_queries::db::model::CrucibleDataset;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::VolumeDeleteStatus;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::DatasetUuid;
use serde_json::json;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

pub struct VolumeDeleter {
    datastore: Arc<DataStore>,
    reqwest_client: reqwest::Client,
}

/// Almost all endpoints that the Crucible Agent exposes are requests for
/// something asynchronous to occur. Functions that poll for the expected change
/// will either return that the change occurred, or that the current activation
/// of this task has to wait for the change to occur in a future activations of
/// this task.
#[derive(PartialEq)]
enum DeleteResult {
    Deleted,

    WaitForNextActivation,
}

// The functions in this impl can roughly be separated into two categories:
//
// 1. those that aceept a `status` argument and return a `DeleteResult`
// 2. those that do not accept a `status` argument and return a Result
//
// This separation attempts to prevent duplicates in the status' errors field.
// Functions that accept a status argument are responsible for adding to it,
// particularly adding to the errors field the Err results of functions that do
// not accept a status argument. Any function can write to the log.

impl VolumeDeleter {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        VolumeDeleter { datastore, reqwest_client: reqwest::Client::new() }
    }

    fn crucible_agent_client_for_dataset(
        &self,
        dataset: &CrucibleDataset,
    ) -> CrucibleAgentClient {
        CrucibleAgentClient::new_with_client(
            &format!("http://{}", dataset.address()),
            self.reqwest_client.clone(),
        )
    }

    /// Return true if the Crucible agent is gone, and false if it's expected to
    /// be there and answer Nexus.
    async fn crucible_agent_is_gone(
        &self,
        dataset_id: DatasetUuid,
    ) -> Result<bool, Error> {
        let on_in_service_physical_disk = self
            .datastore
            .crucible_dataset_physical_disk_in_service(dataset_id)
            .await?;

        let is_gone = !on_in_service_physical_disk;

        Ok(is_gone)
    }

    /// Returns a Ok(Some(Region)) if a region with id {region_id} exists,
    /// Ok(None) if it does not (a 404 was seen), and Err otherwise.
    async fn maybe_get_crucible_region(
        &self,
        log: &Logger,
        dataset: &CrucibleDataset,
        region_id: Uuid,
    ) -> Result<Option<Region>, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        if self.crucible_agent_is_gone(dataset_id).await? {
            return Err(Error::Gone);
        }

        match client.region_get(&RegionId(region_id.to_string())).await {
            Ok(v) => Ok(Some(v.into_inner())),

            Err(e) => {
                if is_not_found(&e) {
                    // A 404 Not Found is ok for this function, just return None
                    Ok(None)
                } else {
                    error!(
                        log,
                        "region_get saw {:?}",
                        e;
                        "region_id" => %region_id,
                        "dataset_id" => %dataset_id,
                    );

                    Err(into_external_error(&e))
                }
            }
        }
    }

    /// Send a region deletion request
    async fn request_crucible_region_delete(
        &self,
        log: &Logger,
        dataset: &CrucibleDataset,
        region_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        if self.crucible_agent_is_gone(dataset_id).await? {
            return Err(Error::Gone);
        }

        match client.region_delete(&RegionId(region_id.to_string())).await {
            Ok(_) => Ok(()),

            Err(e) => {
                error!(
                    log,
                    "region_delete saw {:?}",
                    e;
                    "region_id" => %region_id,
                    "dataset_id" => %dataset.id(),
                );

                Err(into_external_error(&e))
            }
        }
    }

    /// Call out to a Crucible agent to delete a region. Poll to see if that
    /// region's state has changed and indicated it has been deleted.
    async fn delete_crucible_region(
        &self,
        log: &Logger,
        dataset: &CrucibleDataset,
        region_id: Uuid,
    ) -> Result<DeleteResult, Error> {
        // If the region never existed, then a `GET` will return 404, and so
        // will a `DELETE`. Catch this case, and return Ok if the region never
        // existed. This can occur if ensuring all datasets and regions in a
        // region set partially fails, and the entire region set is being
        // deleted.

        match self.maybe_get_crucible_region(log, dataset, region_id).await {
            Ok(Some(_)) => {
                // region found, proceed with deleting
            }

            Ok(None) => {
                // region never exited, return Ok
                return Ok(DeleteResult::Deleted);
            }

            // Return Ok if the dataset's agent is gone, no delete call
            // is required.
            Err(Error::Gone) => {
                warn!(
                    log,
                    "dataset is gone";
                    "dataset_id" => %dataset.id(),
                );

                return Ok(DeleteResult::Deleted);
            }

            Err(e) => return Err(e),
        }

        // Past here, the region exists (or existed at some point): ensure it is
        // deleted. Request the deletion (which is idempotent), then query for
        // the appropriate state change.

        self.request_crucible_region_delete(log, dataset, region_id).await?;

        let region =
            match self.maybe_get_crucible_region(log, dataset, region_id).await
            {
                // Previous to the delete request, we queried and found this
                // region, and now it's gone.
                Ok(None) => Err(Error::internal_error(format!(
                    "dataset {} region {region_id} is missing now!",
                    dataset.id(),
                ))),

                Ok(Some(region)) => Ok(region),

                // Return Ok if the dataset's agent is gone, no state check is
                // required.
                Err(Error::Gone) => {
                    warn!(
                        log,
                        "dataset is gone";
                        "dataset_id" => %dataset.id(),
                    );

                    return Ok(DeleteResult::Deleted);
                }

                Err(e) => Err(e),
            }?;

        // If this state change query shows that the region was deleted,
        // proceed, otherwise the overall task needs to wait.

        match region.state {
            RegionState::Tombstoned => Ok(DeleteResult::WaitForNextActivation),

            RegionState::Destroyed => Ok(DeleteResult::Deleted),

            RegionState::Failed => {
                // If the delete failed, Nexus can re-request that the region be
                // deleted, and it will move back to Tombstoned. This will occur
                // on the next invocation of this task.

                Ok(DeleteResult::WaitForNextActivation)
            }

            RegionState::Requested | RegionState::Created => {
                // It's unexpected that the region be in this state after a
                // deletion request. We successfully requested the region
                // deletion before entering this check, and the Crucible agent
                // should prevent the state transition from Tombstoned to either
                // of these states.

                Err(Error::internal_error(format!(
                    "region is {:?} after successful deletion request!",
                    region.state,
                )))
            }
        }
    }

    async fn delete_crucible_regions(
        &self,
        log: &Logger,
        regions: &[model::Region],
        status: &mut VolumeDeleteStatus,
    ) {
        if regions.is_empty() {
            return;
        }

        let mut region_ids_to_hard_delete: Vec<Uuid> =
            Vec::with_capacity(regions.len());

        // Send DELETE calls to the corresponding Crucible agents

        for region in regions {
            let dataset = match self
                .datastore
                .crucible_dataset_get(region.dataset_id())
                .await
            {
                Ok(dataset) => dataset,

                Err(e) => {
                    let s = format!(
                        "failed calling crucible_dataset_get for {}: {e}",
                        region.dataset_id(),
                    );
                    error!(log, "{s}");
                    status.errors.push(s);

                    continue;
                }
            };

            match self.delete_crucible_region(log, &dataset, region.id()).await
            {
                Ok(DeleteResult::Deleted) => {
                    status
                        .region_results
                        .push(format!("{}: deleted", region.id()));

                    region_ids_to_hard_delete.push(region.id());
                }

                Ok(DeleteResult::WaitForNextActivation) => {
                    status.region_results.push(format!(
                        "{}: requested deletion, waiting",
                        region.id()
                    ));
                }

                Err(e) => {
                    status.errors.push(format!("{e}"));
                }
            }
        }

        // When all crucible resources are cleaned up, hard delete the region
        // records. This also re-computes the crucible_dataset size_used column
        // for those region's datasets.

        if let Err(e) = self
            .datastore
            .regions_hard_delete(log, region_ids_to_hard_delete)
            .await
        {
            let s = format!("error calling regions_hard_delete: {e}");
            error!(log, "{s}");
            status.errors.push(s);
        }
    }

    async fn request_crucible_running_snapshot_delete(
        &self,
        log: &Logger,
        dataset: &CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        if self.crucible_agent_is_gone(dataset_id).await? {
            return Err(Error::Gone);
        }

        match client
            .region_delete_running_snapshot(
                &RegionId(region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .await
        {
            Ok(_) => Ok(()),

            Err(e) => {
                error!(
                    log,
                    "region_delete_running_snapshot saw {:?}",
                    e;
                    "dataset_id" => %dataset.id(),
                    "region_id" => %region_id,
                    "snapshot_id" => %snapshot_id,
                );

                Err(into_external_error(&e))
            }
        }
    }

    async fn get_crucible_region_snapshots(
        &self,
        log: &Logger,
        dataset: &CrucibleDataset,
        region_id: Uuid,
    ) -> Result<GetSnapshotResponse, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        if self.crucible_agent_is_gone(dataset_id).await? {
            return Err(Error::Gone);
        }

        match client
            .region_get_snapshots(&RegionId(region_id.to_string()))
            .await
        {
            Ok(v) => Ok(v.into_inner()),

            Err(e) => {
                error!(
                    log,
                    "region_get_snapshots saw {:?}",
                    e;
                    "dataset_id" => %dataset.id(),
                    "region_id" => %region_id,
                );

                Err(into_external_error(&e))
            }
        }
    }

    async fn delete_crucible_running_snapshot(
        &self,
        log: &Logger,
        dataset: &CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<DeleteResult, Error> {
        // Request the deletion (which is idempotent), then query for the
        // appropriate state change.

        match self
            .request_crucible_running_snapshot_delete(
                log,
                dataset,
                region_id,
                snapshot_id,
            )
            .await
        {
            Ok(()) => {
                // ok
            }

            Err(Error::Gone) => {
                warn!(
                    log,
                    "dataset is gone";
                    "dataset_id" => %dataset.id(),
                );

                return Ok(DeleteResult::Deleted);
            }

            Err(e) => {
                return Err(e);
            }
        }

        let response = match self
            .get_crucible_region_snapshots(log, dataset, region_id)
            .await
        {
            Ok(v) => v,

            // Return Ok if the dataset's agent is gone, no
            // delete call is required.
            Err(Error::Gone) => {
                warn!(
                    log,
                    "dataset is gone";
                    "dataset_id" => %dataset.id(),
                );

                return Ok(DeleteResult::Deleted);
            }

            Err(e) => {
                return Err(e);
            }
        };

        match response.running_snapshots.get(&snapshot_id.to_string()) {
            Some(running_snapshot) => match running_snapshot.state {
                RegionState::Tombstoned => {
                    Ok(DeleteResult::WaitForNextActivation)
                }

                RegionState::Destroyed => Ok(DeleteResult::Deleted),

                _ => {
                    warn!(
                        log,
                        "running_snapshot is Some, state is {}",
                        running_snapshot.state.to_string();
                        "region_id" => %region_id,
                        "snapshot_id" => %snapshot_id,
                    );

                    Ok(DeleteResult::WaitForNextActivation)
                }
            },

            None => {
                // It's possible that the running snapshot record was GCed, and
                // it won't come back - consider it deleted.

                info!(
                    log,
                    "running_snapshot is None";
                    "region_id" => %region_id,
                    "snapshot_id" => %snapshot_id,
                );

                Ok(DeleteResult::Deleted)
            }
        }
    }

    async fn request_crucible_snapshot_delete(
        &self,
        log: &Logger,
        dataset: &CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        if self.crucible_agent_is_gone(dataset_id).await? {
            return Err(Error::Gone);
        }

        match client
            .region_delete_snapshot(
                &RegionId(region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .await
        {
            Ok(_) => Ok(()),

            Err(e) => {
                error!(
                    log,
                    "region_delete_snapshot saw {:?}",
                    e;
                    "dataset_id" => %dataset_id,
                    "region_id" => %region_id,
                    "snapshot_id" => %snapshot_id,
                );

                Err(into_external_error(&e))
            }
        }
    }

    async fn delete_crucible_snapshot(
        &self,
        log: &Logger,
        dataset: &CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<DeleteResult, Error> {
        // Unlike other Crucible agent endpoints, this one is synchronous in
        // that it is not only a request to the Crucible agent: `zfs destroy` is
        // performed right away. However this is still a request to illumos that
        // may not take effect right away. Wait until the snapshot no longer
        // appears in the list of region snapshots, meaning it was not returned
        // from `zfs list`.

        // Request the deletion (which is idempotent), then query for the
        // appropriate state change.

        match self
            .request_crucible_snapshot_delete(
                log,
                dataset,
                region_id,
                snapshot_id,
            )
            .await
        {
            Ok(()) => {
                // ok
            }

            Err(Error::Gone) => {
                return Ok(DeleteResult::Deleted);
            }

            Err(e) => {
                return Err(e);
            }
        }

        let response = match self
            .get_crucible_region_snapshots(log, dataset, region_id)
            .await
        {
            Ok(v) => v,

            // Return Ok if the dataset's agent is gone, no
            // delete call is required.
            Err(Error::Gone) => {
                warn!(
                    log,
                    "dataset is gone";
                    "dataset_id" => %dataset.id(),
                );

                return Ok(DeleteResult::Deleted);
            }

            Err(e) => {
                return Err(e);
            }
        };

        // If the snapshot is still returned in the list of snapshots, wait for
        // it to be deleted.

        if response.snapshots.iter().any(|x| x.name == snapshot_id.to_string())
        {
            Ok(DeleteResult::WaitForNextActivation)
        } else {
            Ok(DeleteResult::Deleted)
        }
    }

    async fn delete_crucible_snapshots(
        &self,
        log: &Logger,
        region_snapshots: &[model::RegionSnapshot],
        status: &mut VolumeDeleteStatus,
    ) {
        if region_snapshots.is_empty() {
            return;
        }

        // Send DELETE calls to the corresponding Crucible agents

        let mut region_snapshots_to_delete: Vec<&model::RegionSnapshot> =
            Vec::with_capacity(region_snapshots.len());

        for region_snapshot in region_snapshots {
            let dataset = match self
                .datastore
                .crucible_dataset_get(region_snapshot.dataset_id())
                .await
            {
                Ok(dataset) => dataset,

                Err(e) => {
                    let s = format!(
                        "failed calling crucible_dataset_get for {}: {e}",
                        region_snapshot.dataset_id(),
                    );
                    error!(log, "{s}");
                    status.errors.push(s);

                    continue;
                }
            };

            let region_id = region_snapshot.region_id;
            let snapshot_id = region_snapshot.snapshot_id;

            // First, delete the running snapshot

            match self
                .delete_crucible_running_snapshot(
                    log,
                    &dataset,
                    region_id,
                    snapshot_id,
                )
                .await
            {
                Ok(DeleteResult::Deleted) => {
                    status.running_snapshot_results.push(format!(
                        "{} / {}: deleted",
                        region_id, snapshot_id
                    ));

                    // continue on to deleting the snapshot
                }

                Ok(DeleteResult::WaitForNextActivation) => {
                    status.running_snapshot_results.push(format!(
                        "{} / {}: requested deletion, waiting",
                        region_id, snapshot_id,
                    ));

                    continue;
                }

                Err(e) => {
                    status.errors.push(format!("{e}"));

                    continue;
                }
            }

            // Then delete the snapshot

            match self
                .delete_crucible_snapshot(log, &dataset, region_id, snapshot_id)
                .await
            {
                Ok(DeleteResult::Deleted) => {
                    status.snapshot_results.push(format!(
                        "{} / {}: deleted",
                        region_id, snapshot_id
                    ));

                    region_snapshots_to_delete.push(region_snapshot);
                }

                Ok(DeleteResult::WaitForNextActivation) => {
                    status.snapshot_results.push(format!(
                        "{} / {}: requested deletion, waiting",
                        region_id, snapshot_id,
                    ));
                }

                Err(e) => {
                    status.errors.push(format!("{e}"));
                }
            }
        }

        // When all crucible resources are cleaned up, hard delete the region
        // snapshot records.

        for region_snapshot in region_snapshots_to_delete {
            if let Err(e) = self
                .datastore
                .region_snapshot_remove(
                    region_snapshot.dataset_id.into(),
                    region_snapshot.region_id,
                    region_snapshot.snapshot_id,
                )
                .await
            {
                let s = format!("error calling region_snapshot_remove: {e}");
                error!(log, "{s}");
                status.errors.push(s);
            }
        }
    }

    async fn delete_all_marked_resources(
        &self,
        log: &Logger,
        opctx: &OpContext,
        status: &mut VolumeDeleteStatus,
    ) {
        // Grab all regions and region snapshots that are marked for deletion

        let mut soft_deleted_crucible_resources = match self
            .datastore
            .crucible_resources_marked_for_deletion(opctx)
            .await
        {
            Ok(soft_deleted_crucible_resources) => {
                soft_deleted_crucible_resources
            }

            Err(e) => {
                let s = format!(
                    "error calling crucible_resources_marked_for_deletion: {e}"
                );
                error!(log, "{s}");
                status.errors.push(s);
                return;
            }
        };

        // Do the following in order:
        //
        // - delete running snapshots (read: read-only regions backed by
        //   snapshots)
        // - delete region snapshots
        // - delete top level regions
        //
        // Running snapshots have to be deleted before region snapshots are, and
        // region snapshots have to be deleted before regions are. Before
        // deleting regions, remove those that aren't ready for deletion.

        eprintln!("first: {soft_deleted_crucible_resources:?}");

        self.delete_crucible_snapshots(
            log,
            &soft_deleted_crucible_resources.region_snapshots,
            status,
        )
        .await;

        if let Err(e) = soft_deleted_crucible_resources
            .remove_regions_with_snapshots(&self.datastore)
            .await
        {
            let s = format!("error calling remove_regions_with_snapshots: {e}");
            error!(log, "{s}");
            status.errors.push(s);
            return;
        }

        eprintln!("second: {soft_deleted_crucible_resources:?}");

        self.delete_crucible_regions(
            log,
            &soft_deleted_crucible_resources.regions,
            status,
        )
        .await;
    }
}

impl BackgroundTask for VolumeDeleter {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            let mut status = VolumeDeleteStatus::default();

            // The volume soft delete transaction now simply marks regions and
            // region snapshots for deletion, but previous versions of Nexus
            // would serialize a set of resources to delete. Take that, mark all
            // those resources for deletion, then hard-delete the volume.

            let soft_deleted_volumes =
                match self.datastore.get_soft_deleted_volumes(opctx).await {
                    Ok(v) => v,

                    Err(e) => {
                        let s = format!(
                            "error calling get_soft_deleted_volumes: {e}"
                        );
                        error!(log, "{s}");
                        status.errors.push(s);
                        return json!(status);
                    }
                };

            for volume in soft_deleted_volumes {
                let volume_id = volume.id();

                let Some(resources_to_clean_up) = &volume.resources_to_clean_up
                else {
                    let s = format!(
                        "volume {volume_id} has no resources to clean up",
                    );
                    error!(log, "{s}");
                    status.errors.push(s);
                    continue;
                };

                let resources_to_clean_up: CrucibleResources =
                    match serde_json::from_str(&resources_to_clean_up) {
                        Ok(v) => v,

                        Err(e) => {
                            let s = format!(
                                "volume {volume_id} resources to clean up did \
                                not deserialize: {e}",
                            );
                            error!(log, "{s}");
                            status.errors.push(s);
                            continue;
                        }
                    };

                if let Err(e) = self
                    .datastore
                    .mark_crucible_resources_resources_for_deletion(
                        opctx,
                        resources_to_clean_up,
                    )
                    .await
                {
                    let s = format!(
                        "failed to mark_resources_for_delete for {volume_id}: \
                        {e}",
                    );

                    error!(log, "{s}");
                    status.errors.push(s);

                    continue;
                }

                match self.datastore.volume_hard_delete(volume_id).await {
                    Ok(()) => {
                        status.volumes_deleted.push(volume_id.to_string());
                    }

                    Err(e) => {
                        let s = format!(
                            "failed to volume_hard_delete for \
                            {volume_id}: {e}",
                        );

                        error!(log, "{s}");
                        status.errors.push(s);
                    }
                }
            }

            // Now delete all marked resources

            self.delete_all_marked_resources(log, opctx, &mut status).await;

            json!(status)
        }
        .boxed()
    }
}

fn is_not_found(
    e: &crucible_agent_client::Error<crucible_agent_client::types::Error>,
) -> bool {
    match e {
        crucible_agent_client::Error::ErrorResponse(rv) => match rv.status() {
            http::StatusCode::NOT_FOUND => true,
            _ => false,
        },

        _ => false,
    }
}

fn into_external_error(
    e: &crucible_agent_client::Error<crucible_agent_client::types::Error>,
) -> Error {
    match e {
        crucible_agent_client::Error::ErrorResponse(rv) => {
            if rv.status().is_client_error() {
                Error::invalid_request(&rv.message)
            } else {
                Error::internal_error(&rv.message)
            }
        }

        _ => Error::internal_error(format!("unexpected failure: {e}")),
    }
}
