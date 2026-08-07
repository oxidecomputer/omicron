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
use nexus_db_queries::db::datastore::FreedCrucibleResources;
use nexus_db_queries::db::model::CrucibleDataset;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::VolumeDeleteStatus;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::VolumeUuid;
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
        crucible_resources_to_delete: &CrucibleResources,
        status: &mut VolumeDeleteStatus,
    ) -> DeleteResult {
        let datasets_and_regions = match self
            .datastore
            .regions_to_delete(&crucible_resources_to_delete)
            .await
        {
            Ok(datasets_and_regions) => datasets_and_regions,

            Err(e) => {
                let s = format!("error calling regions_to_delete: {e}");
                error!(log, "{s}");
                status.errors.push(s);

                // Try again next time!
                return DeleteResult::WaitForNextActivation;
            }
        };

        let request_count = datasets_and_regions.len();
        if request_count == 0 {
            return DeleteResult::Deleted;
        }

        // Send DELETE calls to the corresponding Crucible agents
        let mut all_deleted = true;

        for (dataset, region) in &datasets_and_regions {
            match self.delete_crucible_region(log, &dataset, region.id()).await
            {
                Ok(DeleteResult::Deleted) => {
                    status
                        .region_results
                        .push(format!("{}: deleted", region.id()));
                }

                Ok(DeleteResult::WaitForNextActivation) => {
                    status.region_results.push(format!(
                        "{}: requested deletion, waiting",
                        region.id()
                    ));

                    all_deleted = false;
                }

                Err(e) => {
                    status.errors.push(format!("{e}"));

                    all_deleted = false;
                }
            }
        }

        // If any of the operations require waiting for the next activation,
        // then return that, otherwise return that all were deleted.

        if all_deleted {
            // When all crucible resources are cleaned up, hard delete the
            // region records. This also re-computes the crucible_dataset
            // size_used column for those region's datasets.

            let region_ids_to_delete =
                datasets_and_regions.iter().map(|(_, r)| r.id()).collect();

            match self
                .datastore
                .regions_hard_delete(log, region_ids_to_delete)
                .await
            {
                Ok(()) => DeleteResult::Deleted,

                Err(e) => {
                    let s = format!("error calling regions_hard_delete: {e}");
                    error!(log, "{s}");
                    status.errors.push(s);

                    // More work is required next task activation
                    DeleteResult::WaitForNextActivation
                }
            }
        } else {
            // More work is required next task activation
            DeleteResult::WaitForNextActivation
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

    async fn delete_crucible_running_snapshots(
        &self,
        log: &Logger,
        crucible_resources_to_delete: &CrucibleResources,
        status: &mut VolumeDeleteStatus,
    ) -> DeleteResult {
        let datasets_and_snapshots = match self
            .datastore
            .snapshots_to_delete(&crucible_resources_to_delete)
            .await
        {
            Ok(datasets_and_snapshots) => datasets_and_snapshots,

            Err(e) => {
                let s = format!("error calling snapshots_to_delete: {e}");
                error!(log, "{s}");
                status.errors.push(s);

                // Try again next time!
                return DeleteResult::WaitForNextActivation;
            }
        };

        let request_count = datasets_and_snapshots.len();
        if request_count == 0 {
            return DeleteResult::Deleted;
        }

        // Send DELETE calls to the corresponding Crucible agents
        let mut all_deleted = true;

        for (dataset, region_snapshot) in datasets_and_snapshots {
            let region_id = region_snapshot.region_id;
            let snapshot_id = region_snapshot.snapshot_id;

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
                }

                Ok(DeleteResult::WaitForNextActivation) => {
                    status.running_snapshot_results.push(format!(
                        "{} / {}: requested deletion, waiting",
                        region_id, snapshot_id,
                    ));

                    all_deleted = false;
                }

                Err(e) => {
                    status.errors.push(format!("{e}"));

                    all_deleted = false;
                }
            }
        }

        // If any of the operations require waiting for the next activation,
        // then return that, otherwise return that all were deleted.

        if all_deleted {
            DeleteResult::Deleted
        } else {
            DeleteResult::WaitForNextActivation
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
        crucible_resources_to_delete: &CrucibleResources,
        status: &mut VolumeDeleteStatus,
    ) -> DeleteResult {
        let datasets_and_snapshots = match self
            .datastore
            .snapshots_to_delete(&crucible_resources_to_delete)
            .await
        {
            Ok(datasets_and_snapshots) => datasets_and_snapshots,

            Err(e) => {
                let s = format!("error calling snapshots_to_delete: {e}");
                error!(log, "{s}");
                status.errors.push(s);

                // Try again next time!
                return DeleteResult::WaitForNextActivation;
            }
        };

        let request_count = datasets_and_snapshots.len();
        if request_count == 0 {
            return DeleteResult::Deleted;
        }

        // Send DELETE calls to the corresponding Crucible agents
        let mut all_deleted = true;

        for (dataset, region_snapshot) in &datasets_and_snapshots {
            let region_id = region_snapshot.region_id;
            let snapshot_id = region_snapshot.snapshot_id;

            match self
                .delete_crucible_snapshot(log, &dataset, region_id, snapshot_id)
                .await
            {
                Ok(DeleteResult::Deleted) => {
                    status.snapshot_results.push(format!(
                        "{} / {}: deleted",
                        region_id, snapshot_id
                    ));
                }

                Ok(DeleteResult::WaitForNextActivation) => {
                    status.snapshot_results.push(format!(
                        "{} / {}: requested deletion, waiting",
                        region_id, snapshot_id,
                    ));

                    all_deleted = false;
                }

                Err(e) => {
                    status.errors.push(format!("{e}"));

                    all_deleted = false;
                }
            }
        }

        // If any of the operations require waiting for the next activation,
        // then return that, otherwise return that all were deleted.

        if all_deleted {
            // When all crucible resources are cleaned up, hard delete the
            // region snapshot records.

            let mut result = DeleteResult::Deleted;

            for (_, region_snapshot) in datasets_and_snapshots {
                if let Err(e) = self
                    .datastore
                    .region_snapshot_remove(
                        region_snapshot.dataset_id.into(),
                        region_snapshot.region_id,
                        region_snapshot.snapshot_id,
                    )
                    .await
                {
                    let s =
                        format!("error calling region_snapshot_remove: {e}");
                    error!(log, "{s}");
                    status.errors.push(s);

                    // More work is required next task activation
                    result = DeleteResult::WaitForNextActivation
                }
            }

            result
        } else {
            DeleteResult::WaitForNextActivation
        }
    }

    async fn clean_up_volume_resources(
        &self,
        log: &Logger,
        crucible_resources_to_delete: &CrucibleResources,
        status: &mut VolumeDeleteStatus,
    ) -> DeleteResult {
        // For any resource that is no longer referenced due to the soft-delete
        // of a volume, do the following in order:
        //
        // - delete top level regions
        // - delete running snapshots (read: read-only regions backed by
        //   snapshots)
        // - delete region snapshots
        //
        // Running snapshots have to be deleted before snapshots are, but
        // attempt deleting both regions and running snapshots each task
        // activation as they are independent.

        let delete_regions_result = self
            .delete_crucible_regions(log, crucible_resources_to_delete, status)
            .await;

        let delete_region_snapshots_result = self
            .delete_crucible_running_snapshots(
                log,
                crucible_resources_to_delete,
                status,
            )
            .await;

        if delete_regions_result == DeleteResult::Deleted
            && delete_region_snapshots_result == DeleteResult::Deleted
        {
            // We're able to continue on with deleting snapshots
        } else {
            // Otherwise, more work is required from the next task activation.
            return DeleteResult::WaitForNextActivation;
        }

        // Past this point, running snapshots ar deleted, so delete any
        // snapshots. If this is successful, then all crucible resources to
        // delete have been cleaned up

        self.delete_crucible_snapshots(
            log,
            crucible_resources_to_delete,
            status,
        )
        .await
    }

    async fn conditionally_hard_delete_volume_record(
        &self,
        log: &Logger,
        volume_id: VolumeUuid,
        status: &mut VolumeDeleteStatus,
    ) {
        // Do not hard delete the volume record if there are lingering regions
        // associated with them. This occurs when a region snapshot hasn't been
        // deleted, which means we can't delete the region. Later on, deleting
        // the region snapshot when its reference count goes to zero will free
        // up the region(s) to be deleted (by delete_freed_crucible_regions).

        let allocated_regions =
            match self.datastore.get_allocated_regions(volume_id).await {
                Ok(allocated_regions) => allocated_regions,

                Err(e) => {
                    let s = format!(
                        "failed to get_allocated_regions for {volume_id}: {e}"
                    );
                    error!(log, "{s}");
                    status.errors.push(s);
                    return;
                }
            };

        if !allocated_regions.is_empty() {
            info!(
                &log,
                "allocated regions for {volume_id} is not-empty, skipping \
                hard delete",
            );

            return;
        }

        if let Err(e) = self.datastore.volume_hard_delete(volume_id).await {
            let s =
                format!("failed to volume_hard_delete for {volume_id}: {e}");
            error!(log, "{s}");
            status.errors.push(s);
            return;
        }

        status.volumes_deleted.push(volume_id.to_string());
    }

    /// Deleting region snapshots may have freed up regions that were deleted in
    /// the DB but couldn't previously be deleted by the Crucible Agent because
    /// a snapshot existed. Look for those here.
    ///
    /// It's insufficient to rely on the struct of CrucibleResources to clean
    /// up: imagine a disk that is composed of three regions (a subset of the
    /// VolumeConstructionRequest is shown here):
    ///
    /// ```json
    /// {
    ///   "type": "volume",
    ///   "id": "6b353c87-afac-4ee2-b71a-6fe35fcf9e46",
    ///   "sub_volumes": [
    ///     {
    ///       "type": "region",
    ///       "opts": {
    ///         "targets": [
    ///           "[fd00:1122:3344:101::5]:1000",
    ///           "[fd00:1122:3344:102::9]:1000",
    ///           "[fd00:1122:3344:103::2]:1000"
    ///         ],
    ///         "read_only": false
    ///       }
    ///     }
    ///   ],
    ///   "read_only_parent": null,
    /// }
    /// ```
    ///
    /// Taking a snapshot of this will produce the following volume:
    ///
    /// ```json
    /// {
    ///   "type": "volume",
    ///   "id": "1ef7282e-a3fb-4222-85a8-b16d3fbfd738",   <-- new UUID
    ///   "sub_volumes": [
    ///     {
    ///       "type": "region",
    ///       "opts": {
    ///         "targets": [
    ///           "[fd00:1122:3344:101::5]:1001",         <-- port changed
    ///           "[fd00:1122:3344:102::9]:1001",         <-- port changed
    ///           "[fd00:1122:3344:103::2]:1001"          <-- port changed
    ///         ],
    ///         "read_only": true                         <-- read_only now true
    ///       }
    ///     }
    ///   ],
    ///   "read_only_parent": null,
    /// }
    /// ```
    ///
    /// The snapshot targets will use the same IP but different port: snapshots
    /// are initially located on the same filesystem as their region.
    ///
    /// The disk's volume has no read only resources, while the snapshot's
    /// volume does. The disk volume's targets are all regions (backed by
    /// downstairs that are read/write) while the snapshot volume's targets are
    /// all snapshots (backed by downstairs that are read-only). The two volumes
    /// are linked in the sense that the snapshots from the second are contained
    /// *within* the regions of the first, reflecting the resource nesting from
    /// ZFS. This is also reflected in the REST endpoint that the Crucible agent
    /// uses:
    ///
    ///   /crucible/0/regions/{id}/snapshots/{name}
    ///
    /// If the disk is then deleted, the CrucibleResources struct returned as
    /// from the soft-delete function will contain *nothing* to clean up: the
    /// regions contain snapshots that are part of other volumes and cannot be
    /// deleted, and the disk's volume doesn't reference any read-only
    /// resources.
    ///
    /// This is expected and normal: regions are "leaked" all the time due to
    /// snapshots preventing their deletion. This function detects when those
    /// regions can be cleaned up.
    ///
    /// Note: each delete of a snapshot could trigger another delete of a
    /// region, if that region's use has gone to zero. A snapshot delete will
    /// never trigger another snapshot delete.
    async fn delete_freed_crucible_regions(
        &self,
        log: &Logger,
        status: &mut VolumeDeleteStatus,
    ) {
        // Find regions freed up for deletion by a previous delete of region
        // snapshots.
        let freed_datasets_regions_and_volumes =
            match self.datastore.find_deleted_volume_regions().await {
                Ok(freed_datasets_regions_and_volumes) => {
                    freed_datasets_regions_and_volumes
                }

                Err(e) => {
                    let s =
                        format!("failed to find_deleted_volume_regions: {e}");
                    error!(log, "{s}");
                    status.errors.push(s);

                    // Try again next time!
                    return;
                }
            };

        if freed_datasets_regions_and_volumes.is_empty() {
            return;
        }

        let FreedCrucibleResources { datasets_and_regions, volumes } =
            freed_datasets_regions_and_volumes;

        let mut all_deleted = true;

        for (dataset, region) in &datasets_and_regions {
            match self.delete_crucible_region(log, &dataset, region.id()).await
            {
                Ok(DeleteResult::Deleted) => {
                    status
                        .region_results
                        .push(format!("{}: deleted", region.id()));
                }

                Ok(DeleteResult::WaitForNextActivation) => {
                    status.region_results.push(format!(
                        "{}: requested deletion, waiting",
                        region.id()
                    ));

                    all_deleted = false;
                }

                Err(e) => {
                    all_deleted = false;

                    let s = format!(
                        "failed delete_crucible_region for {}: {e}",
                        region.id(),
                    );
                    error!(log, "{s}");
                    status.errors.push(s);
                }
            }
        }

        if all_deleted {
            // When all crucible resources are cleaned up, hard delete the
            // region records. This also re-computes the crucible_dataset
            // size_used column for those region's datasets.

            let region_ids_to_delete =
                datasets_and_regions.iter().map(|(_, r)| r.id()).collect();

            match self
                .datastore
                .regions_hard_delete(log, region_ids_to_delete)
                .await
            {
                Ok(()) => {
                    // ok
                }

                Err(e) => {
                    let s = format!("error calling regions_hard_delete: {e}");
                    error!(log, "{s}");
                    status.errors.push(s);

                    // More work is required next task activation
                    return;
                }
            }

            for volume_id in volumes {
                // A Volume returned by `find_deleted_volume_regions` will not
                // have read/write regions, so it is safe to delete without
                // checking.

                if let Err(e) =
                    self.datastore.volume_hard_delete(volume_id).await
                {
                    let s = format!(
                        "error calling volume_hard_delete for {volume_id}: {e}"
                    );
                    error!(log, "{s}");
                    status.errors.push(s);
                }
            }
        } else {
            // More work is required next task activation
        }
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
                let Some(resources_to_clean_up) = &volume.resources_to_clean_up
                else {
                    let s = format!(
                        "volume {} has no resources to clean up",
                        volume.id(),
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
                                "volume {} resources to clean up did not \
                                deserialize: {e}",
                                volume.id(),
                            );
                            error!(log, "{s}");
                            status.errors.push(s);
                            continue;
                        }
                    };

                match self
                    .clean_up_volume_resources(
                        log,
                        &resources_to_clean_up,
                        &mut status,
                    )
                    .await
                {
                    DeleteResult::Deleted => {
                        self.conditionally_hard_delete_volume_record(
                            log,
                            volume.id(),
                            &mut status,
                        )
                        .await;
                    }

                    DeleteResult::WaitForNextActivation => {
                        continue;
                    }
                }
            }

            self.delete_freed_crucible_regions(log, &mut status).await;

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
