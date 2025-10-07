// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::deployment::PlanningReport;
use crate::external_api::views;
use chrono::DateTime;
use chrono::Utc;
use gateway_types::component::SpType;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::AlertReceiverUuid;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::TufRepoUuid;
use omicron_uuid_kinds::WebhookDeliveryUuid;
use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use swrite::SWrite;
use swrite::swriteln;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

/// The status of a `region_replacement` background task activation
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RegionReplacementStatus {
    pub requests_created_ok: Vec<String>,
    pub start_invoked_ok: Vec<String>,
    pub requests_completed_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_replacement_drive` background task activation
#[derive(Serialize, Deserialize, Default)]
pub struct RegionReplacementDriverStatus {
    pub drive_invoked_ok: Vec<String>,
    pub finish_invoked_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `lookup_region_port` background task activation
#[derive(Serialize, Deserialize, Default)]
pub struct LookupRegionPortStatus {
    pub found_port_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_snapshot_replacement_start` background task
/// activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct RegionSnapshotReplacementStartStatus {
    pub requests_created_ok: Vec<String>,
    pub start_invoked_ok: Vec<String>,
    pub requests_completed_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_snapshot_replacement_garbage_collect` background
/// task activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct RegionSnapshotReplacementGarbageCollectStatus {
    pub garbage_collect_requested: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_snapshot_replacement_step` background task
/// activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct RegionSnapshotReplacementStepStatus {
    pub step_records_created_ok: Vec<String>,
    pub step_garbage_collect_invoked_ok: Vec<String>,
    pub step_invoked_ok: Vec<String>,
    pub step_set_volume_deleted_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of a `region_snapshot_replacement_finish` background task activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct RegionSnapshotReplacementFinishStatus {
    pub finish_invoked_ok: Vec<String>,
    pub errors: Vec<String>,
}

/// The status of an `abandoned_vmm_reaper` background task activation.
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct AbandonedVmmReaperStatus {
    pub vmms_found: usize,
    pub sled_reservations_deleted: usize,
    pub vmms_deleted: usize,
    pub vmms_already_deleted: usize,
    pub errors: Vec<String>,
}

/// The status of an `instance_updater` background task activation.
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct InstanceUpdaterStatus {
    /// if `true`, background instance updates have been explicitly disabled.
    pub disabled: bool,

    /// number of instances found with destroyed active VMMs
    pub destroyed_active_vmms: usize,

    /// number of instances found with failed active VMMs
    pub failed_active_vmms: usize,

    /// number of instances found with terminated active migrations
    pub terminated_active_migrations: usize,

    /// number of update sagas started.
    pub sagas_started: usize,

    /// number of sagas completed successfully
    pub sagas_completed: usize,

    /// errors returned by instance update sagas which failed, and the UUID of
    /// the instance which could not be updated.
    pub saga_errors: Vec<(Option<Uuid>, String)>,

    /// errors which occurred while querying the database for instances in need
    /// of updates.
    pub query_errors: Vec<String>,
}

impl InstanceUpdaterStatus {
    pub fn errors(&self) -> usize {
        self.saga_errors.len() + self.query_errors.len()
    }

    pub fn total_instances_found(&self) -> usize {
        self.destroyed_active_vmms
            + self.failed_active_vmms
            + self.terminated_active_migrations
    }
}

/// The status of an `instance_reincarnation` background task activation.
#[derive(Default, Serialize, Deserialize, Debug)]
pub struct InstanceReincarnationStatus {
    /// If `true`, then instance reincarnation has been explicitly disabled by
    /// the config file.
    pub disabled: bool,
    /// Total number of instances in need of reincarnation on this activation.
    /// This is broken down by the reason that the instance needed
    /// reincarnation.
    pub instances_found: BTreeMap<ReincarnationReason, usize>,
    /// UUIDs of instances reincarnated successfully by this activation.
    pub instances_reincarnated: Vec<ReincarnatableInstance>,
    /// UUIDs of instances which changed state before they could be
    /// reincarnated.
    pub changed_state: Vec<ReincarnatableInstance>,
    /// Any errors that occured while finding instances in need of reincarnation.
    pub errors: Vec<String>,
    /// Errors that occurred while restarting individual instances.
    pub restart_errors: Vec<(ReincarnatableInstance, String)>,
}

impl InstanceReincarnationStatus {
    pub fn total_instances_found(&self) -> usize {
        self.instances_found.values().sum()
    }

    pub fn total_errors(&self) -> usize {
        self.errors.len() + self.restart_errors.len()
    }

    pub fn total_sagas_started(&self) -> usize {
        self.instances_reincarnated.len()
            + self.changed_state.len()
            + self.restart_errors.len()
    }
}

/// Describes a reason why an instance needs reincarnation.
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Ord, PartialOrd,
)]
pub enum ReincarnationReason {
    /// The instance is Failed.
    Failed,
    /// A previous instance-start saga for this instance has failed.
    SagaUnwound,
}

impl std::fmt::Display for ReincarnationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Failed => "instance failed",
            Self::SagaUnwound => "start saga failed",
        })
    }
}

/// An instance eligible for reincarnation
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Ord, PartialOrd,
)]
pub struct ReincarnatableInstance {
    /// The instance's UUID
    pub instance_id: Uuid,
    /// Why the instance required reincarnation
    pub reason: ReincarnationReason,
}

impl std::fmt::Display for ReincarnatableInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { instance_id, reason } = self;
        write!(f, "{instance_id} ({reason})")
    }
}

/// Describes what happened while attempting to clean up Support Bundles.
#[derive(Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct SupportBundleCleanupReport {
    // Responses from Sled Agents
    pub sled_bundles_deleted_ok: usize,
    pub sled_bundles_deleted_not_found: usize,
    pub sled_bundles_delete_failed: usize,

    // Results from updating our database records
    pub db_destroying_bundles_removed: usize,
    pub db_failing_bundles_updated: usize,
}

/// Identifies what we could or could not store within a support bundle.
///
/// This struct will get emitted as part of the background task infrastructure.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SupportBundleCollectionReport {
    pub bundle: SupportBundleUuid,

    /// True iff we could list in-service sleds
    pub listed_in_service_sleds: bool,

    /// True iff we could list the service processors.
    pub listed_sps: bool,

    /// True iff the bundle was successfully made 'active' in the database.
    pub activated_in_db_ok: bool,

    /// Status of host OS ereport collection.
    pub host_ereports: SupportBundleEreportStatus,

    /// Status of SP ereport collection.
    pub sp_ereports: SupportBundleEreportStatus,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum SupportBundleEreportStatus {
    /// Ereports were not requested for this bundle.
    NotRequested,

    /// Ereports were collected successfully.
    Collected { n_collected: usize },

    /// Ereport collection failed, though some ereports may have been written.
    Failed { n_collected: usize, error: String },
}

impl SupportBundleCollectionReport {
    pub fn new(bundle: SupportBundleUuid) -> Self {
        Self {
            bundle,
            listed_in_service_sleds: false,
            listed_sps: false,
            activated_in_db_ok: false,
            host_ereports: SupportBundleEreportStatus::NotRequested,
            sp_ereports: SupportBundleEreportStatus::NotRequested,
        }
    }
}

/// The status of a `tuf_artifact_replication` background task activation
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TufArtifactReplicationStatus {
    pub generation: Generation,
    pub last_run_counters: TufArtifactReplicationCounters,
    pub lifetime_counters: TufArtifactReplicationCounters,
    pub request_debug_ringbuf: Arc<VecDeque<TufArtifactReplicationRequest>>,
    pub local_repos: usize,
}

impl TufArtifactReplicationStatus {
    pub fn last_run_ok(&self) -> bool {
        self.last_run_counters.list_err == 0
            && self.last_run_counters.put_err == 0
            && self.last_run_counters.copy_err == 0
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    derive_more::AddAssign,
)]
pub struct TufArtifactReplicationCounters {
    pub put_config_ok: usize,
    pub put_config_err: usize,
    pub list_ok: usize,
    pub list_err: usize,
    pub put_ok: usize,
    pub put_err: usize,
    pub copy_ok: usize,
    pub copy_err: usize,
}

impl TufArtifactReplicationCounters {
    pub fn inc(&mut self, request: &TufArtifactReplicationRequest) {
        match (&request.operation, &request.error) {
            (TufArtifactReplicationOperation::PutConfig { .. }, Some(_)) => {
                self.put_config_err += 1
            }
            (TufArtifactReplicationOperation::PutConfig { .. }, None) => {
                self.put_config_ok += 1
            }
            (TufArtifactReplicationOperation::List, Some(_)) => {
                self.list_err += 1
            }
            (TufArtifactReplicationOperation::List, None) => self.list_ok += 1,
            (TufArtifactReplicationOperation::Put { .. }, Some(_)) => {
                self.put_err += 1
            }
            (TufArtifactReplicationOperation::Put { .. }, None) => {
                self.put_ok += 1
            }
            (TufArtifactReplicationOperation::Copy { .. }, Some(_)) => {
                self.copy_err += 1
            }
            (TufArtifactReplicationOperation::Copy { .. }, None) => {
                self.copy_ok += 1
            }
        }
    }

    pub fn ok(&self) -> usize {
        self.put_config_ok
            .saturating_add(self.list_ok)
            .saturating_add(self.put_ok)
            .saturating_add(self.copy_ok)
    }

    pub fn err(&self) -> usize {
        self.put_config_err
            .saturating_add(self.list_err)
            .saturating_add(self.put_err)
            .saturating_add(self.copy_err)
    }

    pub fn sum(&self) -> usize {
        self.ok().saturating_add(self.err())
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct TufArtifactReplicationRequest {
    pub time: DateTime<Utc>,
    pub target_sled: SledUuid,
    #[serde(flatten)]
    pub operation: TufArtifactReplicationOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum TufArtifactReplicationOperation {
    PutConfig { generation: Generation },
    List,
    Put { hash: ArtifactHash },
    Copy { hash: ArtifactHash, source_sled: SledUuid },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TufRepoPrunerStatus {
    // Input
    /// how many recent releases we're configured to keep
    pub nkeep_recent_releases: u8,
    /// how many recent uploads we're configured to keep
    pub nkeep_recent_uploads: u8,

    // Output
    /// repos that we're keeping because they're a recent target release
    pub repos_keep_target_release: IdOrdMap<TufRepoInfo>,
    /// repos that we're keeping because they were recently uploaded
    pub repos_keep_recent_uploads: IdOrdMap<TufRepoInfo>,
    /// repo that we're pruning
    pub repo_prune: Option<TufRepoInfo>,
    /// other repos that were eligible for pruning
    pub other_repos_eligible_to_prune: IdOrdMap<TufRepoInfo>,
    /// runtime warnings while attempting to prune repos
    pub warnings: Vec<String>,
}

impl std::fmt::Display for TufRepoPrunerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn print_collection(c: &IdOrdMap<TufRepoInfo>) -> String {
            if c.is_empty() {
                return String::from("none\n");
            }

            let mut rv = String::from("\n");
            for repo in c {
                swriteln!(
                    rv,
                    "        {} ({}, created {})",
                    repo.id,
                    repo.system_version,
                    repo.time_created,
                );
            }

            rv
        }

        // This is indented appropriately for use in `omdb`.
        writeln!(f, "    configuration:")?;
        writeln!(
            f,
            "        nkeep_recent_releases: {}",
            self.nkeep_recent_releases
        )?;
        writeln!(
            f,
            "        nkeep_recent_uploads:  {}",
            self.nkeep_recent_releases
        )?;

        write!(f, "    repo pruned:")?;
        if let Some(pruned) = &self.repo_prune {
            writeln!(
                f,
                " {} ({}, created {})",
                pruned.id, pruned.system_version, pruned.time_created
            )?;
        } else {
            writeln!(f, " none")?;
        }

        write!(
            f,
            "    repos kept because they're recent target releases: {}",
            print_collection(&self.repos_keep_target_release)
        )?;

        write!(
            f,
            "    repos kept because they're recent uploads: {}",
            print_collection(&self.repos_keep_recent_uploads)
        )?;

        write!(
            f,
            "    other repos eligible for pruning: {}",
            print_collection(&self.other_repos_eligible_to_prune)
        )?;

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TufRepoInfo {
    pub id: TufRepoUuid,
    pub system_version: Version,
    pub time_created: DateTime<Utc>,
}

impl IdOrdItem for TufRepoInfo {
    type Key<'a> = &'a TufRepoUuid;

    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    id_upcast!();
}

/// The status of an `blueprint_rendezvous` background task activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlueprintRendezvousStatus {
    /// ID of the target blueprint during this activation.
    pub blueprint_id: BlueprintUuid,
    /// ID of the inventory collection used by this activation.
    pub inventory_collection_id: CollectionUuid,
    /// Counts of operations performed.
    pub stats: BlueprintRendezvousStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlueprintRendezvousStats {
    pub debug_dataset: DebugDatasetsRendezvousStats,
    pub crucible_dataset: CrucibleDatasetsRendezvousStats,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct CrucibleDatasetsRendezvousStats {
    /// Number of new Crucible datasets recorded.
    ///
    /// This is a count of in-service Crucible datasets that were also present
    /// in inventory and newly-inserted into `crucible_dataset`.
    pub num_inserted: usize,
    /// Number of Crucible datasets that would have been inserted, except
    /// records for them already existed.
    pub num_already_exist: usize,
    /// Number of Crucible datasets that the current blueprint says are
    /// in-service, but we did not attempt to insert them because they're not
    /// present in the latest inventory collection.
    pub num_not_in_inventory: usize,
}

impl slog::KV for CrucibleDatasetsRendezvousStats {
    fn serialize(
        &self,
        _record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { num_inserted, num_already_exist, num_not_in_inventory } =
            *self;
        serializer.emit_usize("num_inserted".into(), num_inserted)?;
        serializer.emit_usize("num_already_exist".into(), num_already_exist)?;
        serializer
            .emit_usize("num_not_in_inventory".into(), num_not_in_inventory)?;
        Ok(())
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct DebugDatasetsRendezvousStats {
    /// Number of new Debug datasets recorded.
    ///
    /// This is a count of in-service Debug datasets that were also present
    /// in inventory and newly-inserted into `rendezvous_debug_dataset`.
    pub num_inserted: usize,
    /// Number of Debug datasets that would have been inserted, except
    /// records for them already existed.
    pub num_already_exist: usize,
    /// Number of Debug datasets that the current blueprint says are
    /// in-service, but we did not attempt to insert them because they're not
    /// present in the latest inventory collection.
    pub num_not_in_inventory: usize,
    /// Number of Debug datasets that we tombstoned based on their disposition
    /// in the current blueprint being expunged.
    pub num_tombstoned: usize,
    /// Number of Debug datasets that we would have tombstoned, except they were
    /// already tombstoned or deleted.
    pub num_already_tombstoned: usize,
}

impl slog::KV for DebugDatasetsRendezvousStats {
    fn serialize(
        &self,
        _record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self {
            num_inserted,
            num_already_exist,
            num_not_in_inventory,
            num_tombstoned,
            num_already_tombstoned,
        } = *self;
        serializer.emit_usize("num_inserted".into(), num_inserted)?;
        serializer.emit_usize("num_already_exist".into(), num_already_exist)?;
        serializer
            .emit_usize("num_not_in_inventory".into(), num_not_in_inventory)?;
        serializer.emit_usize("num_tombstoned".into(), num_tombstoned)?;
        serializer.emit_usize(
            "num_already_tombstoned".into(),
            num_already_tombstoned,
        )?;
        Ok(())
    }
}

/// The status of a `blueprint_planner` background task activation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum BlueprintPlannerStatus {
    /// Automatic blueprint planning has been explicitly disabled
    /// by the config file.
    Disabled,

    /// The blueprint limit was reached, so automatic blueprint planning was
    /// disabled.
    LimitReached { limit: u64 },

    /// An error occurred during planning or blueprint insertion.
    Error(String),

    /// Planning produced a blueprint identital to the current target,
    /// so we threw it away and did nothing.
    Unchanged {
        parent_blueprint_id: BlueprintUuid,
        report: Arc<PlanningReport>,
    },

    /// Planning produced a new blueprint, but we failed to make it
    /// the current target and so deleted it.
    Planned {
        parent_blueprint_id: BlueprintUuid,
        error: String,
        report: Arc<PlanningReport>,
    },

    /// Planing succeeded, and we saved and made the new blueprint the
    /// current target.
    Targeted {
        parent_blueprint_id: BlueprintUuid,
        blueprint_id: BlueprintUuid,
        report: Arc<PlanningReport>,
    },
}

/// The status of a `alert_dispatcher` background task activation.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AlertDispatcherStatus {
    pub globs_reprocessed: BTreeMap<AlertReceiverUuid, ReprocessedGlobs>,

    pub glob_version: semver::Version,

    /// The alerts dispatched on this activation.
    pub dispatched: Vec<AlertDispatched>,

    /// Alerts  which did not have receivers.
    pub no_receivers: Vec<AlertUuid>,

    /// Any errors that occurred during activation.
    pub errors: Vec<String>,
}

type ReprocessedGlobs = BTreeMap<String, Result<AlertGlobStatus, String>>;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum AlertGlobStatus {
    AlreadyReprocessed,
    Reprocessed {
        created: usize,
        deleted: usize,
        prev_version: Option<semver::Version>,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AlertDispatched {
    pub alert_id: AlertUuid,
    pub subscribed: usize,
    pub dispatched: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDeliveratorStatus {
    pub by_rx: BTreeMap<AlertReceiverUuid, WebhookRxDeliveryStatus>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebhookRxDeliveryStatus {
    pub ready: usize,
    pub delivered_ok: usize,
    pub already_delivered: usize,
    pub in_progress: usize,
    pub failed_deliveries: Vec<WebhookDeliveryFailure>,
    pub delivery_errors: BTreeMap<WebhookDeliveryUuid, String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDeliveryFailure {
    pub delivery_id: WebhookDeliveryUuid,
    pub alert_id: AlertUuid,
    pub attempt: usize,
    pub result: views::WebhookDeliveryAttemptResult,
    pub response_status: Option<u16>,
    pub response_duration: Option<chrono::TimeDelta>,
}

/// The status of a `read_only_region_replacement_start` background task
/// activation
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct ReadOnlyRegionReplacementStartStatus {
    pub requests_created_ok: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct SpEreportIngesterStatus {
    /// If `true`, then ereport ingestion has been explicitly disabled by
    /// the config file.
    pub disabled: bool,
    pub sps: Vec<SpEreporterStatus>,
    pub errors: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SpEreporterStatus {
    pub sp_type: SpType,
    pub slot: u16,
    #[serde(flatten)]
    pub status: EreporterStatus,
}

#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub struct EreporterStatus {
    /// total number of ereports received from this reporter
    pub ereports_received: usize,
    /// number of new ereports ingested from this reporter (this may be less
    /// than `ereports_received` if some ereports were collected by another
    /// Nexus)
    pub new_ereports: usize,
    /// total number of HTTP requests sent.
    pub requests: usize,
    pub errors: Vec<String>,
}

#[cfg(test)]
mod test {
    use super::TufRepoInfo;
    use super::TufRepoPrunerStatus;
    use expectorate::assert_contents;
    use iddqd::IdOrdMap;

    #[test]
    fn test_display_tuf_repo_pruner_status() {
        let repo1 = TufRepoInfo {
            id: "4e8a87a0-3102-4014-99d3-e1bf486685bd".parse().unwrap(),
            system_version: "1.2.3".parse().unwrap(),
            time_created: "2025-09-29T01:23:45Z".parse().unwrap(),
        };
        let repo2 = TufRepoInfo {
            id: "867e42ae-ed72-4dc3-abcd-508b875c9601".parse().unwrap(),
            system_version: "4.5.6".parse().unwrap(),
            time_created: "2025-09-29T02:34:56Z".parse().unwrap(),
        };
        let repo_map: IdOrdMap<_> = std::iter::once(repo1.clone()).collect();

        let status = TufRepoPrunerStatus {
            nkeep_recent_releases: 1,
            nkeep_recent_uploads: 2,
            repos_keep_target_release: repo_map,
            repos_keep_recent_uploads: IdOrdMap::new(),
            repo_prune: Some(repo1.clone()),
            other_repos_eligible_to_prune: [repo1.clone(), repo2.clone()]
                .into_iter()
                .collect(),
            warnings: vec![String::from("fake-oh problem-oh")],
        };

        assert_contents(
            "output/tuf_repo_pruner_status.out",
            &status.to_string(),
        );
    }
}
