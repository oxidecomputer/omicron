// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Software Updates

use super::deployment::BlueprintTargetReleaseStatus;
use crate::app::background::LoadedTargetBlueprint;
use bytes::Bytes;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use dropshot::HttpError;
use futures::Stream;
use nexus_auth::authz;
use nexus_db_lookup::LookupPath;
use nexus_db_model::Generation;
use nexus_db_model::TufRepoUpload;
use nexus_db_model::TufTrustRoot;
use nexus_db_model::saga_types::Saga;
use nexus_db_model::saga_types::SagaId;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::{datastore::SQL_BATCH_SIZE, pagination::Paginator};
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::TargetReleaseDescription;
use nexus_types::external_api::update;
use nexus_types::external_api::update::TufSignedRootRole;
use nexus_types::identity::Asset;
use nexus_types::internal_api::views as internal_views;
use nexus_types::inventory::Collection;
use nexus_types::inventory::Zpool;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::Nullable;
use omicron_common::api::external::{DataPageParams, Error};
use omicron_uuid_kinds::{GenericUuid, SledUuid, TufTrustRootUuid};
use semver::Version;
use sled_agent_types::inventory::SvcsEnabledNotOnlineResult;
use sled_hardware_types::BaseboardId;
use slog::KV;
use slog::Record;
use slog::Serializer;
use slog::info;
use slog::warn;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::iter;
use std::sync::Arc;
use tokio::sync::watch;
use update_common::artifacts::{
    ArtifactsWithPlan, ControlPlaneZonesMode, VerificationMode,
};
use uuid::Uuid;

/// Threshold at which we consider an active saga stuck.
///
/// Sagas can sometimes spend time being unassigned or recovered across Nexus
/// restarts. To calculate this threshold we took a sample of 10,000 sagas and
/// only 3 took longer than 15 minutes from time_created to done (1h32m, 34m24s
/// and 19m23s). Since sagas running longer than 15 minutes are so rare in
/// practice, we use that as the threshold. Anything older is much more likely
/// stuck than legitimately still in progress.
// TODO-K: Remove in https://github.com/oxidecomputer/omicron/issues/10538
#[cfg(test)]
const STUCK_SAGA_THRESHOLD: TimeDelta = TimeDelta::minutes(15);

/// Threshold at which we consider an inventory collection too old for the
/// purpose of reporting system health via the update status endpoint
///
/// During an update, inventories are collected pretty frequently (around
/// once a minute or more).
const STALE_INVENTORY_THRESHOLD: TimeDelta = TimeDelta::minutes(20);

/// Threshold at which we consider the update status's last step planned to be
/// within the boundaries of an update in progress.
///
/// This is chosen to be large enough to cover any update-related step (e.g.,
/// sled reboot) under normal conditions. Host OS updates can take a very long
/// time, usually around 10 minutes. Using `omdb reconfigurator history` we
/// took a sample of 1000 events and the longest interval between steps during
/// an update was ~13 minutes between 2 sled host OS updates. We give ourselves
/// a bit more time than that before considering an update stuck.
const STUCK_UPDATE_THRESHOLD: TimeDelta = TimeDelta::minutes(20);

/// Used to pull data out of the channels
#[derive(Clone)]
pub struct UpdateStatusHandle {
    latest_blueprint: watch::Receiver<Option<LoadedTargetBlueprint>>,
}

impl UpdateStatusHandle {
    pub fn new(
        latest_blueprint: watch::Receiver<Option<LoadedTargetBlueprint>>,
    ) -> Self {
        Self { latest_blueprint }
    }
}

/// Inputs used to decide, based on health checks of a subset of system
/// components, whether the user should contact support before or after an
/// update.
struct UpdateContactSupportChecksInput {
    inventory: Arc<Collection>,
    stuck_sagas: Result<Vec<Saga>, Error>,
    blueprint: Arc<Blueprint>,
    // None when no target release has ever been set on the system.
    current_target_version: Option<Version>,
    internal_update_status: internal_views::UpdateStatus,
}

impl UpdateContactSupportChecksInput {
    /// Identify a set of problems present in the system based on a series of
    /// health checks.
    fn problems(&self) -> UpdateStatusProblems {
        let stuck_update_last_blueprint_created_time =
            match UpdateActivityState::new(
                &self.blueprint,
                self.current_target_version.as_ref(),
            ) {
                UpdateActivityState::Stuck => Some(self.blueprint.time_created),
                UpdateActivityState::Idle | UpdateActivityState::InProgress => {
                    None
                }
            };

        let missing_sleds: BTreeSet<SledUuid> = self
            .internal_update_status
            .sleds
            .iter()
            .filter(|sled| {
                // `unknown()` returns the representation of the update status
                // for a given sled ID that isn't present in inventory or hasn't
                // reported a reconciliation result yet.
                **sled
                    == internal_views::SledAgentUpdateStatus::unknown(
                        sled.sled_id,
                    )
            })
            .map(|sled| sled.sled_id)
            .collect();

        let (stuck_sagas, stuck_sagas_error_message) = match &self.stuck_sagas {
            Ok(sagas) => (
                sagas
                    .iter()
                    .map(|s| StuckSaga { id: s.id, name: s.name.clone() })
                    .collect(),
                None,
            ),
            Err(e) => (BTreeSet::new(), Some(e.to_string())),
        };

        let stale_inventory_last_collection_time_done = if self
            .inventory
            .time_done
            < Utc::now() - STALE_INVENTORY_THRESHOLD
        {
            Some(self.inventory.time_done)
        } else {
            None
        };

        let unhealthy_zpools_by_sled = self
            .inventory
            .unhealthy_zpools()
            .into_iter()
            .map(|(sled, zpools)| (sled, zpools.into_iter().cloned().collect()))
            .collect();

        let enabled_smf_services_not_online_by_sled = self
            .inventory
            .enabled_smf_services_not_online()
            .into_iter()
            .map(|(sled, svcs)| (sled, svcs.clone()))
            .collect();

        UpdateStatusProblems {
            stuck_sagas,
            stuck_sagas_error_message,
            stuck_update_last_blueprint_created_time,
            stale_inventory_last_collection_time_done,
            unhealthy_zpools_by_sled,
            enabled_smf_services_not_online_by_sled,
            missing_sleds,
        }
    }
}

/// Identifies a saga that has been running or unwinding for too long.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StuckSaga {
    id: SagaId,
    name: String,
}

/// Problems identified by update health checks.
#[derive(Debug, Default, PartialEq, Eq)]
struct UpdateStatusProblems {
    /// Sagas that have been running or unwinding longer than
    /// `STUCK_SAGA_THRESHOLD`.
    stuck_sagas: BTreeSet<StuckSaga>,
    /// Error message if the query for stuck sagas itself failed.
    stuck_sagas_error_message: Option<String>,
    /// The time the last blueprint was created if an update is in progress, and
    /// the last blueprint created is older than `STUCK_UPDATE_THRESHOLD`.
    stuck_update_last_blueprint_created_time: Option<DateTime<Utc>>,
    /// The time the last collection was finished if the latest inventory
    /// collection is older than `STALE_INVENTORY_THRESHOLD`.
    stale_inventory_last_collection_time_done: Option<DateTime<Utc>>,
    /// Zpools that are not in an `Online` state.
    unhealthy_zpools_by_sled: BTreeMap<SledUuid, Vec<Zpool>>,
    /// Enabled SMF services that are not in an `online` state.
    enabled_smf_services_not_online_by_sled:
        BTreeMap<SledUuid, SvcsEnabledNotOnlineResult>,
    /// IDs of sleds that aren't present in inventory or haven't reported a
    /// reconciliation result yet.
    missing_sleds: BTreeSet<SledUuid>,
}

impl UpdateStatusProblems {
    fn is_empty(&self) -> bool {
        let Self {
            stuck_sagas,
            stuck_sagas_error_message,
            stuck_update_last_blueprint_created_time,
            stale_inventory_last_collection_time_done,
            unhealthy_zpools_by_sled,
            enabled_smf_services_not_online_by_sled,
            missing_sleds,
        } = self;
        stuck_sagas.is_empty()
            && stuck_sagas_error_message.is_none()
            && stuck_update_last_blueprint_created_time.is_none()
            && stale_inventory_last_collection_time_done.is_none()
            && unhealthy_zpools_by_sled.is_empty()
            && enabled_smf_services_not_online_by_sled.is_empty()
            && missing_sleds.is_empty()
    }
}

impl KV for UpdateStatusProblems {
    // We keep this custome serialisation as using slog-derive would print out
    // every item always, we only want to log fields that contain problems.
    fn serialize(
        &self,
        _record: &Record,
        serializer: &mut dyn Serializer,
    ) -> slog::Result {
        let Self {
            stuck_sagas,
            stuck_sagas_error_message,
            stuck_update_last_blueprint_created_time,
            stale_inventory_last_collection_time_done,
            unhealthy_zpools_by_sled,
            enabled_smf_services_not_online_by_sled,
            missing_sleds,
        } = self;

        if !stuck_sagas.is_empty() {
            serializer.emit_arguments(
                "stuck_sagas".into(),
                &format_args!("{:?}", stuck_sagas),
            )?;
        }
        if let Some(error) = &stuck_sagas_error_message {
            serializer.emit_arguments(
                "stuck_sagas_error_message".into(),
                &format_args!("{error}"),
            )?;
        }
        if let Some(time_last_blueprint_created) =
            &stuck_update_last_blueprint_created_time
        {
            serializer.emit_arguments(
                "stuck_update_last_blueprint_created_time".into(),
                &format_args!("{time_last_blueprint_created}"),
            )?;
        }
        if let Some(collection_time_done) =
            &stale_inventory_last_collection_time_done
        {
            serializer.emit_arguments(
                "stale_inventory_last_collection_time_done".into(),
                &format_args!("{collection_time_done}"),
            )?;
        }
        if !unhealthy_zpools_by_sled.is_empty() {
            serializer.emit_arguments(
                "unhealthy_zpools_by_sled".into(),
                &format_args!("{:?}", unhealthy_zpools_by_sled),
            )?;
        }
        if !enabled_smf_services_not_online_by_sled.is_empty() {
            serializer.emit_arguments(
                "enabled_smf_services_not_online_by_sled".into(),
                &format_args!("{:?}", enabled_smf_services_not_online_by_sled),
            )?;
        }
        if !missing_sleds.is_empty() {
            serializer.emit_arguments(
                "missing_sleds".into(),
                &format_args!("{:?}", missing_sleds),
            )?;
        }
        Ok(())
    }
}

/// Returns true if the system appears to be mid-update.
///
/// If no target release has ever been set, the system has only ever been
/// mupdated and is not mid-update.
///
/// Otherwise, a system is considered mid-update when the current target
/// blueprint shows a previous update is still in progress (relative to
/// `current_target_version`). This catches the window between a new target
/// release being set and any components actually moving to that version, where
/// every component is still on the prior version.
fn is_update_in_progress(
    blueprint: &Blueprint,
    current_target_version: Option<&Version>,
) -> bool {
    // `BlueprintTargetReleaseStatus::new` does not check Hubris components,
    // but for the sake of what we need here are not be fully necessary.
    //
    // `BlueprintTargetReleaseStatus::new` will report that the update is in
    // progress until all zones and OS images are on the current version. We
    // don't update them until after all the Hubris components have completed
    // their updates, so if we were to get stuck while still updating Hubris
    // components, the check below will sitll be sufficient.
    let blueprint_in_progress = match current_target_version {
        Some(v) => match BlueprintTargetReleaseStatus::new(
            blueprint,
            &v.to_string(),
        ) {
            BlueprintTargetReleaseStatus::PreviousUpdateInProgress(_) => true,
            // We don't consider a Mupdate as an "update in-progress" because
            // recofigurator is not driving this update.
            BlueprintTargetReleaseStatus::WaitingForMupdateToBeCleared { .. }
            | BlueprintTargetReleaseStatus::AllComponentsOnCurrentTargetRelease
                => false,
        },
        // When `current_target_version` is `None` no target release has ever
        // been set. We can safely assume no update is in progress.
        None => false,
    };

    blueprint_in_progress
}

#[derive(Clone, Debug)]
enum UpdateActivityState {
    Idle,
    InProgress,
    Stuck,
}

impl UpdateActivityState {
    fn new(
        blueprint: &Blueprint,
        current_target_version: Option<&Version>,
    ) -> Self {
        // First, we determine if an update is not in progress.
        if !is_update_in_progress(blueprint, current_target_version) {
            return UpdateActivityState::Idle;
        }

        // An update is considered "stuck" if it is in progress but the last
        // created blueprint is older than `STUCK_UPDATE_THRESHOLD`.
        if blueprint.time_created < Utc::now() - STUCK_UPDATE_THRESHOLD {
            UpdateActivityState::Stuck
        } else {
            UpdateActivityState::InProgress
        }
    }
}

impl super::Nexus {
    pub(crate) async fn updates_put_repository(
        &self,
        opctx: &OpContext,
        body: impl Stream<Item = Result<Bytes, HttpError>> + Send + Sync + 'static,
        file_name: String,
    ) -> Result<TufRepoUpload, HttpError> {
        let mut trusted_roots = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .db_datastore
                .tuf_trust_root_list(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|a| a.id.into_untyped_uuid());
            for root in batch {
                trusted_roots.push(root.root_role.0.to_bytes());
            }
        }

        let artifacts_with_plan = ArtifactsWithPlan::from_stream(
            body,
            Some(file_name),
            ControlPlaneZonesMode::Split,
            VerificationMode::TrustStore(&trusted_roots),
            &self.log,
        )
        .await
        .map_err(|error| error.to_http_error())?;

        // Now store the artifacts in the database.
        let response = self
            .db_datastore
            .tuf_repo_insert(opctx, artifacts_with_plan.description())
            .await
            .map_err(HttpError::from)?;

        // Move the `ArtifactsWithPlan` (which carries with it the
        // `Utf8TempDir`s storing the artifacts) into the artifact replication
        // background task, then immediately activate the task. (If this repo
        // was already uploaded, the artifacts should immediately be dropped by
        // the task.)
        self.tuf_artifact_replication_tx
            .send(artifacts_with_plan)
            .await
            .map_err(|err| {
                // This error can only happen while Nexus's Tokio runtime is
                // shutting down; Sender::send returns an error only if the
                // receiver has hung up, and the receiver should live for
                // as long as Nexus does (it belongs to the background task
                // driver.)
                //
                // In the unlikely event that it does happen within this narrow
                // window, the impact is that the database has recorded a
                // repository for which we no longer have the artifacts. The fix
                // would be to reupload the repository.
                Error::internal_error(&format!(
                    "failed to send artifacts for replication: {err}"
                ))
            })?;
        self.background_tasks.task_tuf_artifact_replication.activate();

        Ok(response)
    }

    pub(crate) async fn updates_get_repository(
        &self,
        opctx: &OpContext,
        system_version: Version,
    ) -> Result<nexus_db_model::TufRepo, Error> {
        self.db_datastore
            .tuf_repo_get_by_version(opctx, system_version.into())
            .await
    }

    pub(crate) async fn updates_list_repositories(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Version>,
    ) -> Result<Vec<nexus_db_model::TufRepo>, Error> {
        self.db_datastore.tuf_repo_list(opctx, pagparams).await
    }

    pub(crate) async fn updates_add_trust_root(
        &self,
        opctx: &OpContext,
        trust_root: TufSignedRootRole,
    ) -> Result<TufTrustRoot, HttpError> {
        self.db_datastore
            .tuf_trust_root_insert(opctx, TufTrustRoot::new(trust_root))
            .await
            .map_err(HttpError::from)
    }

    pub(crate) async fn updates_get_trust_root(
        &self,
        opctx: &OpContext,
        id: TufTrustRootUuid,
    ) -> Result<TufTrustRoot, HttpError> {
        let (.., trust_root) = LookupPath::new(opctx, &self.db_datastore)
            .tuf_trust_root(id)
            .fetch()
            .await?;
        Ok(trust_root)
    }

    pub(crate) async fn updates_list_trust_roots(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> Result<Vec<TufTrustRoot>, HttpError> {
        self.db_datastore
            .tuf_trust_root_list(opctx, pagparams)
            .await
            .map_err(HttpError::from)
    }

    pub(crate) async fn updates_delete_trust_root(
        &self,
        opctx: &OpContext,
        id: TufTrustRootUuid,
    ) -> Result<(), HttpError> {
        let (authz, ..) = LookupPath::new(opctx, &self.db_datastore)
            .tuf_trust_root(id)
            .fetch_for(authz::Action::Delete)
            .await?;
        self.db_datastore
            .tuf_trust_root_delete(opctx, &authz)
            .await
            .map_err(HttpError::from)
    }

    /// Get external update status with aggregated component counts
    pub async fn update_status_external(
        &self,
        opctx: &OpContext,
    ) -> Result<update::UpdateStatus, Error> {
        let db_target_release =
            self.datastore().target_release_get_current(opctx).await?;

        let current_tuf_repo = match db_target_release.tuf_repo_id {
            Some(tuf_repo_id) => Some(
                self.datastore()
                    .tuf_repo_get_by_id(opctx, tuf_repo_id.into())
                    .await?,
            ),
            None => None,
        };

        let target_release =
            current_tuf_repo.as_ref().map(|repo| update::TargetRelease {
                time_requested: db_target_release.time_requested,
                version: repo.repo.system_version.0.clone(),
            });

        let Some(inventory) =
            self.inventory_load_rx().borrow_and_update().clone()
        else {
            return Err(Error::internal_error("No inventory collection found"));
        };

        let internal_status = self
            .get_internal_update_status(
                opctx,
                &db_target_release,
                current_tuf_repo,
                &inventory,
            )
            .await?;

        let components_by_release_version =
            component_version_counts(&internal_status).await?;

        let blueprint_target = self
            .update_status
            .latest_blueprint
            .borrow()
            .clone() // drop read lock held by outstanding borrow
            .ok_or_else(|| {
                Error::internal_error(
                    "Tried to get update status before \
                     target blueprint is loaded",
                )
            })?;

        let time_last_step_planned = blueprint_target.target.time_made_target;

        // Update activity is suspended if the current target release generation
        // is less than the blueprint's minimum generation
        let suspended = *db_target_release.generation
            < blueprint_target.blueprint.target_release_minimum_generation;

        // Decide whether to surface a "contact support" signal based on health
        // checks against a subset of components in the system
        let contact_support = self
            .contact_support(
                opctx,
                inventory,
                Arc::clone(&blueprint_target.blueprint),
                target_release.as_ref().map(|t| &t.version),
                internal_status,
            )
            .await?;

        Ok(update::UpdateStatus {
            target_release: Nullable(target_release),
            components_by_release_version,
            time_last_step_planned,
            suspended,
            contact_support,
        })
    }

    /// Identify known reasons why we would want a customer to call support
    /// before starting an upgrade or if an upgrade just finished. This is not
    /// an exhaustive health check. Long term, this will be replaced by an
    /// "active problems" facility driven by the Fault Management system. For
    /// now, we look for this list of known, serious problems:
    ///
    /// - No sagas have been running for longer than an hour.
    /// - An inventory collection exists
    /// - No update is in progress, or an update is in progress and the last
    ///   blueprint created is not older than the value of
    ///   STUCK_UPDATE_THRESHOLD.
    /// - All zpools are online.
    /// - All enabled SMF services are in an online state.
    /// - All expacted sleds are present.
    async fn contact_support(
        &self,
        opctx: &OpContext,
        inventory: Arc<Collection>,
        blueprint: Arc<Blueprint>,
        current_target_version: Option<&Version>,
        internal_update_status: internal_views::UpdateStatus,
    ) -> Result<bool, Error> {
        // If an update is in progress but not stuck, the remaining checks
        // could fail mid-update and shouldn't trigger a contact-support
        // signal.
        match UpdateActivityState::new(&blueprint, current_target_version) {
            UpdateActivityState::InProgress => {
                info!(
                    opctx.log,
                    "skipping update health checks; update in progress with last \
                    blueprint created within the last {}",
                    omicron_common::format_time_delta(STUCK_UPDATE_THRESHOLD);
                );
                return Ok(false);
            }
            UpdateActivityState::Idle | UpdateActivityState::Stuck => {}
        };

        let checks = UpdateContactSupportChecksInput {
            inventory,
            // TODO-K: Temporarily disabling the retrieval of stuck sagas.
            // In https://github.com/oxidecomputer/omicron/issues/10531 we found
            // some old unwinding sagas that didn't really affect the update
            // process in any way. The actual new retrieval method will be in
            // https://github.com/oxidecomputer/omicron/issues/10538, but to
            // make sure we don't block the upcoming release, we are disabling
            // saga reporting for now.
            stuck_sagas: Ok(vec![]),
            blueprint,
            current_target_version: current_target_version.cloned(),
            internal_update_status,
        };

        let problems = checks.problems();
        let contact_support = !problems.is_empty();

        if contact_support {
            warn!(
                opctx.log,
                "found problems in the system before or after an update";
                problems
            );
        }

        Ok(contact_support)
    }

    async fn get_internal_update_status(
        &self,
        opctx: &OpContext,
        target_release: &nexus_db_model::TargetRelease,
        current_tuf_repo: Option<nexus_db_model::TufRepoDescription>,
        inventory: &Arc<Collection>,
    ) -> Result<internal_views::UpdateStatus, Error> {
        // Build current TargetReleaseDescription, defaulting to Initial if
        // there is no tuf repo ID which, based on DB constraints, happens if
        // and only if target_release_source is 'unspecified', which should only
        // happen in the initial state before any target release has been set
        let curr_target_desc = match current_tuf_repo {
            Some(repo) => {
                TargetReleaseDescription::TufRepo(repo.into_external())
            }
            None => TargetReleaseDescription::Initial,
        };

        // Get previous target release (if it exists). Build the "prev"
        // TargetReleaseDescription from the previous generation if available,
        // otherwise fall back to Initial.
        let prev_repo_id =
            if let Some(prev_gen) = target_release.generation.prev() {
                self.datastore()
                    .target_release_get_generation(opctx, Generation(prev_gen))
                    .await
                    .internal_context("fetching previous target release")?
                    .and_then(|r| r.tuf_repo_id)
            } else {
                None
            };

        // It should never happen that a target release other than the initial
        // one with target_release_source unspecified should be missing a
        // tuf_repo_id. So if we have a tuf_repo_id for the previous target
        // release, we should always have one for the current target.
        if prev_repo_id.is_some() && target_release.tuf_repo_id.is_none() {
            return Err(Error::internal_error(
                "Target release has no tuf repo but previous release has one",
            ));
        }

        let prev_target_desc = match prev_repo_id {
            Some(id) => TargetReleaseDescription::TufRepo(
                self.datastore()
                    .tuf_repo_get_by_id(opctx, id.into())
                    .await?
                    .into_external(),
            ),
            None => TargetReleaseDescription::Initial,
        };

        // Get the list of sleds that should be reported as a part of the update
        // status. (In particular, this allows us to filter out sleds that are
        // physically present but not part of the cluster, as well as add
        // "unknown" counts for sleds that ought to be present but aren't.)
        let expected_sleds = self
            .datastore()
            .sled_list_all_batched(
                opctx,
                SledFilter::SpsUpdatedByReconfigurator,
            )
            .await?
            .iter()
            .map(|sled| {
                (
                    BaseboardId {
                        part_number: sled.part_number().to_string(),
                        serial_number: sled.serial_number().to_string(),
                    },
                    sled.id(),
                )
            })
            .collect();

        // It's weird to use the internal view this way. It would feel more
        // correct to extract shared logic and call it in both places. On the
        // other hand, that sharing would be boilerplatey and not add much yet.
        // So for now, use the internal view, but plan to extract shared logic
        // or do our own thing here once things settle.
        let status = internal_views::UpdateStatus::new(
            &prev_target_desc,
            &curr_target_desc,
            &expected_sleds,
            &inventory,
        );

        Ok(status)
    }
}

/// Build a map of version strings to the number of components on that
/// version
async fn component_version_counts(
    status: &internal_views::UpdateStatus,
) -> Result<BTreeMap<String, usize>, Error> {
    let sled_versions = status.sleds.iter().flat_map(|sled| {
        let zone_versions = sled.zones.iter().map(|zone| zone.version.clone());

        // boot_disk tells you which slot is relevant
        let host_version = sled.host_phase_2.boot_disk_version();

        zone_versions.chain(iter::once(host_version))
    });

    let mgs_driven_versions = status.mgs_driven.iter().flat_map(|status| {
        // for the SP, slot0_version is the active one
        let sp_version = status.sp.slot0_version.clone();

        // for the bootloader, stage0_version is the active one.
        let bootloader_version = status.rot_bootloader.stage0_version.clone();

        // for the RoT, get the version of the active slot.
        let rot_version = status.rot.active_slot_version();

        // This is an SP; it will only have a host OS phase 1 if it's a
        // sled (and not a switch / PSC). If it does, we have to check
        // the version of the active slot.
        let host_version = status.host_os_phase_1.active_slot_version();

        iter::once(sp_version)
            .chain(iter::once(rot_version))
            .chain(iter::once(bootloader_version))
            .chain(host_version)
    });

    let mut counts = BTreeMap::new();
    for version in sled_versions.chain(mgs_driven_versions) {
        // Don't use `version.to_string()` here because that will report
        // specific errors; instead, flatten all errors to just "error".
        // It's fine to use `.to_string()` for the non-error variants.
        let version = match version {
            internal_views::TufRepoVersion::Unknown
            | internal_views::TufRepoVersion::InstallDataset
            | internal_views::TufRepoVersion::Version(_) => version.to_string(),
            internal_views::TufRepoVersion::Error(_) => "error".to_string(),
        };
        *counts.entry(version).or_insert(0) += 1;
    }
    Ok(counts)
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Utc;
    use nexus_db_model::saga_types::Saga;
    use nexus_db_model::saga_types::SecId;
    use nexus_inventory::CollectionBuilder;
    use nexus_reconfigurator_planning::example::example;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::BlueprintArtifactVersion;
    use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
    use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use omicron_common::api::external::ByteCount;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_types::inventory::Baseboard;
    use sled_agent_types::inventory::ConfigReconcilerInventoryStatus;
    use sled_agent_types::inventory::FmdInventory;
    use sled_agent_types::inventory::Inventory;
    use sled_agent_types::inventory::InventoryZpool;
    use sled_agent_types::inventory::OmicronFileSourceResolverInventory;
    use sled_agent_types::inventory::SledCpuFamily;
    use sled_agent_types::inventory::SledRole;
    use sled_agent_types::inventory::SvcEnabledNotOnline;
    use sled_agent_types::inventory::SvcEnabledNotOnlineState;
    use sled_agent_types::inventory::SvcsEnabledNotOnline;
    use sled_agent_types::inventory::SvcsEnabledNotOnlineResult;
    use sled_agent_types::inventory::ZpoolHealth;
    use slog::Logger;
    use slog::o;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactVersion;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    fn fake_sled_inventory(
        zpools: Vec<InventoryZpool>,
        smf_services: SvcsEnabledNotOnlineResult,
    ) -> Inventory {
        Inventory {
            baseboard: Baseboard::Pc {
                identifier: "test-pc".to_string(),
                model: "test-model".to_string(),
            },
            reservoir_size: ByteCount::from(1024),
            sled_role: SledRole::Gimlet,
            sled_agent_address: "[::1]:56792".parse().unwrap(),
            sled_id: SledUuid::new_v4(),
            usable_hardware_threads: 10,
            usable_physical_ram: ByteCount::from(1024 * 1024),
            cpu_family: SledCpuFamily::AmdMilan,
            disks: vec![],
            zpools,
            datasets: vec![],
            ledgered_sled_config: None,
            reconciler_status: ConfigReconcilerInventoryStatus::NotYetRun,
            last_reconciliation: None,
            file_source_resolver: OmicronFileSourceResolverInventory::new_fake(
            ),
            smf_services_enabled_not_online: smf_services,
            reference_measurements: iddqd::IdOrdMap::new(),
            fmd: Ok(FmdInventory::default()),
        }
    }

    fn healthy_zpools() -> Vec<InventoryZpool> {
        vec![InventoryZpool {
            id: ZpoolUuid::new_v4(),
            total_size: ByteCount::from(1024 * 1024),
            health: ZpoolHealth::Online,
        }]
    }

    fn unhealthy_zpools() -> Vec<InventoryZpool> {
        vec![
            InventoryZpool {
                id: ZpoolUuid::new_v4(),
                total_size: ByteCount::from(1024 * 1024),
                health: ZpoolHealth::Online,
            },
            InventoryZpool {
                id: ZpoolUuid::new_v4(),
                total_size: ByteCount::from(1024 * 1024),
                health: ZpoolHealth::Degraded,
            },
        ]
    }

    fn healthy_services() -> SvcsEnabledNotOnlineResult {
        SvcsEnabledNotOnlineResult::SvcsEnabledNotOnline(SvcsEnabledNotOnline {
            services: vec![],
            errors: vec![],
            time_of_status: Utc::now(),
        })
    }

    fn unhealthy_services() -> SvcsEnabledNotOnlineResult {
        SvcsEnabledNotOnlineResult::SvcsEnabledNotOnline(SvcsEnabledNotOnline {
            services: vec![
                SvcEnabledNotOnline {
                    fmri: "svc:/system/test:default".to_string(),
                    zone: "global".to_string(),
                    state: SvcEnabledNotOnlineState::Maintenance,
                },
                SvcEnabledNotOnline {
                    fmri: "svc:/system/test2:default".to_string(),
                    zone: "global".to_string(),
                    state: SvcEnabledNotOnlineState::Offline,
                },
            ],
            errors: vec![],
            time_of_status: Utc::now(),
        })
    }

    fn fake_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    fn fake_target_version() -> Version {
        "9.2.0-0.ci+gite4b75dde134".parse().expect("valid version")
    }

    // Build an inventory collection with fake health statuses and insert it
    // as the latest collection.
    async fn insert_fake_collection(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
        zpools: Vec<InventoryZpool>,
        smf_services: SvcsEnabledNotOnlineResult,
    ) {
        let datastore = cptestctx.server.server_context().nexus.datastore();
        let collection =
            fake_collection_with_ids(sled_id(), zpools, smf_services);
        datastore
            .inventory_insert_collection(opctx, &collection)
            .await
            .expect("inserted inventory collection");
    }

    // Insert a running saga whose `time_created` is older than
    // `STUCK_SAGA_THRESHOLD`.
    async fn insert_stuck_running_saga(cptestctx: &ControlPlaneTestContext) {
        let datastore = cptestctx.server.server_context().nexus.datastore();
        let params = steno::SagaCreateParams {
            id: steno::SagaId(Uuid::new_v4()),
            name: steno::SagaName::new("test stuck saga"),
            dag: serde_json::Value::Null,
            state: steno::SagaCachedState::Running,
        };
        let mut saga = Saga::new(SecId(Uuid::new_v4()), params);
        saga.time_created =
            Utc::now() - STUCK_SAGA_THRESHOLD - TimeDelta::seconds(10);
        datastore.saga_create(&saga).await.expect("inserted stuck saga");
    }

    // Build a `Saga` without inserting it into the datastore for unit tests
    // that exercise `problems()` directly.
    fn fake_saga() -> Saga {
        let params = steno::SagaCreateParams {
            id: steno::SagaId(saga_uuid()),
            name: steno::SagaName::new("test stuck saga"),
            dag: serde_json::Value::Null,
            state: steno::SagaCachedState::Running,
        };
        Saga::new(SecId(Uuid::nil()), params)
    }

    // Build an in-memory `Collection` for unit tests with a hardcoded sled id.
    fn fake_collection_with_ids(
        sled_id: SledUuid,
        zpools: Vec<InventoryZpool>,
        smf_services: SvcsEnabledNotOnlineResult,
    ) -> Collection {
        let mut inv = fake_sled_inventory(zpools, smf_services);
        inv.sled_id = sled_id;
        let mut builder = CollectionBuilder::new("test");
        builder.found_sled_inventory("test", inv).unwrap();
        builder.build()
    }

    // Build a blueprint for tests targeting `version`.
    // When `in_progress` is true, one zone's `image_source` is overridden to an
    // older artifact version, which makes `is_update_in_progress` see the
    // system as in-progress.
    // When false, every component is on `version`.
    fn fake_blueprint(
        log: &Logger,
        version: &Version,
        time_created: DateTime<Utc>,
        in_progress: bool,
    ) -> Arc<Blueprint> {
        let (_, _, mut bp) = example(log, "test");
        bp.time_created = time_created;
        let artifact_version =
            ArtifactVersion::new(version.to_string()).unwrap();
        let os_artifact = BlueprintHostPhase2DesiredContents::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: artifact_version.clone(),
            },
            hash: ArtifactHash([0; 32]),
        };
        let zone_artifact = BlueprintZoneImageSource::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: artifact_version,
            },
            hash: ArtifactHash([0; 32]),
        };
        for sled_config in bp.sleds.values_mut() {
            sled_config.remove_mupdate_override = None;
            sled_config.host_phase_2 = BlueprintHostPhase2DesiredSlots {
                slot_a: os_artifact.clone(),
                slot_b: os_artifact.clone(),
            };
            for mut zone_config in sled_config.zones.iter_mut() {
                zone_config.image_source = zone_artifact.clone();
            }
        }

        // To make the blueprint look like an update is in progress, override
        // the first zone's `image_source` to an older artifact version. That
        // single zone is enough for `BlueprintTargetReleaseStatus::new` to
        // return `PreviousUpdateInProgress` against `version`.
        if in_progress {
            let older_zone_artifact = BlueprintZoneImageSource::Artifact {
                version: BlueprintArtifactVersion::Available {
                    version: ArtifactVersion::new("0.0.0-test-older").unwrap(),
                },
                hash: ArtifactHash([0; 32]),
            };
            if let Some(sled_config) = bp.sleds.values_mut().next()
                && let Some(mut zone_config) =
                    sled_config.zones.iter_mut().next()
            {
                zone_config.image_source = older_zone_artifact;
            }
        }
        Arc::new(bp)
    }

    fn sled_id() -> SledUuid {
        SledUuid::from_untyped_uuid(Uuid::from_u128(0xAAAA))
    }

    fn zpool_id() -> ZpoolUuid {
        ZpoolUuid::from_untyped_uuid(Uuid::from_u128(0xBBBB))
    }

    fn saga_uuid() -> Uuid {
        Uuid::from_u128(0xCCCC)
    }

    // `Zpool.time_collected` is stamped by `CollectionBuilder::build` with the
    // current time and isn't predictable. Replace it with the epoch so we can
    // assert against the rest of the zpool fields exactly.
    fn normalise_zpool_times(
        mut problems: UpdateStatusProblems,
    ) -> UpdateStatusProblems {
        for zpools in problems.unhealthy_zpools_by_sled.values_mut() {
            for z in zpools {
                z.time_collected = DateTime::<Utc>::UNIX_EPOCH;
            }
        }
        problems
    }

    // Build an empty `internal_views::UpdateStatus` for tests that don't care
    // about the missing-sleds check.
    fn empty_internal_update_status() -> internal_views::UpdateStatus {
        internal_views::UpdateStatus {
            mgs_driven: iddqd::IdOrdMap::new(),
            sleds: iddqd::IdOrdMap::new(),
        }
    }

    // Build a `SledAgentUpdateStatus` that looks "healthy" (i.e. has a known
    // boot disk and known slot versions).
    fn healthy_sled_agent_update_status(
        sled_id: SledUuid,
    ) -> internal_views::SledAgentUpdateStatus {
        internal_views::SledAgentUpdateStatus {
            sled_id,
            zones: iddqd::IdOrdMap::new(),
            host_phase_2: internal_views::HostPhase2Status {
                boot_disk: Ok(omicron_common::disk::M2Slot::A),
                slot_a_version: internal_views::TufRepoVersion::Version(
                    fake_target_version(),
                ),
                slot_b_version: internal_views::TufRepoVersion::Version(
                    fake_target_version(),
                ),
            },
        }
    }

    // Build an `internal_views::UpdateStatus` whose `sleds` contains an
    // "unknown" entry for each provided missing sled id, plus a healthy entry
    // for each provided healthy sled id.
    fn internal_update_status_with_missing_sleds(
        missing_sled_ids: impl IntoIterator<Item = SledUuid>,
        healthy_sled_ids: impl IntoIterator<Item = SledUuid>,
    ) -> internal_views::UpdateStatus {
        let mut sleds: iddqd::IdOrdMap<internal_views::SledAgentUpdateStatus> =
            missing_sled_ids
                .into_iter()
                .map(internal_views::SledAgentUpdateStatus::unknown)
                .collect();
        for sled_id in healthy_sled_ids {
            sleds
                .insert_unique(healthy_sled_agent_update_status(sled_id))
                .expect("sled ids are unique");
        }
        internal_views::UpdateStatus {
            mgs_driven: iddqd::IdOrdMap::new(),
            sleds,
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_healthy_system(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        insert_fake_collection(
            cptestctx,
            &opctx,
            healthy_zpools(),
            healthy_services(),
        )
        .await;
        // We get the latest collection directly from the datastore instead of
        // using the background task to make sure we get the most recent
        // collection that we just inserted
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );
        let version = fake_target_version();
        let blueprint =
            fake_blueprint(&cptestctx.logctx.log, &version, Utc::now(), false);
        // No health checks failed and no update is running, contact support
        // should be false
        assert!(
            !nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    Some(&version),
                    empty_internal_update_status(),
                )
                .await
                .unwrap()
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_unhealthy_zpools_healthy_services(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        insert_fake_collection(
            cptestctx,
            &opctx,
            unhealthy_zpools(),
            healthy_services(),
        )
        .await;
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );
        let version = fake_target_version();
        let blueprint =
            fake_blueprint(&cptestctx.logctx.log, &version, Utc::now(), false);
        // There are unhealthy zpools and no update is running, contact support
        // should be true
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    Some(&version),
                    empty_internal_update_status(),
                )
                .await
                .unwrap()
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_healthy_zpools_unhealthy_services(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        insert_fake_collection(
            cptestctx,
            &opctx,
            healthy_zpools(),
            unhealthy_services(),
        )
        .await;
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );
        let version = fake_target_version();
        let blueprint =
            fake_blueprint(&cptestctx.logctx.log, &version, Utc::now(), false);
        // There are unhealthy SMF services and no update is running, contact
        // support should be true
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    Some(&version),
                    empty_internal_update_status(),
                )
                .await
                .unwrap()
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_unhealthy_zpools_and_services(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        insert_fake_collection(
            cptestctx,
            &opctx,
            unhealthy_zpools(),
            unhealthy_services(),
        )
        .await;
        // There are unhealthy zpools and SMF services and no update has ever
        // been run, contact support should be true
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );
        let version = fake_target_version();
        let blueprint =
            fake_blueprint(&cptestctx.logctx.log, &version, Utc::now(), false);
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    None,
                    empty_internal_update_status(),
                )
                .await
                .unwrap()
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_stuck_saga(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        insert_fake_collection(
            cptestctx,
            &opctx,
            healthy_zpools(),
            healthy_services(),
        )
        .await;
        insert_stuck_running_saga(cptestctx).await;
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );
        let version = fake_target_version();
        let blueprint =
            fake_blueprint(&cptestctx.logctx.log, &version, Utc::now(), false);
        // There is a stuck active saga, but no update has ever been run. This
        // should prompt the user to contact support, but we disabled stuck
        // staga retrieval temporarily. Contact support should be false
        assert!(
            !nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    None,
                    empty_internal_update_status(),
                )
                .await
                .unwrap()
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_stuck_update(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        insert_fake_collection(
            cptestctx,
            &opctx,
            healthy_zpools(),
            healthy_services(),
        )
        .await;
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );
        let version = fake_target_version();
        let blueprint = fake_blueprint(
            &cptestctx.logctx.log,
            &version,
            Utc::now() - STUCK_UPDATE_THRESHOLD - TimeDelta::seconds(10),
            true,
        );
        // Components are split across multiple non-initial versions and the
        // last step planned is older than `STUCK_UPDATE_THRESHOLD`, so the
        // update is considered stuck and contact support is true.
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    Some(&version),
                    empty_internal_update_status(),
                )
                .await
                .unwrap()
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_all_unhealthy(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);

        // Add unhealthy zpools and enabled svcs not online
        insert_fake_collection(
            cptestctx,
            &opctx,
            unhealthy_zpools(),
            unhealthy_services(),
        )
        .await;

        // Insert stuck sagas
        insert_stuck_running_saga(cptestctx).await;
        insert_stuck_running_saga(cptestctx).await;

        // Backdate the collection so the stale-inventory check fires
        let mut collection = nexus
            .datastore()
            .inventory_get_latest_collection(&opctx)
            .await
            .unwrap()
            .unwrap();
        collection.time_done =
            Utc::now() - STALE_INVENTORY_THRESHOLD - TimeDelta::seconds(10);

        let inventory = Arc::new(collection);
        let version = fake_target_version();
        let blueprint = fake_blueprint(
            &cptestctx.logctx.log,
            &version,
            Utc::now() - STUCK_UPDATE_THRESHOLD - TimeDelta::seconds(10),
            true,
        );
        // Every health check is unhealthy: stuck saga, stuck update, stale
        // inventory, unhealthy zpools, and unhealthy SMF services, plus a
        // missing sled. Contact support should be true.
        let missing_sled_id = SledUuid::new_v4();
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    Some(&version),
                    internal_update_status_with_missing_sleds(
                        [missing_sled_id],
                        [sled_id()],
                    ),
                )
                .await
                .unwrap()
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_update_in_progress(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        // Even with unhealthy zpools and services, an update in progress with a
        // last step planned within the normal range skips the remaining health
        // checks and returns false.
        insert_fake_collection(
            cptestctx,
            &opctx,
            unhealthy_zpools(),
            unhealthy_services(),
        )
        .await;
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );
        let version = fake_target_version();
        let blueprint =
            fake_blueprint(&cptestctx.logctx.log, &version, Utc::now(), true);
        assert!(
            !nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    Some(&version),
                    empty_internal_update_status(),
                )
                .await
                .unwrap()
        );
    }

    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_missing_sleds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        insert_fake_collection(
            cptestctx,
            &opctx,
            healthy_zpools(),
            healthy_services(),
        )
        .await;
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );
        let version = fake_target_version();
        let blueprint =
            fake_blueprint(&cptestctx.logctx.log, &version, Utc::now(), false);
        // The system is otherwise healthy, but a sled we expected to see in
        // inventory is missing. Contact support should be true.
        let missing_sled_id = SledUuid::new_v4();
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    Some(&version),
                    internal_update_status_with_missing_sleds(
                        [missing_sled_id],
                        [sled_id()],
                    ),
                )
                .await
                .unwrap()
        );
    }

    // Tests the edge case where a new target release has been set, but no
    // components report being at the target release yet. We consider this case
    // as an update in-progress, so `contact_support()` should return `false`
    // even though the system has unhealthy zpools and SMF services.
    #[nexus_test(server = crate::Server)]
    async fn test_contact_support_target_set_but_components_on_old_version(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = fake_opctx(cptestctx);
        insert_fake_collection(
            cptestctx,
            &opctx,
            unhealthy_zpools(),
            unhealthy_services(),
        )
        .await;
        let inventory = Arc::new(
            nexus
                .datastore()
                .inventory_get_latest_collection(&opctx)
                .await
                .unwrap()
                .unwrap(),
        );

        let prev_version: Version =
            "8.0.0-0.ci+gitprev0000000".parse().unwrap();
        let target_release = fake_target_version();
        // The whole blueprint is on `prev_version`, which differs from the
        // current target `target_release` we pass below — that's what makes
        // `BlueprintTargetReleaseStatus::new` return `PreviousUpdateInProgress`
        // and the system look mid-update. `in_progress: false` is fine here
        // because the version mismatch alone is sufficient.
        let blueprint = fake_blueprint(
            &cptestctx.logctx.log,
            &prev_version,
            Utc::now(),
            false,
        );

        assert!(
            !nexus
                .contact_support(
                    &opctx,
                    inventory,
                    blueprint,
                    Some(&target_release),
                    empty_internal_update_status(),
                )
                .await
                .unwrap()
        );
    }

    #[test]
    fn test_problems_healthy_system() {
        let logctx = test_setup_log("test_problems_healthy_system");
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            false,
        );

        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };
        assert_eq!(checks.problems(), UpdateStatusProblems::default());

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_stuck_sagas() {
        let logctx = test_setup_log("test_problems_stuck_sagas");
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            false,
        );
        let saga = fake_saga();
        let expected_stuck_saga =
            StuckSaga { id: saga.id, name: saga.name.clone() };

        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![saga]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };

        let expected = UpdateStatusProblems {
            stuck_sagas: BTreeSet::from([expected_stuck_saga]),
            ..Default::default()
        };
        assert_eq!(checks.problems(), expected);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_stuck_sagas_error_message() {
        let logctx = test_setup_log("test_problems_stuck_sagas_error_message");
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            false,
        );
        let err = Error::internal_error("db boom");
        let expected_error = err.to_string();

        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Err(err),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };

        let expected = UpdateStatusProblems {
            stuck_sagas_error_message: Some(expected_error),
            ..Default::default()
        };
        assert_eq!(checks.problems(), expected);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_stuck_update() {
        let logctx = test_setup_log("test_problems_stuck_update");
        let time_last_blueprint_created =
            Utc::now() - STUCK_UPDATE_THRESHOLD - TimeDelta::seconds(10);
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            time_last_blueprint_created,
            true,
        );

        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };

        let expected = UpdateStatusProblems {
            stuck_update_last_blueprint_created_time: Some(
                time_last_blueprint_created,
            ),
            ..Default::default()
        };
        assert_eq!(checks.problems(), expected);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_update_in_progress_not_stuck_is_not_a_problem() {
        let logctx = test_setup_log(
            "test_problems_update_in_progress_not_stuck_is_not_a_problem",
        );
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            true,
        );
        // Update in progress but the last step planned is recent — not stuck.
        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };
        assert_eq!(checks.problems(), UpdateStatusProblems::default());

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_stale_inventory() {
        let logctx = test_setup_log("test_problems_stale_inventory");
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            false,
        );
        let mut collection = fake_collection_with_ids(
            sled_id(),
            healthy_zpools(),
            healthy_services(),
        );
        let collection_time_done =
            Utc::now() - STALE_INVENTORY_THRESHOLD - TimeDelta::seconds(10);
        collection.time_done = collection_time_done;

        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(collection),
            stuck_sagas: Ok(vec![]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };

        let expected = UpdateStatusProblems {
            stale_inventory_last_collection_time_done: Some(
                collection_time_done,
            ),
            ..Default::default()
        };
        assert_eq!(checks.problems(), expected);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_unhealthy_zpools() {
        let logctx = test_setup_log("test_problems_unhealthy_zpools");
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            false,
        );
        let sled_id = sled_id();
        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id,
                vec![InventoryZpool {
                    id: zpool_id(),
                    total_size: ByteCount::from(1024 * 1024),
                    health: ZpoolHealth::Degraded,
                }],
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };

        let expected_zpool = Zpool {
            time_collected: DateTime::<Utc>::UNIX_EPOCH,
            id: zpool_id(),
            total_size: ByteCount::from(1024 * 1024),
            health: ZpoolHealth::Degraded,
        };
        let expected = UpdateStatusProblems {
            unhealthy_zpools_by_sled: BTreeMap::from([(
                sled_id,
                vec![expected_zpool],
            )]),
            ..Default::default()
        };
        assert_eq!(normalise_zpool_times(checks.problems()), expected);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_unhealthy_services() {
        let logctx = test_setup_log("test_problems_unhealthy_services");
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            false,
        );
        let sled_id = sled_id();
        let services = unhealthy_services();
        let collection = fake_collection_with_ids(
            sled_id,
            healthy_zpools(),
            services.clone(),
        );

        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(collection),
            stuck_sagas: Ok(vec![]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };

        let expected = UpdateStatusProblems {
            enabled_smf_services_not_online_by_sled: BTreeMap::from([(
                sled_id, services,
            )]),
            ..Default::default()
        };
        assert_eq!(checks.problems(), expected);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_stuck_sagas_and_unhealthy_zpools() {
        let logctx =
            test_setup_log("test_problems_stuck_sagas_and_unhealthy_zpools");
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            false,
        );
        let sled_id = sled_id();
        let saga = fake_saga();
        let expected_stuck_saga =
            StuckSaga { id: saga.id, name: saga.name.clone() };

        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id,
                vec![InventoryZpool {
                    id: zpool_id(),
                    total_size: ByteCount::from(1024 * 1024),
                    health: ZpoolHealth::Degraded,
                }],
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![saga]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: empty_internal_update_status(),
        };

        let expected_zpool = Zpool {
            time_collected: DateTime::<Utc>::UNIX_EPOCH,
            id: zpool_id(),
            total_size: ByteCount::from(1024 * 1024),
            health: ZpoolHealth::Degraded,
        };
        let expected = UpdateStatusProblems {
            stuck_sagas: BTreeSet::from([expected_stuck_saga]),
            unhealthy_zpools_by_sled: BTreeMap::from([(
                sled_id,
                vec![expected_zpool],
            )]),
            ..Default::default()
        };
        assert_eq!(normalise_zpool_times(checks.problems()), expected);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_all_unhealthy() {
        let logctx = test_setup_log("test_problems_all_unhealthy");
        let time_last_blueprint_created =
            Utc::now() - STUCK_UPDATE_THRESHOLD - TimeDelta::seconds(10);
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            time_last_blueprint_created,
            true,
        );
        let sled_id = sled_id();
        let saga = fake_saga();
        let expected_stuck_saga =
            StuckSaga { id: saga.id, name: saga.name.clone() };

        let services = unhealthy_services();
        let mut collection = fake_collection_with_ids(
            sled_id,
            vec![InventoryZpool {
                id: zpool_id(),
                total_size: ByteCount::from(1024 * 1024),
                health: ZpoolHealth::Degraded,
            }],
            services.clone(),
        );
        let collection_time_done =
            Utc::now() - STALE_INVENTORY_THRESHOLD - TimeDelta::seconds(10);
        collection.time_done = collection_time_done;

        let missing_sled_id = SledUuid::new_v4();
        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(collection),
            stuck_sagas: Ok(vec![saga]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: internal_update_status_with_missing_sleds(
                [missing_sled_id],
                [sled_id],
            ),
        };

        let expected_zpool = Zpool {
            time_collected: DateTime::<Utc>::UNIX_EPOCH,
            id: zpool_id(),
            total_size: ByteCount::from(1024 * 1024),
            health: ZpoolHealth::Degraded,
        };
        let expected = UpdateStatusProblems {
            stuck_sagas: BTreeSet::from([expected_stuck_saga]),
            stuck_sagas_error_message: None,
            stuck_update_last_blueprint_created_time: Some(
                time_last_blueprint_created,
            ),
            stale_inventory_last_collection_time_done: Some(
                collection_time_done,
            ),
            unhealthy_zpools_by_sled: BTreeMap::from([(
                sled_id,
                vec![expected_zpool],
            )]),
            enabled_smf_services_not_online_by_sled: BTreeMap::from([(
                sled_id, services,
            )]),
            missing_sleds: BTreeSet::from([missing_sled_id]),
        };
        assert_eq!(normalise_zpool_times(checks.problems()), expected);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_problems_missing_sleds() {
        let logctx = test_setup_log("test_problems_missing_sleds");
        let blueprint = fake_blueprint(
            &logctx.log,
            &fake_target_version(),
            Utc::now(),
            false,
        );

        // Two sleds expected: one missing from inventory, one present and
        // healthy. Only the missing sled should be picked up.
        let missing_sled_id = SledUuid::new_v4();
        let healthy_sled_id = SledUuid::new_v4();
        let checks = UpdateContactSupportChecksInput {
            inventory: Arc::new(fake_collection_with_ids(
                healthy_sled_id,
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![]),
            blueprint,
            current_target_version: Some(fake_target_version()),
            internal_update_status: internal_update_status_with_missing_sleds(
                [missing_sled_id],
                [healthy_sled_id],
            ),
        };

        let expected = UpdateStatusProblems {
            missing_sleds: BTreeSet::from([missing_sled_id]),
            ..Default::default()
        };
        assert_eq!(checks.problems(), expected);

        logctx.cleanup_successful();
    }
}
