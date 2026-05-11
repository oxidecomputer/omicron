// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Software Updates

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
use slog::OwnedKV;
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
/// Sometimes sagas can sometimes spend time being unassigned or recovered
/// across Nexus restarts. To calculate this threshold we took a sample of
/// 10,000 sagas and the longest running saga took ~1h30m so we give ample time
/// before we consider a saga stuck.
const STUCK_SAGA_THRESHOLD: TimeDelta = TimeDelta::minutes(120);

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
/// time, usually around 10-13 minutes. We give ourselves a bit more time than
/// that before considering an update stuck.
const STUCK_UPDATE_THRESHOLD: TimeDelta = TimeDelta::minutes(30);

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

/// Inputs needed to identify update health problems.
struct UpdateContactSupportChecks {
    inventory: Arc<Collection>,
    stuck_sagas: Result<Vec<Saga>, Error>,
    components_by_release_version: BTreeMap<String, usize>,
    time_last_step_planned: DateTime<Utc>,
}

/// Identifies a saga that has been running or unwinding for too long.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StuckSaga {
    id: SagaId,
    name: String,
}

/// A problem identified by an update health check.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum UpdateStatusProblem {
    /// One or more sagas have been running or unwinding longer than
    /// `STUCK_SAGA_THRESHOLD`.
    StuckSagas { sagas: Vec<StuckSaga> },
    /// The query for stuck sagas itself failed.
    StuckSagasQueryFailed { error: String },
    /// An update is in progress and the last step planned in the blueprint is
    /// older than `STUCK_UPDATE_THRESHOLD`.
    StuckUpdate { time_last_step_planned: DateTime<Utc> },
    /// The latest inventory collection is older than
    /// `STALE_INVENTORY_THRESHOLD`.
    StaleInventory { collection_time_done: DateTime<Utc> },
    /// One or more zpools are not in an `Online` state.
    UnhealthyZpools { zpools_by_sled: BTreeMap<SledUuid, Vec<Zpool>> },
    /// One or more enabled SMF services are not in an `online` state.
    EnabledSmfServicesNotOnline {
        svcs_by_sled: BTreeMap<SledUuid, SvcsEnabledNotOnlineResult>,
    },
}

impl KV for UpdateStatusProblem {
    fn serialize(
        &self,
        _record: &Record,
        serializer: &mut dyn Serializer,
    ) -> slog::Result {
        match self {
            Self::StuckSagas { sagas } => serializer.emit_arguments(
                "stuck_sagas".into(),
                &format_args!("{sagas:?}"),
            ),
            Self::StuckSagasQueryFailed { error } => serializer.emit_arguments(
                "error_message".into(),
                &format_args!("{error}"),
            ),
            Self::StuckUpdate { time_last_step_planned } => serializer
                .emit_arguments(
                    "stuck_update_last_step_planned_time".into(),
                    &format_args!("{time_last_step_planned:?}"),
                ),
            Self::StaleInventory { collection_time_done } => serializer
                .emit_arguments(
                    "stale_inventory_last_collection_time_done".into(),
                    &format_args!("{collection_time_done:?}"),
                ),
            Self::UnhealthyZpools { zpools_by_sled } => serializer
                .emit_arguments(
                    "unhealthy_zpools_by_sled".into(),
                    &format_args!("{zpools_by_sled:?}"),
                ),
            Self::EnabledSmfServicesNotOnline { svcs_by_sled } => serializer
                .emit_arguments(
                    "enabled_not_online_svcs_by_sled".into(),
                    &format_args!("{svcs_by_sled:?}"),
                ),
        }
    }
}

/// Wrapper that lets a whole set of [`UpdateStatusProblem`]s contribute their
/// kvs to a single log record.
struct UpdateStatusProblemsKv(BTreeSet<UpdateStatusProblem>);

impl KV for UpdateStatusProblemsKv {
    fn serialize(
        &self,
        record: &Record,
        serializer: &mut dyn Serializer,
    ) -> slog::Result {
        for problem in &self.0 {
            problem.serialize(record, serializer)?;
        }
        Ok(())
    }
}

/// We assume an update is in progress if not all components are at the
/// same version or there are only two versions and they are not
/// "install dataset" and "unknown".
fn is_update_in_progress(
    components_by_release_version: &BTreeMap<String, usize>,
) -> bool {
    let versions_at_initial_state = components_by_release_version.len() == 2
        && components_by_release_version
            .contains_key(&internal_views::TufRepoVersion::Unknown.to_string())
        && components_by_release_version.contains_key(
            &internal_views::TufRepoVersion::InstallDataset.to_string(),
        );
    components_by_release_version.len() != 1 && !versions_at_initial_state
}

/// An update is "stuck" if it is in progress but the last step planned
/// in the blueprint is older than `STUCK_UPDATE_THRESHOLD`.
fn is_update_stuck(
    components_by_release_version: &BTreeMap<String, usize>,
    time_last_step_planned: DateTime<Utc>,
) -> bool {
    is_update_in_progress(components_by_release_version)
        && time_last_step_planned < Utc::now() - STUCK_UPDATE_THRESHOLD
}

impl UpdateContactSupportChecks {
    /// Identify the set of problems present with given inputs.
    fn problems(&self) -> BTreeSet<UpdateStatusProblem> {
        let mut problems = BTreeSet::new();

        if is_update_stuck(
            &self.components_by_release_version,
            self.time_last_step_planned,
        ) {
            problems.insert(UpdateStatusProblem::StuckUpdate {
                time_last_step_planned: self.time_last_step_planned,
            });
        }

        match &self.stuck_sagas {
            Ok(sagas) => {
                let sagas: Vec<_> = sagas
                    .iter()
                    .map(|s| StuckSaga { id: s.id, name: s.name.clone() })
                    .collect();
                if !sagas.is_empty() {
                    problems.insert(UpdateStatusProblem::StuckSagas { sagas });
                }
            }
            Err(e) => {
                problems.insert(UpdateStatusProblem::StuckSagasQueryFailed {
                    error: e.to_string(),
                });
            }
        }

        if self.inventory.time_done < Utc::now() - STALE_INVENTORY_THRESHOLD {
            problems.insert(UpdateStatusProblem::StaleInventory {
                collection_time_done: self.inventory.time_done,
            });
        }

        let zpools_by_sled: BTreeMap<_, Vec<Zpool>> = self
            .inventory
            .unhealthy_zpools()
            .into_iter()
            .map(|(sled, zpools)| (sled, zpools.into_iter().cloned().collect()))
            .collect();
        if !zpools_by_sled.is_empty() {
            problems.insert(UpdateStatusProblem::UnhealthyZpools {
                zpools_by_sled,
            });
        }

        let svcs_by_sled: BTreeMap<_, SvcsEnabledNotOnlineResult> = self
            .inventory
            .enabled_smf_services_not_online()
            .into_iter()
            .map(|(sled, svcs)| (sled, svcs.clone()))
            .collect();
        if !svcs_by_sled.is_empty() {
            problems.insert(UpdateStatusProblem::EnabledSmfServicesNotOnline {
                svcs_by_sled,
            });
        }

        problems
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

        let components_by_release_version = self
            .component_version_counts(
                opctx,
                &db_target_release,
                current_tuf_repo,
                &inventory,
            )
            .await?;

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

        // We want a rough idea of whether the system is in a healthy state or
        // not. We do this by retrieving the latest inventory collection and
        // performing a series of checks.
        let contact_support = self
            .contact_support(
                opctx,
                time_last_step_planned,
                components_by_release_version.clone(),
                inventory,
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
    ///   step planned is not older than the value of STUCK_UPDATE_THRESHOLD.
    /// - All zpools are online.
    /// - All enabled SMF services are in an online state.
    async fn contact_support(
        &self,
        opctx: &OpContext,
        time_last_step_planned: DateTime<Utc>,
        components_by_release_version: BTreeMap<String, usize>,
        inventory: Arc<Collection>,
    ) -> Result<bool, Error> {
        // If an update is in progress but not stuck, the remaining checks
        // could fail mid-update and shouldn't trigger a contact-support
        // signal.
        if is_update_in_progress(&components_by_release_version)
            && !is_update_stuck(
                &components_by_release_version,
                time_last_step_planned,
            )
        {
            info!(
                opctx.log,
                "skipping update health checks; update in progress with last \
                step planned within the last {}",
                omicron_common::format_time_delta(STUCK_UPDATE_THRESHOLD);
            );
            return Ok(false);
        }

        let stuck_sagas = self
            .datastore()
            .saga_list_running_or_unwinding_older_than(
                opctx,
                STUCK_SAGA_THRESHOLD,
            )
            .await;

        let checks = UpdateContactSupportChecks {
            inventory,
            stuck_sagas,
            components_by_release_version,
            time_last_step_planned,
        };

        let problems = checks.problems();
        let contact_support = !problems.is_empty();

        if contact_support {
            let log = opctx.log.new(OwnedKV(UpdateStatusProblemsKv(problems)));
            warn!(
                log,
                "found problems in the system before or after an update"
            );
        }

        Ok(contact_support)
    }

    /// Build a map of version strings to the number of components on that
    /// version
    async fn component_version_counts(
        &self,
        opctx: &OpContext,
        target_release: &nexus_db_model::TargetRelease,
        current_tuf_repo: Option<nexus_db_model::TufRepoDescription>,
        inventory: &Arc<Collection>,
    ) -> Result<BTreeMap<String, usize>, Error> {
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

        let sled_versions = status.sleds.into_iter().flat_map(|sled| {
            let zone_versions = sled.zones.into_iter().map(|zone| zone.version);

            // boot_disk tells you which slot is relevant
            let host_version = sled.host_phase_2.boot_disk_version();

            zone_versions.chain(iter::once(host_version))
        });

        let mgs_driven_versions =
            status.mgs_driven.into_iter().flat_map(|status| {
                // for the SP, slot0_version is the active one
                let sp_version = status.sp.slot0_version.clone();

                // for the bootloader, stage0_version is the active one.
                let bootloader_version =
                    status.rot_bootloader.stage0_version.clone();

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
                | internal_views::TufRepoVersion::Version(_) => {
                    version.to_string()
                }
                internal_views::TufRepoVersion::Error(_) => "error".to_string(),
            };
            *counts.entry(version).or_insert(0) += 1;
        }
        Ok(counts)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Utc;
    use nexus_db_model::saga_types::Saga;
    use nexus_db_model::saga_types::SecId;
    use nexus_inventory::CollectionBuilder;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_types::inventory::Baseboard;
    use sled_agent_types::inventory::ConfigReconcilerInventoryStatus;
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
    use slog::o;
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

    fn system_versions_initial_state() -> BTreeMap<String, usize> {
        BTreeMap::from([
            ("install dataset".to_string(), 13),
            ("unknown".to_string(), 2),
        ])
    }

    fn system_version_update_finished() -> BTreeMap<String, usize> {
        BTreeMap::from([("9.2.0-0.ci+gite4b75dde134".to_string(), 15)])
    }

    fn system_version_update_in_progress() -> BTreeMap<String, usize> {
        BTreeMap::from([
            ("9.2.0-0.ci+gite4b75dde134".to_string(), 12),
            ("install dataset".to_string(), 3),
        ])
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
        problems: BTreeSet<UpdateStatusProblem>,
    ) -> BTreeSet<UpdateStatusProblem> {
        problems
            .into_iter()
            .map(|mut p| {
                if let UpdateStatusProblem::UnhealthyZpools { zpools_by_sled } =
                    &mut p
                {
                    for zpools in zpools_by_sled.values_mut() {
                        for z in zpools {
                            z.time_collected = DateTime::<Utc>::UNIX_EPOCH;
                        }
                    }
                }
                p
            })
            .collect()
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
        // No health checks failed and no update is running, contact support
        // should be false
        assert!(
            !nexus
                .contact_support(
                    &opctx,
                    Utc::now(),
                    system_version_update_finished(),
                    inventory,
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
        // There are unhealthy zpools and no update is running, contact support
        // should be true
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    Utc::now(),
                    system_version_update_finished(),
                    inventory,
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
        // There are unhealthy SMF services and no update is running, contact
        // support should be true
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    Utc::now(),
                    system_version_update_finished(),
                    inventory,
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
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    Utc::now(),
                    system_versions_initial_state(),
                    inventory,
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
        // There is a stuck active saga no update has ever been run, contact
        // support should be true
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    Utc::now(),
                    system_versions_initial_state(),
                    inventory
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
        // Components are split across multiple non-initial versions and the
        // last step planned is older than `STUCK_UPDATE_THRESHOLD`, so the
        // update is considered stuck and contact support is true.
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    Utc::now()
                        - STUCK_UPDATE_THRESHOLD
                        - TimeDelta::seconds(10),
                    system_version_update_in_progress(),
                    inventory,
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
        // Every health check is unhealthy: stuck saga, stuck update, stale
        // inventory, unhealthy zpools, and unhealthy SMF services. Contact
        // support should be true.
        assert!(
            nexus
                .contact_support(
                    &opctx,
                    Utc::now()
                        - STUCK_UPDATE_THRESHOLD
                        - TimeDelta::seconds(10),
                    system_version_update_in_progress(),
                    inventory,
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
        assert!(
            !nexus
                .contact_support(
                    &opctx,
                    Utc::now(),
                    system_version_update_in_progress(),
                    inventory,
                )
                .await
                .unwrap()
        );
    }

    #[test]
    fn test_problems_healthy_system() {
        let checks = UpdateContactSupportChecks {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![]),
            components_by_release_version: system_version_update_finished(),
            time_last_step_planned: Utc::now(),
        };
        assert_eq!(checks.problems(), BTreeSet::new());
    }

    #[test]
    fn test_problems_stuck_sagas() {
        let saga = fake_saga();
        let expected_stuck_saga =
            StuckSaga { id: saga.id, name: saga.name.clone() };

        let checks = UpdateContactSupportChecks {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![saga]),
            components_by_release_version: system_version_update_finished(),
            time_last_step_planned: Utc::now(),
        };

        let expected = BTreeSet::from([UpdateStatusProblem::StuckSagas {
            sagas: vec![expected_stuck_saga],
        }]);
        assert_eq!(checks.problems(), expected);
    }

    #[test]
    fn test_problems_stuck_sagas_query_failed() {
        let err = Error::internal_error("db boom");
        let expected_error = err.to_string();

        let checks = UpdateContactSupportChecks {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Err(err),
            components_by_release_version: system_version_update_finished(),
            time_last_step_planned: Utc::now(),
        };

        let expected =
            BTreeSet::from([UpdateStatusProblem::StuckSagasQueryFailed {
                error: expected_error,
            }]);
        assert_eq!(checks.problems(), expected);
    }

    #[test]
    fn test_problems_stuck_update() {
        let time_last_step_planned =
            Utc::now() - STUCK_UPDATE_THRESHOLD - TimeDelta::seconds(10);

        let checks = UpdateContactSupportChecks {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![]),
            components_by_release_version: system_version_update_in_progress(),
            time_last_step_planned,
        };

        let expected = BTreeSet::from([UpdateStatusProblem::StuckUpdate {
            time_last_step_planned,
        }]);
        assert_eq!(checks.problems(), expected);
    }

    #[test]
    fn test_problems_update_in_progress_not_stuck_is_not_a_problem() {
        // Update in progress but the last step planned is recent — not stuck.
        let checks = UpdateContactSupportChecks {
            inventory: Arc::new(fake_collection_with_ids(
                sled_id(),
                healthy_zpools(),
                healthy_services(),
            )),
            stuck_sagas: Ok(vec![]),
            components_by_release_version: system_version_update_in_progress(),
            time_last_step_planned: Utc::now(),
        };
        assert_eq!(checks.problems(), BTreeSet::new());
    }

    #[test]
    fn test_problems_stale_inventory() {
        let mut collection = fake_collection_with_ids(
            sled_id(),
            healthy_zpools(),
            healthy_services(),
        );
        let collection_time_done =
            Utc::now() - STALE_INVENTORY_THRESHOLD - TimeDelta::seconds(10);
        collection.time_done = collection_time_done;

        let checks = UpdateContactSupportChecks {
            inventory: Arc::new(collection),
            stuck_sagas: Ok(vec![]),
            components_by_release_version: system_version_update_finished(),
            time_last_step_planned: Utc::now(),
        };

        let expected = BTreeSet::from([UpdateStatusProblem::StaleInventory {
            collection_time_done,
        }]);
        assert_eq!(checks.problems(), expected);
    }

    #[test]
    fn test_problems_unhealthy_zpools() {
        let sled_id = sled_id();
        let checks = UpdateContactSupportChecks {
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
            components_by_release_version: system_version_update_finished(),
            time_last_step_planned: Utc::now(),
        };

        let expected_zpool = Zpool {
            time_collected: DateTime::<Utc>::UNIX_EPOCH,
            id: zpool_id(),
            total_size: ByteCount::from(1024 * 1024),
            health: ZpoolHealth::Degraded,
        };
        let expected = BTreeSet::from([UpdateStatusProblem::UnhealthyZpools {
            zpools_by_sled: BTreeMap::from([(sled_id, vec![expected_zpool])]),
        }]);
        assert_eq!(normalise_zpool_times(checks.problems()), expected);
    }

    #[test]
    fn test_problems_unhealthy_services() {
        let sled_id = sled_id();
        let services = unhealthy_services();
        let collection = fake_collection_with_ids(
            sled_id,
            healthy_zpools(),
            services.clone(),
        );

        let checks = UpdateContactSupportChecks {
            inventory: Arc::new(collection),
            stuck_sagas: Ok(vec![]),
            components_by_release_version: system_version_update_finished(),
            time_last_step_planned: Utc::now(),
        };

        let expected = BTreeSet::from([
            UpdateStatusProblem::EnabledSmfServicesNotOnline {
                svcs_by_sled: BTreeMap::from([(sled_id, services)]),
            },
        ]);
        assert_eq!(checks.problems(), expected);
    }

    #[test]
    fn test_problems_stuck_sagas_and_unhealthy_zpools() {
        let sled_id = sled_id();
        let saga = fake_saga();
        let expected_stuck_saga =
            StuckSaga { id: saga.id, name: saga.name.clone() };

        let checks = UpdateContactSupportChecks {
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
            components_by_release_version: system_version_update_finished(),
            time_last_step_planned: Utc::now(),
        };

        let expected_zpool = Zpool {
            time_collected: DateTime::<Utc>::UNIX_EPOCH,
            id: zpool_id(),
            total_size: ByteCount::from(1024 * 1024),
            health: ZpoolHealth::Degraded,
        };
        let expected = BTreeSet::from([
            UpdateStatusProblem::StuckSagas {
                sagas: vec![expected_stuck_saga],
            },
            UpdateStatusProblem::UnhealthyZpools {
                zpools_by_sled: BTreeMap::from([(
                    sled_id,
                    vec![expected_zpool],
                )]),
            },
        ]);
        assert_eq!(normalise_zpool_times(checks.problems()), expected);
    }

    #[test]
    fn test_problems_all_unhealthy() {
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

        let time_last_step_planned =
            Utc::now() - STUCK_UPDATE_THRESHOLD - TimeDelta::seconds(10);

        let checks = UpdateContactSupportChecks {
            inventory: Arc::new(collection),
            stuck_sagas: Ok(vec![saga]),
            components_by_release_version: system_version_update_in_progress(),
            time_last_step_planned,
        };

        let expected_zpool = Zpool {
            time_collected: DateTime::<Utc>::UNIX_EPOCH,
            id: zpool_id(),
            total_size: ByteCount::from(1024 * 1024),
            health: ZpoolHealth::Degraded,
        };
        let expected = BTreeSet::from([
            UpdateStatusProblem::StuckSagas {
                sagas: vec![expected_stuck_saga],
            },
            UpdateStatusProblem::StuckUpdate { time_last_step_planned },
            UpdateStatusProblem::StaleInventory { collection_time_done },
            UpdateStatusProblem::UnhealthyZpools {
                zpools_by_sled: BTreeMap::from([(
                    sled_id,
                    vec![expected_zpool],
                )]),
            },
            UpdateStatusProblem::EnabledSmfServicesNotOnline {
                svcs_by_sled: BTreeMap::from([(sled_id, services)]),
            },
        ]);
        assert_eq!(normalise_zpool_times(checks.problems()), expected);
    }
}
