// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Software Updates

use bytes::Bytes;
use dropshot::HttpError;
use futures::Stream;
use nexus_auth::authz;
use nexus_db_lookup::LookupPath;
use nexus_db_model::{Generation, TufRepoDescription, TufTrustRoot};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::{datastore::SQL_BATCH_SIZE, pagination::Paginator};
use nexus_types::deployment::TargetReleaseDescription;
use nexus_types::external_api::shared::TufSignedRootRole;
use nexus_types::external_api::views;
use nexus_types::internal_api::views as internal_views;
use nexus_types::inventory::RotSlot;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::{
    DataPageParams, Error, TufRepoInsertResponse, TufRepoInsertStatus,
};
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::{GenericUuid, TufTrustRootUuid};
use semver::Version;
use std::collections::BTreeMap;
use update_common::artifacts::{
    ArtifactsWithPlan, ControlPlaneZonesMode, VerificationMode,
};
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn updates_put_repository(
        &self,
        opctx: &OpContext,
        body: impl Stream<Item = Result<Bytes, HttpError>> + Send + Sync + 'static,
        file_name: String,
    ) -> Result<TufRepoInsertResponse, HttpError> {
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

        // If we inserted a new repository, move the `ArtifactsWithPlan` (which
        // carries with it the `Utf8TempDir`s storing the artifacts) into the
        // artifact replication background task, then immediately activate the
        // task.
        if response.status == TufRepoInsertStatus::Inserted {
            self.tuf_artifact_replication_tx
                .send(artifacts_with_plan)
                .await
                .map_err(|err| {
                    // In theory this should never happen; `Sender::send`
                    // returns an error only if the receiver has hung up, and
                    // the receiver should live for as long as Nexus does (it
                    // belongs to the background task driver).
                    //
                    // If this _does_ happen, the impact is that the database
                    // has recorded a repository for which we no longer have
                    // the artifacts.
                    Error::internal_error(&format!(
                        "failed to send artifacts for replication: {err}"
                    ))
                })?;
            self.background_tasks.task_tuf_artifact_replication.activate();
        }

        Ok(response.into_external())
    }

    pub(crate) async fn updates_get_repository(
        &self,
        opctx: &OpContext,
        system_version: Version,
    ) -> Result<TufRepoDescription, HttpError> {
        self.db_datastore
            .tuf_repo_get_by_version(opctx, system_version.into())
            .await
            .map_err(HttpError::from)
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
    ) -> Result<views::UpdateStatus, Error> {
        // ? because we expect there to always be a current target release. but
        // it can still have an Unspecified release_source
        let db_target_release =
            self.datastore().target_release_get_current(opctx).await?;
        let target_release = match db_target_release.tuf_repo_id {
            Some(tuf_repo_id) => {
                let version = self
                    .datastore()
                    .tuf_repo_get_version(opctx, &tuf_repo_id)
                    .await?;
                Some(views::TargetRelease {
                    time_requested: db_target_release.time_requested,
                    version,
                })
            }
            None => None,
        };

        let components_by_release_version =
            self.component_version_counts(opctx, &db_target_release).await?;

        let time_last_progress = self
            .datastore()
            .blueprint_target_get_current(opctx)
            .await?
            .time_made_target;

        Ok(views::UpdateStatus {
            target_release,
            components_by_release_version,
            time_last_progress,
        })
    }

    /// Get component status using read-only queries to avoid batch operations
    async fn component_version_counts(
        &self,
        opctx: &OpContext,
        target_release: &nexus_db_model::TargetRelease,
    ) -> Result<BTreeMap<String, usize>, Error> {
        // Get the latest inventory collection
        let Some(inventory) =
            self.datastore().inventory_get_latest_collection(opctx).await?
        else {
            // No inventory collection available, return empty counts
            return Ok(BTreeMap::new());
        };

        // Build current TargetReleaseDescription, defaulting to Initial if
        // there is no tuf repo ID which, based on DB constraints, happens if
        // and only if target_release_source is 'unspecified', which should only
        // happen in the initial state before any target release has been set
        let curr_target_desc = match target_release.tuf_repo_id {
            Some(id) => TargetReleaseDescription::TufRepo(
                self.datastore()
                    .tuf_repo_get_by_id(opctx, id.into())
                    .await?
                    .into_external(),
            ),
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

        // It's weird to use the internal view this way. It would feel more
        // correct to extract shared logic and call it in both places. On the
        // other hand, that sharing would be boilerplatey and not add much yet.
        // So for now, use the internal view, but plan to extract shared logic
        // or do our own thing here once things settle.
        let status = internal_views::UpdateStatus::new(
            &prev_target_desc,
            &curr_target_desc,
            &inventory,
        );

        let sled_versions = status.sleds.into_iter().flat_map(|sled| {
            let zone_versions = sled.zones.into_iter().map(|zone| zone.version);

            // boot_disk tells you which slot is relevant
            let host_version =
                sled.host_phase_2.boot_disk.ok().map(|slot| match slot {
                    M2Slot::A => sled.host_phase_2.slot_a_version.clone(),
                    M2Slot::B => sled.host_phase_2.slot_b_version.clone(),
                });

            zone_versions.chain(host_version)
        });

        let mgs_driven_versions =
            status.mgs_driven.into_iter().flat_map(|status| {
                // for the SP, slot0_version is the active one
                let sp_version = status.sp.slot0_version.clone();

                // for the bootloader, stage0_version is the active one.
                let bootloader_version =
                    status.rot_bootloader.stage0_version.clone();

                let rot_version =
                    status.rot.active_slot.map(|slot| match slot {
                        RotSlot::A => status.rot.slot_a_version.clone(),
                        RotSlot::B => status.rot.slot_b_version.clone(),
                    });

                let host_version = match &status.host_os_phase_1 {
                    internal_views::HostPhase1Status::Sled {
                        slot_a_version,
                        slot_b_version,
                        active_slot,
                        ..
                    } => active_slot.map(|slot| match slot {
                        M2Slot::A => slot_a_version.clone(),
                        M2Slot::B => slot_b_version.clone(),
                    }),
                    internal_views::HostPhase1Status::NotASled => None,
                };

                std::iter::once(sp_version)
                    .chain(rot_version)
                    .chain(std::iter::once(bootloader_version))
                    .chain(host_version)
            });

        let mut counts = BTreeMap::new();
        for version in sled_versions.chain(mgs_driven_versions) {
            *counts.entry(version.to_string()).or_insert(0) += 1;
        }
        Ok(counts)
    }
}
