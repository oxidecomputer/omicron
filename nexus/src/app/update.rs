// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Software Updates

use bytes::Bytes;
use dropshot::HttpError;
use futures::Stream;
use nexus_auth::authz;
use nexus_db_lookup::LookupPath;
use nexus_db_model::{TufRepoDescription, TufTrustRoot};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::{datastore::SQL_BATCH_SIZE, pagination::Paginator};
use nexus_types::external_api::shared::TufSignedRootRole;
use nexus_types::external_api::views;
use omicron_common::api::external::{
    DataPageParams, Error, TufRepoInsertResponse, TufRepoInsertStatus,
};
use omicron_uuid_kinds::{GenericUuid, TufTrustRootUuid};
use semver::Version;
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

    /// Get external update status with aggregated component counts and blockers
    pub async fn update_status_external(
        &self,
        opctx: &OpContext,
    ) -> Result<views::UpdateStatus, HttpError> {
        // Get target release information
        let target_release =
            match self.datastore().target_release_get_current(opctx).await {
                Ok(tr) => Some(
                    self.datastore().target_release_view(opctx, &tr).await?,
                ),
                Err(_) => None, // No target release set
            };

        // Get component status using read-only queries
        let components_by_release =
            self.get_component_status_readonly(opctx).await?;

        // Get last blueprint time
        let last_blueprint_time = self.get_last_blueprint_time(opctx).await?;

        // Generate blockers (placeholder for now)
        let blockers = self.generate_blockers(opctx).await?;

        Ok(views::UpdateStatus {
            target_release,
            components_by_release,
            last_blueprint_time,
            blockers,
        })
    }

    /// Get the time of the most recently created blueprint
    async fn get_last_blueprint_time(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, HttpError> {
        use omicron_common::api::external::DataPageParams;

        let blueprints = self
            .datastore()
            .blueprints_list(opctx, &DataPageParams::max_page())
            .await
            .map_err(HttpError::from)?;

        let latest_time =
            blueprints.into_iter().map(|bp| bp.time_created).max();

        Ok(latest_time)
    }

    /// Generate list of blockers (placeholder implementation)
    async fn generate_blockers(
        &self,
        _opctx: &OpContext,
    ) -> Result<Vec<views::UpdateBlocker>, HttpError> {
        // TODO: Implement actual blocker detection based on planning reports
        // For now, return empty list
        Ok(Vec::new())
    }

    /// Get component status using read-only queries to avoid batch operations
    async fn get_component_status_readonly(
        &self,
        opctx: &OpContext,
    ) -> Result<
        std::collections::BTreeMap<String, views::ComponentCounts>,
        HttpError,
    > {
        use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
        use std::collections::BTreeMap;
        use std::collections::HashMap;

        let mut counts = BTreeMap::new();

        // Get the latest inventory collection
        let Some(inventory) = self
            .datastore()
            .inventory_get_latest_collection(opctx)
            .await
            .map_err(HttpError::from)?
        else {
            // No inventory collection available, return empty counts
            return Ok(counts);
        };

        // Build hash-to-version mappings from TUF repos (current and previous)
        let mut hash_to_version = HashMap::new();

        // Get current target release
        if let Ok(target_release) =
            self.datastore().target_release_get_current(opctx).await
        {
            if let Some(tuf_repo_id) = target_release.tuf_repo_id {
                if let Ok(tuf_repo_desc) = self
                    .datastore()
                    .tuf_repo_get_by_id(opctx, tuf_repo_id.into())
                    .await
                {
                    let version_name =
                        format!("v{}", tuf_repo_desc.repo.system_version);
                    for artifact in &tuf_repo_desc.artifacts {
                        hash_to_version
                            .insert(artifact.sha256, version_name.clone());
                    }
                }
            }
        }

        // Get previous target release (if exists)
        // For simplicity, we'll try to get one generation back
        if let Ok(current) =
            self.datastore().target_release_get_current(opctx).await
        {
            if let Some(prev_gen) = current.generation.prev() {
                if let Ok(Some(prev_release)) = self
                    .datastore()
                    .target_release_get_generation(
                        opctx,
                        nexus_db_model::Generation(prev_gen),
                    )
                    .await
                {
                    if let Some(prev_tuf_repo_id) = prev_release.tuf_repo_id {
                        if let Ok(prev_tuf_repo_desc) = self
                            .datastore()
                            .tuf_repo_get_by_id(opctx, prev_tuf_repo_id.into())
                            .await
                        {
                            let prev_version_name = format!(
                                "v{}",
                                prev_tuf_repo_desc.repo.system_version
                            );
                            for artifact in &prev_tuf_repo_desc.artifacts {
                                // Only add if not already present (current takes priority)
                                hash_to_version
                                    .entry(artifact.sha256)
                                    .or_insert(prev_version_name.clone());
                            }
                        }
                    }
                }
            }
        }

        // Count zones by version using the hash mappings
        for agent in inventory.sled_agents.iter() {
            if let Some(reconciliation) = &agent.last_reconciliation {
                for (_zone_config, _result) in
                    reconciliation.reconciled_omicron_zones()
                {
                    let version_key = match &_zone_config.image_source {
                        OmicronZoneImageSource::InstallDataset => {
                            "install_dataset".to_string()
                        }
                        OmicronZoneImageSource::Artifact { hash } => {
                            // Convert tufaceous_artifact::ArtifactHash to nexus_db_model::ArtifactHash
                            let db_hash = nexus_db_model::ArtifactHash(*hash);
                            // Map hash to actual version if possible
                            hash_to_version
                                .get(&db_hash)
                                .cloned()
                                .unwrap_or_else(|| {
                                    format!(
                                        "unknown_artifact_{}",
                                        &hash.to_string()[..8]
                                    )
                                })
                        }
                    };

                    let entry =
                        counts.entry(version_key).or_insert_with(|| {
                            views::ComponentCounts {
                                zone_count: 0,
                                sp_count: 0,
                            }
                        });
                    entry.zone_count += 1;
                }
            }
        }

        // Count SPs - for now we can't easily determine SP versions without
        // complex operations, so group them all together
        let sp_count = inventory.sps.len() as u32;
        if sp_count > 0 {
            let entry =
                counts.entry("sp_firmware".to_string()).or_insert_with(|| {
                    views::ComponentCounts { zone_count: 0, sp_count: 0 }
                });
            entry.sp_count = sp_count;
        }

        Ok(counts)
    }
}
