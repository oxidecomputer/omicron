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
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::{
    DataPageParams, Error, TufRepoInsertResponse, TufRepoInsertStatus,
};
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
            .await?;

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
    ) -> Result<views::UpdateStatus, Error> {
        // Get target release information
        let target_release =
            match self.datastore().target_release_get_current(opctx).await {
                Ok(tr) => Some(
                    self.datastore().target_release_view(opctx, &tr).await?,
                ),
                Err(_) => None, // No target release set
            };

        let components_by_release =
            self.component_version_counts(opctx).await?;

        let last_blueprint_time =
            self.datastore().blueprint_get_latest_time(opctx).await?;

        // TODO: Figure out how to list things blocking progress

        Ok(views::UpdateStatus {
            target_release,
            components_by_release,
            last_blueprint_time,
        })
    }

    /// Get component status using read-only queries to avoid batch operations
    async fn component_version_counts(
        &self,
        opctx: &OpContext,
    ) -> Result<BTreeMap<String, usize>, Error> {
        // Get the latest inventory collection
        let Some(inventory) =
            self.datastore().inventory_get_latest_collection(opctx).await?
        else {
            // No inventory collection available, return empty counts
            return Ok(BTreeMap::new());
        };

        let target_release =
            self.datastore().target_release_get_current(opctx).await?;

        let Some(target_release_tuf_repo_id) = target_release.tuf_repo_id
        else {
            return Err(Error::internal_error(
                "target release has no TUF repo",
            ));
        };
        let target_release_desc = self
            .datastore()
            .tuf_repo_get_by_id(opctx, target_release_tuf_repo_id.into())
            .await?
            .into_external();

        // TODO: fall back to TargetReleaseDescription::Initial if there's no
        // previous release. Might not want to eat *all* errors, though.

        // Get previous target release (if exists)
        // For simplicity, we'll try to get one generation back
        let Some(prev_gen) = target_release.generation.prev() else {
            return Err(Error::internal_error(
                "target release has no prev gen",
            ));
        };

        let prev_release = self
            .datastore()
            .target_release_get_generation(opctx, Generation(prev_gen))
            .await
            .internal_context("fetching previous target release")?;
        let Some(prev_release_tuf_repo_id) =
            prev_release.and_then(|r| r.tuf_repo_id)
        else {
            return Err(Error::internal_error("prev release has no TUF repo"));
        };
        let prev_release_desc = self
            .datastore()
            .tuf_repo_get_by_id(opctx, prev_release_tuf_repo_id.into())
            .await?
            .into_external();

        // TODO: It's weird to use the internal view this way. On the other hand
        // it feels silly to extract a shared structure that's basically the
        // same as this struct.
        let status = internal_views::UpdateStatus::new(
            &TargetReleaseDescription::TufRepo(prev_release_desc),
            &TargetReleaseDescription::TufRepo(target_release_desc),
            &inventory,
        );

        let zone_versions = status
            .zones
            .values()
            .flat_map(|zones| zones.iter().map(|zone| zone.version.clone()));
        let sp_versions = status.sps.values().flat_map(|sp| {
            [sp.slot0_version.clone(), sp.slot1_version.clone()]
        });

        let mut counts = BTreeMap::new();
        for version in zone_versions.chain(sp_versions) {
            *counts.entry(version.to_string()).or_insert(0) += 1;
        }
        Ok(counts)
    }
}
