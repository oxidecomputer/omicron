// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Software Updates

use bytes::Bytes;
use dropshot::HttpError;
use futures::Stream;
use nexus_db_model::{TufRepoDescription, TufTrustRoot};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::{datastore::SQL_BATCH_SIZE, pagination::Paginator};
use nexus_types::external_api::shared::TufSignedRootRole;
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
        self.db_datastore
            .tuf_trust_root_get_by_id(opctx, id)
            .await
            .map_err(HttpError::from)
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
        self.db_datastore
            .tuf_trust_root_delete(opctx, id)
            .await
            .map_err(HttpError::from)
    }
}
