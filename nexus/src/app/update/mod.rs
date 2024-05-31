// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Software Updates

use bytes::Bytes;
use dropshot::HttpError;
use futures::Stream;
use nexus_config::UpdatesConfig;
use nexus_db_model::TufRepoDescription;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use omicron_common::api::external::{
    Error, SemverVersion, TufRepoInsertResponse,
};
use omicron_common::update::ArtifactId;
use slog::Logger;
use std::sync::Arc;
use update_common::artifacts::ArtifactsWithPlan;

mod common_sp_update;
mod host_phase1_updater;
mod mgs_clients;
mod rot_updater;
mod sp_updater;

pub use common_sp_update::SpComponentUpdateError;
pub use host_phase1_updater::HostPhase1Updater;
pub use mgs_clients::MgsClients;
pub use rot_updater::RotUpdater;
pub use sp_updater::SpUpdater;

#[derive(Debug, PartialEq, Clone)]
pub enum UpdateProgress {
    Started,
    Preparing { progress: Option<f64> },
    InProgress { progress: Option<f64> },
    Complete,
    Failed(String),
}

/// Application level operations related to software updates
#[derive(Clone)]
pub struct Update {
    log: Logger,
    datastore: Arc<db::DataStore>,
    updates_config: Option<UpdatesConfig>,
}

impl Update {
    pub fn new(
        log: Logger,
        datastore: Arc<db::DataStore>,
        updates_config: Option<UpdatesConfig>,
    ) -> Update {
        Update { log, datastore, updates_config }
    }

    pub(crate) async fn updates_put_repository(
        &self,
        opctx: &OpContext,
        body: impl Stream<Item = Result<Bytes, HttpError>> + Send + Sync + 'static,
        file_name: String,
    ) -> Result<TufRepoInsertResponse, HttpError> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // XXX: this needs to validate against the trusted root!
        let _updates_config =
            self.updates_config.as_ref().ok_or_else(|| {
                Error::internal_error("updates system not initialized")
            })?;

        let artifacts_with_plan =
            ArtifactsWithPlan::from_stream(body, Some(file_name), &self.log)
                .await
                .map_err(|error| error.to_http_error())?;

        // Now store the artifacts in the database.
        let tuf_repo_description = TufRepoDescription::from_external(
            artifacts_with_plan.description().clone(),
        );

        let response = self
            .datastore
            .update_tuf_repo_insert(opctx, tuf_repo_description)
            .await
            .map_err(HttpError::from)?;
        Ok(response.into_external())
    }

    pub(crate) async fn updates_get_repository(
        &self,
        opctx: &OpContext,
        system_version: SemverVersion,
    ) -> Result<TufRepoDescription, HttpError> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let _updates_config =
            self.updates_config.as_ref().ok_or_else(|| {
                Error::internal_error("updates system not initialized")
            })?;

        let tuf_repo_description = self
            .datastore
            .update_tuf_repo_get(opctx, system_version.into())
            .await
            .map_err(HttpError::from)?;

        Ok(tuf_repo_description)
    }

    /// Downloads a file (currently not implemented).
    pub(crate) async fn updates_download_artifact(
        &self,
        _opctx: &OpContext,
        _artifact: ArtifactId,
    ) -> Result<Vec<u8>, Error> {
        // TODO: this is part of the TUF repo depot.
        return Err(Error::internal_error(
            "artifact download not implemented, \
             will be part of TUF repo depot",
        ));
    }
}
