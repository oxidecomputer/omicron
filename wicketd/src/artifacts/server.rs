// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::store::WicketdArtifactStore;
use crate::installinator_progress::IprArtifactServer;
use async_trait::async_trait;
use dropshot::HttpError;
use hyper::Body;
use installinator_artifactd::ArtifactGetter;
use installinator_artifactd::EventReportStatus;
use omicron_common::update::ArtifactHashId;
use slog::error;
use slog::Logger;
use uuid::Uuid;

/// The artifact server interface for wicketd.
#[derive(Debug)]
pub(crate) struct WicketdArtifactServer {
    #[allow(dead_code)]
    log: Logger,
    store: WicketdArtifactStore,
    ipr_artifact: IprArtifactServer,
}

impl WicketdArtifactServer {
    pub(crate) fn new(
        log: &Logger,
        store: WicketdArtifactStore,
        ipr_artifact: IprArtifactServer,
    ) -> Self {
        let log = log.new(slog::o!("component" => "wicketd artifact server"));
        Self { log, store, ipr_artifact }
    }
}

#[async_trait]
impl ArtifactGetter for WicketdArtifactServer {
    async fn get_by_hash(&self, id: &ArtifactHashId) -> Option<(u64, Body)> {
        let data_handle = self.store.get_by_hash(id)?;
        let size = data_handle.file_size() as u64;
        let data_stream = match data_handle.reader_stream().await {
            Ok(stream) => stream,
            Err(err) => {
                error!(
                    self.log, "failed to open extracted archive on demand";
                    "error" => #%err,
                );
                return None;
            }
        };

        Some((size, Body::wrap_stream(data_stream)))
    }

    async fn report_progress(
        &self,
        update_id: Uuid,
        report: installinator_common::EventReport,
    ) -> Result<EventReportStatus, HttpError> {
        Ok(self.ipr_artifact.report_progress(update_id, report))
    }
}
