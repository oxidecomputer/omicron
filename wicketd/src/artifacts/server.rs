// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::installinator_progress::IprArtifactServer;
use dropshot::Body;
use dropshot::FreeformBody;
use dropshot::HttpError;
use dropshot::HttpResponseHeaders;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::TypedBody;
use futures::TryStreamExt;
use installinator_api::InstallinatorApi;
use installinator_api::ReportQuery;
use installinator_api::body_to_artifact_response;
use slog::Logger;
use slog::error;
use tufaceous_artifact::ArtifactHashId;
use update_engine::NestedSpec;
use update_engine::events::EventReport;

use super::WicketdArtifactStore;

pub(crate) enum WicketdInstallinatorApiImpl {}

/// The artifact server interface for wicketd.
#[derive(Debug)]
pub struct WicketdInstallinatorContext {
    log: Logger,
    store: WicketdArtifactStore,
    ipr_artifact: IprArtifactServer,
}

impl WicketdInstallinatorContext {
    pub(crate) fn new(
        log: &Logger,
        store: WicketdArtifactStore,
        ipr_artifact: IprArtifactServer,
    ) -> Self {
        Self {
            log: log
                .new(slog::o!("component" => "wicketd installinator server")),
            store,
            ipr_artifact,
        }
    }
}

impl InstallinatorApi for WicketdInstallinatorApiImpl {
    type Context = WicketdInstallinatorContext;

    async fn get_artifact_by_hash(
        rqctx: RequestContext<Self::Context>,
        path: Path<ArtifactHashId>,
    ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>
    {
        let context = rqctx.context();
        match context.store.get_by_hash(&path.into_inner()) {
            Some(data_handle) => {
                let size = data_handle.file_size() as u64;
                let data_stream = match data_handle.reader_stream().await {
                    Ok(stream) => stream,
                    Err(err) => {
                        error!(
                            context.log, "failed to open extracted archive on demand";
                            "error" => #%err,
                        );
                        return Err(HttpError::for_internal_error(format!(
                            // TODO: print error chain
                            "Artifact not found: {err}"
                        )));
                    }
                };

                let body = http_body_util::StreamBody::new(
                    data_stream.map_ok(|b| hyper::body::Frame::data(b)),
                );

                Ok(body_to_artifact_response(size, Body::wrap(body)))
            }
            None => {
                Err(HttpError::for_not_found(None, "Artifact not found".into()))
            }
        }
    }

    async fn report_progress(
        rqctx: RequestContext<Self::Context>,
        path: Path<ReportQuery>,
        report: TypedBody<EventReport<NestedSpec>>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let context = rqctx.context();
        let update_id = path.into_inner().update_id;

        context
            .ipr_artifact
            .report_progress(update_id, report.into_inner())
            .to_http_result(update_id)
    }
}
