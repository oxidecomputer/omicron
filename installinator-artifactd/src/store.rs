// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::fmt;

use async_trait::async_trait;
use dropshot::HttpError;
use hyper::Body;
use installinator_common::ProgressReport;
use omicron_common::update::{ArtifactHashId, ArtifactId};
use slog::Logger;
use uuid::Uuid;

/// Represents a way to fetch artifacts.
#[async_trait]
pub trait ArtifactGetter: fmt::Debug + Send + Sync + 'static {
    /// Gets an artifact, returning it as a [`Body`].
    async fn get(&self, id: &ArtifactId) -> Option<Body>;

    /// Gets an artifact by hash, returning it as a [`Body`].
    async fn get_by_hash(&self, id: &ArtifactHashId) -> Option<Body>;

    /// Reports update progress events from the installinator.
    async fn report_progress(
        &self,
        update_id: Uuid,
        report: ProgressReport,
    ) -> Result<ProgressReportStatus, HttpError>;
}

/// The status returned by [`ArtifactGetter::report_progress`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum ProgressReportStatus {
    /// This event was processed by the server.
    Processed,

    /// The update ID was not recognized by the server.
    UnrecognizedUpdateId,
}

/// The artifact store -- a simple wrapper around a dynamic [`ArtifactGetter`] that does some basic
/// logging.
#[derive(Debug)]
pub(crate) struct ArtifactStore {
    log: Logger,
    getter: Box<dyn ArtifactGetter>,
    // TODO: implement this
}

impl ArtifactStore {
    pub(crate) fn new<Getter: ArtifactGetter>(
        getter: Getter,
        log: &Logger,
    ) -> Self {
        let log = log.new(slog::o!("component" => "artifact store"));
        Self { log, getter: Box::new(getter) }
    }

    pub(crate) async fn get_artifact(&self, id: &ArtifactId) -> Option<Body> {
        slog::debug!(self.log, "Artifact requested: {:?}", id);
        self.getter.get(id).await
    }

    pub(crate) async fn get_artifact_by_hash(
        &self,
        id: &ArtifactHashId,
    ) -> Option<Body> {
        slog::debug!(self.log, "Artifact requested by hash: {:?}", id);
        self.getter.get_by_hash(id).await
    }

    pub(crate) async fn report_progress(
        &self,
        update_id: Uuid,
        event: ProgressReport,
    ) -> Result<ProgressReportStatus, HttpError> {
        slog::debug!(self.log, "Report event for {update_id}: {event:?}");
        self.getter.report_progress(update_id, event).await
    }
}
